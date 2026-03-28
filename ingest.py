"""
Ingestion pipeline for earnings filings.

Fetches 8-K filings, extracts exhibit text, parses with Claude,
and stores results in Neon Postgres + S3.

Usage:
    python -m ingest --ticker SNOW
    python -m ingest --all-watchlist
    python -m ingest --backfill-cache
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import db
import storage
from earnings_guidance_analyzer import (
    fetch_8k_filings,
    fetch_exhibit_text,
    fetch_income_statements,
    fetch_stock_prices,
    is_earnings_8k_quick,
    llm_parse_filing,
    _get_cik_for_ticker,
    _fetch_edgar_8k_filings,
    _fetch_edgar_exhibit_text,
)

PARSE_MODEL = "claude-sonnet-4-20250514"


def _content_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8", errors="replace")).hexdigest()[:12]


def ingest_ticker(ticker: str, job_id: int | None = None, log_fn=None):
    """Full ingestion pipeline for a single ticker. Idempotent — skips already-ingested filings.

    Args:
        ticker: Stock ticker symbol (e.g., "SNOW")
        job_id: Optional ingestion_jobs.id for progress tracking
        log_fn: Optional callback for progress messages
    """
    def log(msg):
        print(msg)
        if log_fn:
            log_fn(msg)

    def update_progress(progress, message=""):
        if job_id:
            db.update_ingestion_job(job_id, progress=progress, message=message)

    # Mark job as running
    if job_id:
        db.update_ingestion_job(job_id, status="running", started_at=datetime.utcnow().isoformat())

    try:
        _do_ingest(ticker, log, update_progress)
        # Mark job as done
        if job_id:
            db.update_ingestion_job(
                job_id, status="done", progress=100,
                finished_at=datetime.utcnow().isoformat(),
                message="Ingestion complete",
            )
        # Update watchlist timestamp
        db.execute(
            "UPDATE watchlist SET last_ingested_at = now() WHERE ticker = %s",
            (ticker,),
        )
    except Exception as e:
        if job_id:
            db.update_ingestion_job(
                job_id, status="failed",
                finished_at=datetime.utcnow().isoformat(),
                error=str(e),
            )
        raise


def _do_ingest(ticker: str, log, update_progress):
    """Core ingestion logic."""
    # 1. Check what we already have
    latest_date = db.get_latest_filing_date(ticker)
    log(f"[{ticker}] Latest filing in DB: {latest_date or 'none'}")

    # 2. Fetch 8-K filing metadata
    update_progress(5, "Fetching 8-K metadata...")
    log(f"[{ticker}] Fetching 8-K filings from primary API...")
    filings = fetch_8k_filings(ticker)

    # Filter to only new filings if we have existing data
    if latest_date:
        filings = [f for f in filings if f.get("report_date", "") > latest_date]
        log(f"[{ticker}] {len(filings)} new filings since {latest_date}")

    # Determine earliest date from primary API for EDGAR supplementation
    fd_dates = sorted(f.get("report_date", "") for f in filings if f.get("report_date"))
    fd_earliest = fd_dates[0] if fd_dates else "9999-01-01"

    # EDGAR supplementation for older filings
    edgar_filings = []
    if not latest_date:  # Only fetch EDGAR on first ingestion
        cik = _get_cik_for_ticker(ticker)
        if cik:
            log(f"[{ticker}] Fetching older filings from SEC EDGAR...")
            all_edgar = _fetch_edgar_8k_filings(ticker, cik)
            edgar_filings = [f for f in all_edgar if f["filing_date"] < fd_earliest]
            log(f"[{ticker}] Found {len(edgar_filings)} additional EDGAR filings")

    update_progress(10, "Fetching exhibit texts...")

    # 3. Fetch exhibit texts
    filing_texts = []  # list of (filing_date, text)

    # EDGAR older filings (sequential)
    for i, filing in enumerate(edgar_filings):
        fd = filing.get("filing_date", "?")
        try:
            text = _fetch_edgar_exhibit_text(filing)
        except Exception as e:
            log(f"[{ticker}] EDGAR fetch failed for {fd}: {e}")
            text = None
        if text and is_earnings_8k_quick(text):
            filing_texts.append((filing["filing_date"], text))

    # Primary API filings (concurrent)
    total = len(filings)
    if total > 0:
        log(f"[{ticker}] Fetching {total} exhibit texts from primary API...")

        def _fetch_one(filing):
            acc = filing["accession_number"]
            try:
                text = fetch_exhibit_text(ticker, acc)
            except Exception:
                return None
            if text and is_earnings_8k_quick(text):
                return (filing.get("report_date", ""), text)
            return None

        done_count = [0]
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {pool.submit(_fetch_one, f): f for f in filings}
            for future in as_completed(futures):
                done_count[0] += 1
                pct = 10 + int(done_count[0] / total * 40)  # 10-50%
                update_progress(pct, f"Fetching texts ({done_count[0]}/{total})...")
                result = future.result()
                if result:
                    filing_texts.append(result)

    log(f"[{ticker}] {len(filing_texts)} earnings releases to parse")
    update_progress(50, "Detecting revenue metric...")

    # 4. Detect the company's primary revenue metric for consistency
    #    First check DB for an established metric from prior ingestions.
    #    If none exists (new ticker), probe the most recent filing to establish one.
    primary_metric = db.get_revenue_metric_for_ticker(ticker)
    if not primary_metric and filing_texts:
        # Sort by date descending, probe the latest filing
        probe_item = sorted(filing_texts, key=lambda x: x[0], reverse=True)[0]
        log(f"[{ticker}] No established metric — probing {probe_item[0]} to detect...")
        probe_result = llm_parse_filing(probe_item[1], ticker)
        if probe_result and probe_result.get("revenue_metric_name"):
            primary_metric = probe_result["revenue_metric_name"]
            log(f"[{ticker}] Detected primary revenue metric: {primary_metric}")
    if primary_metric:
        log(f"[{ticker}] Using revenue metric: {primary_metric}")

    update_progress(52, "Parsing filings with Claude AI...")

    # 5. Upload to S3 + LLM parse + store in DB
    total_parse = len(filing_texts)
    parse_done = [0]

    def _parse_and_store(item):
        filing_date, text = item
        c_hash = _content_hash(text)

        # Check if already in DB (idempotent)
        existing = db.query(
            "SELECT id FROM filings_parsed WHERE ticker = %s AND filing_date = %s AND content_hash = %s",
            (ticker, filing_date, c_hash),
        )
        if existing:
            return  # Already ingested

        # Upload raw text to S3
        try:
            s3_key = storage.upload_raw_text(ticker, filing_date, text)
        except Exception as e:
            log(f"[{ticker}] S3 upload failed for {filing_date}: {e}")
            s3_key = None

        # LLM parse with enforced metric
        result = llm_parse_filing(text, ticker, primary_revenue_metric=primary_metric)
        if not result:
            log(f"[{ticker}] LLM parse returned None for {filing_date}")
            return

        # Store in DB
        db.upsert_filing({
            "ticker": ticker,
            "filing_date": filing_date,
            "reported_quarter": result.get("reported_quarter"),
            "actual_revenue_millions": result.get("actual_revenue_millions"),
            "actual_non_gaap_op_margin_pct": result.get("actual_non_gaap_op_margin_pct"),
            "revenue_metric_name": result.get("revenue_metric_name"),
            "next_q_target": result.get("next_q_target"),
            "next_q_rev_guide_low_millions": result.get("next_q_rev_guide_low_millions"),
            "next_q_rev_guide_high_millions": result.get("next_q_rev_guide_high_millions"),
            "next_q_op_margin_guide_pct": result.get("next_q_op_margin_guide_pct"),
            "fy_target": result.get("fy_target"),
            "fy_rev_guide_low_millions": result.get("fy_rev_guide_low_millions"),
            "fy_rev_guide_high_millions": result.get("fy_rev_guide_high_millions"),
            "fy_rev_growth_low_pct": result.get("fy_rev_growth_low_pct"),
            "fy_rev_growth_high_pct": result.get("fy_rev_growth_high_pct"),
            "fy_op_margin_guide_pct": result.get("fy_op_margin_guide_pct"),
            "fy_fcf_margin_guide_pct": result.get("fy_fcf_margin_guide_pct"),
            "fy_eps_guide_low": result.get("fy_eps_guide_low"),
            "fy_eps_guide_high": result.get("fy_eps_guide_high"),
            "is_earnings_release": result.get("is_earnings_release", True),
            "s3_raw_path": s3_key,
            "content_hash": c_hash,
            "parse_model": PARSE_MODEL,
        })
        log(f"[{ticker}] Stored {filing_date} ({result.get('reported_quarter', '?')})")

    # Parse concurrently
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_parse_and_store, item): item for item in filing_texts}
        for future in as_completed(futures):
            parse_done[0] += 1
            pct = 50 + int(parse_done[0] / max(total_parse, 1) * 30)  # 50-80%
            update_progress(pct, f"Parsing ({parse_done[0]}/{total_parse})...")
            try:
                future.result()
            except Exception as e:
                item = futures[future]
                log(f"[{ticker}] Parse/store failed for {item[0]}: {e}")

    # 5. Fetch and store income statements
    update_progress(80, "Fetching income statements...")
    log(f"[{ticker}] Fetching income statements...")
    try:
        stmts = fetch_income_statements(ticker, limit=80)
        for s in stmts:
            fp = s.get("fiscal_period", "")
            if fp and not fp.startswith("FY") and not fp.startswith("CY"):
                fp = "FY" + fp
            db.upsert_income_statement({
                "ticker": ticker,
                "fiscal_period": fp,
                "revenue": s.get("revenue"),
                "gross_profit": s.get("gross_profit"),
                "operating_income": s.get("operating_income"),
                "net_income": s.get("net_income"),
            })
        log(f"[{ticker}] Stored {len(stmts)} income statements")
    except Exception as e:
        log(f"[{ticker}] Income statement fetch failed: {e}")

    # 6. Fetch and store stock prices around each filing date
    update_progress(90, "Fetching stock prices...")
    log(f"[{ticker}] Fetching stock prices around earnings dates...")
    all_filing_dates = db.query(
        "SELECT DISTINCT filing_date FROM filings_parsed WHERE ticker = %s",
        (ticker,),
    )
    price_rows = []
    for row in all_filing_dates:
        fd = row["filing_date"]
        fd_dt = datetime.strptime(str(fd), "%Y-%m-%d") if isinstance(fd, str) else fd
        start = (fd_dt - timedelta(days=10)).strftime("%Y-%m-%d")
        end = (fd_dt + timedelta(days=10)).strftime("%Y-%m-%d")
        try:
            prices = fetch_stock_prices(ticker, start, end)
            for p in prices:
                price_rows.append((ticker, p["time"][:10], p["close"]))
        except Exception:
            pass

    if price_rows:
        db.upsert_stock_prices_batch(price_rows)
        log(f"[{ticker}] Stored {len(price_rows)} stock price records")

    update_progress(100, "Done")
    log(f"[{ticker}] Ingestion complete")


def backfill_from_cache():
    """One-time: load previously cached LLM parse results into Neon."""
    cache_dir = Path.home() / ".cache" / "guidance_analyzer"
    if not cache_dir.exists():
        print("No cache directory found.")
        return

    files = list(cache_dir.glob("*.json"))
    print(f"Found {len(files)} cached files to backfill")

    for i, p in enumerate(files):
        try:
            # Parse filename: {ticker}_{date}_{hash}.json
            parts = p.stem.split("_")
            if len(parts) < 3:
                continue
            ticker = parts[0]
            filing_date = parts[1]
            c_hash = parts[2]

            data = json.loads(p.read_text())
            if not data.get("is_earnings_release", True):
                continue

            db.upsert_filing({
                "ticker": ticker,
                "filing_date": filing_date,
                "reported_quarter": data.get("reported_quarter"),
                "actual_revenue_millions": data.get("actual_revenue_millions"),
                "actual_non_gaap_op_margin_pct": data.get("actual_non_gaap_op_margin_pct"),
                "revenue_metric_name": data.get("revenue_metric_name"),
                "next_q_target": data.get("next_q_target"),
                "next_q_rev_guide_low_millions": data.get("next_q_rev_guide_low_millions"),
                "next_q_rev_guide_high_millions": data.get("next_q_rev_guide_high_millions"),
                "next_q_op_margin_guide_pct": data.get("next_q_op_margin_guide_pct"),
                "fy_target": data.get("fy_target"),
                "fy_rev_guide_low_millions": data.get("fy_rev_guide_low_millions"),
                "fy_rev_guide_high_millions": data.get("fy_rev_guide_high_millions"),
                "fy_rev_growth_low_pct": data.get("fy_rev_growth_low_pct"),
                "fy_rev_growth_high_pct": data.get("fy_rev_growth_high_pct"),
                "fy_op_margin_guide_pct": data.get("fy_op_margin_guide_pct"),
                "fy_fcf_margin_guide_pct": data.get("fy_fcf_margin_guide_pct"),
                "fy_eps_guide_low": data.get("fy_eps_guide_low"),
                "fy_eps_guide_high": data.get("fy_eps_guide_high"),
                "is_earnings_release": data.get("is_earnings_release", True),
                "s3_raw_path": None,
                "content_hash": c_hash,
                "parse_model": "cache-backfill",
            })

            if (i + 1) % 50 == 0:
                print(f"  Backfilled {i + 1}/{len(files)}...")
        except Exception as e:
            print(f"  Failed to backfill {p.name}: {e}")

    print(f"Backfill complete: processed {len(files)} files")


# ── CLI ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    if "--all-watchlist" in args:
        watchlist = db.get_watchlist()
        print(f"Ingesting {len(watchlist)} tickers from watchlist...")
        for entry in watchlist:
            t = entry["ticker"]
            print(f"\n{'='*60}")
            print(f"Ingesting {t}...")
            print(f"{'='*60}")
            try:
                ingest_ticker(t)
            except Exception as e:
                print(f"FAILED for {t}: {e}")
                continue
        print("\nAll done.")

    elif "--ticker" in args:
        idx = args.index("--ticker")
        if idx + 1 >= len(args):
            print("Usage: python -m ingest --ticker SNOW")
            sys.exit(1)
        ticker = args[idx + 1].upper()
        db.add_to_watchlist(ticker)
        ingest_ticker(ticker)

    elif "--backfill-cache" in args:
        backfill_from_cache()

    else:
        print("Usage:")
        print("  python -m ingest --ticker SNOW        # Ingest a single ticker")
        print("  python -m ingest --all-watchlist       # Ingest all watchlist tickers")
        print("  python -m ingest --backfill-cache      # Load cached parses into DB")
        sys.exit(1)
