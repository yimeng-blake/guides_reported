"""
Re-parse raw filing text from S3 using an updated LLM prompt.
Use this when you've improved the parse prompt and want to regenerate all parsed data.

Usage:
    python reparse.py --ticker SNOW        # Re-parse one ticker
    python reparse.py --all                # Re-parse everything
    python reparse.py --dry-run --ticker SNOW  # Preview what would be re-parsed
"""

import sys

import db
import storage
from earnings_guidance_analyzer import llm_parse_filing

PARSE_MODEL = "claude-sonnet-4-20250514"


def reparse_ticker(ticker: str, dry_run: bool = False):
    """Re-parse all filings for a ticker from S3 raw text."""
    filings = db.query(
        "SELECT id, filing_date, s3_raw_path, content_hash FROM filings_parsed WHERE ticker = %s AND s3_raw_path IS NOT NULL ORDER BY filing_date",
        (ticker,),
    )

    if not filings:
        print(f"[{ticker}] No filings with S3 raw text found")
        return

    print(f"[{ticker}] Re-parsing {len(filings)} filings...")

    for i, f in enumerate(filings):
        filing_date = str(f["filing_date"])
        s3_key = f["s3_raw_path"]
        print(f"  [{i+1}/{len(filings)}] {filing_date} — {s3_key}")

        if dry_run:
            continue

        # Download raw text from S3
        try:
            text = storage.download_raw_text(s3_key)
        except Exception as e:
            print(f"    S3 download failed: {e}")
            continue

        # Re-parse with current prompt
        result = llm_parse_filing(text, ticker)
        if not result:
            print(f"    LLM parse returned None")
            continue

        # Update DB row
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
            "content_hash": f["content_hash"],
            "parse_model": PARSE_MODEL,
        })
        print(f"    Updated: {result.get('reported_quarter', '?')}")

    print(f"[{ticker}] Re-parse complete")


if __name__ == "__main__":
    args = sys.argv[1:]
    dry_run = "--dry-run" in args

    if "--all" in args:
        tickers = [r["ticker"] for r in db.get_watchlist()]
        print(f"Re-parsing {len(tickers)} tickers...")
        for t in tickers:
            reparse_ticker(t, dry_run=dry_run)
    elif "--ticker" in args:
        idx = args.index("--ticker")
        if idx + 1 >= len(args):
            print("Usage: python reparse.py --ticker SNOW")
            sys.exit(1)
        reparse_ticker(args[idx + 1].upper(), dry_run=dry_run)
    else:
        print("Usage:")
        print("  python reparse.py --ticker SNOW")
        print("  python reparse.py --all")
        print("  python reparse.py --dry-run --ticker SNOW")
        sys.exit(1)
