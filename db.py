"""
Database connection layer for Neon Postgres.
Provides connection pooling and query/upsert helpers.
"""
from __future__ import annotations

import os
import psycopg2
import psycopg2.pool
import psycopg2.extras

_pool = None


def _get_database_url() -> str:
    """Read DATABASE_URL from Streamlit secrets or env var."""
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "DATABASE_URL" in st.secrets:
            return st.secrets["DATABASE_URL"]
    except Exception:
        pass
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError(
            "Missing DATABASE_URL. Set it in .streamlit/secrets.toml or as an env var."
        )
    return url


def get_pool():
    """Return a thread-safe connection pool (created once)."""
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=5,
            dsn=_get_database_url(),
            # Neon requires SSL
            sslmode="require",
        )
    return _pool


def query(sql: str, params=None) -> list[dict]:
    """Execute a SELECT and return rows as list of dicts."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        pool.putconn(conn)


def execute(sql: str, params=None):
    """Execute a non-SELECT statement (INSERT, UPDATE, DELETE)."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        pool.putconn(conn)


def execute_returning(sql: str, params=None) -> list[dict]:
    """Execute a statement with RETURNING clause."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(row) for row in cur.fetchall()]
        conn.commit()
        return rows
    finally:
        pool.putconn(conn)


def upsert_filing(data: dict):
    """Insert or update a parsed filing row."""
    sql = """
        INSERT INTO filings_parsed (
            ticker, filing_date, reported_quarter,
            actual_revenue_millions, actual_non_gaap_op_margin_pct,
            revenue_metric_name,
            next_q_target, next_q_rev_guide_low_millions, next_q_rev_guide_high_millions,
            next_q_op_margin_guide_pct,
            fy_target, fy_rev_guide_low_millions, fy_rev_guide_high_millions,
            fy_rev_growth_low_pct, fy_rev_growth_high_pct,
            fy_op_margin_guide_pct, fy_fcf_margin_guide_pct,
            fy_eps_guide_low, fy_eps_guide_high,
            is_earnings_release, s3_raw_path, content_hash, parse_model, parsed_at
        ) VALUES (
            %(ticker)s, %(filing_date)s, %(reported_quarter)s,
            %(actual_revenue_millions)s, %(actual_non_gaap_op_margin_pct)s,
            %(revenue_metric_name)s,
            %(next_q_target)s, %(next_q_rev_guide_low_millions)s, %(next_q_rev_guide_high_millions)s,
            %(next_q_op_margin_guide_pct)s,
            %(fy_target)s, %(fy_rev_guide_low_millions)s, %(fy_rev_guide_high_millions)s,
            %(fy_rev_growth_low_pct)s, %(fy_rev_growth_high_pct)s,
            %(fy_op_margin_guide_pct)s, %(fy_fcf_margin_guide_pct)s,
            %(fy_eps_guide_low)s, %(fy_eps_guide_high)s,
            %(is_earnings_release)s, %(s3_raw_path)s, %(content_hash)s, %(parse_model)s, now()
        )
        ON CONFLICT (ticker, filing_date, content_hash) DO UPDATE SET
            reported_quarter = EXCLUDED.reported_quarter,
            actual_revenue_millions = EXCLUDED.actual_revenue_millions,
            actual_non_gaap_op_margin_pct = EXCLUDED.actual_non_gaap_op_margin_pct,
            revenue_metric_name = EXCLUDED.revenue_metric_name,
            next_q_target = EXCLUDED.next_q_target,
            next_q_rev_guide_low_millions = EXCLUDED.next_q_rev_guide_low_millions,
            next_q_rev_guide_high_millions = EXCLUDED.next_q_rev_guide_high_millions,
            next_q_op_margin_guide_pct = EXCLUDED.next_q_op_margin_guide_pct,
            fy_target = EXCLUDED.fy_target,
            fy_rev_guide_low_millions = EXCLUDED.fy_rev_guide_low_millions,
            fy_rev_guide_high_millions = EXCLUDED.fy_rev_guide_high_millions,
            fy_rev_growth_low_pct = EXCLUDED.fy_rev_growth_low_pct,
            fy_rev_growth_high_pct = EXCLUDED.fy_rev_growth_high_pct,
            fy_op_margin_guide_pct = EXCLUDED.fy_op_margin_guide_pct,
            fy_fcf_margin_guide_pct = EXCLUDED.fy_fcf_margin_guide_pct,
            fy_eps_guide_low = EXCLUDED.fy_eps_guide_low,
            fy_eps_guide_high = EXCLUDED.fy_eps_guide_high,
            is_earnings_release = EXCLUDED.is_earnings_release,
            s3_raw_path = EXCLUDED.s3_raw_path,
            parse_model = EXCLUDED.parse_model,
            parsed_at = now()
    """
    execute(sql, data)


def upsert_income_statement(data: dict):
    """Insert or update an income statement row."""
    sql = """
        INSERT INTO income_statements (ticker, fiscal_period, revenue, gross_profit, operating_income, net_income)
        VALUES (%(ticker)s, %(fiscal_period)s, %(revenue)s, %(gross_profit)s, %(operating_income)s, %(net_income)s)
        ON CONFLICT (ticker, fiscal_period) DO UPDATE SET
            revenue = EXCLUDED.revenue,
            gross_profit = EXCLUDED.gross_profit,
            operating_income = EXCLUDED.operating_income,
            net_income = EXCLUDED.net_income,
            fetched_at = now()
    """
    execute(sql, data)


def upsert_stock_price(ticker: str, date: str, close: float):
    """Insert or update a stock price row."""
    sql = """
        INSERT INTO stock_prices (ticker, date, close)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker, date) DO UPDATE SET close = EXCLUDED.close
    """
    execute(sql, (ticker, date, close))


def upsert_stock_prices_batch(rows: list[tuple]):
    """Batch upsert stock prices. Each row: (ticker, date, close)."""
    if not rows:
        return
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO stock_prices (ticker, date, close)
                VALUES %s
                ON CONFLICT (ticker, date) DO UPDATE SET close = EXCLUDED.close
                """,
                rows,
                template="(%s, %s, %s)",
            )
        conn.commit()
    finally:
        pool.putconn(conn)


# ── Query helpers for the app ──────────────────────────────────────────

def get_watchlist() -> list[dict]:
    """Return all tickers in the watchlist."""
    return query("SELECT ticker, added_at, last_ingested_at FROM watchlist ORDER BY ticker")


def add_to_watchlist(ticker: str):
    """Add a ticker to the watchlist (idempotent)."""
    execute(
        "INSERT INTO watchlist (ticker) VALUES (%s) ON CONFLICT (ticker) DO NOTHING",
        (ticker,),
    )


def get_parsed_filings(ticker: str) -> list[dict]:
    """Get all parsed earnings filings for a ticker, ordered by date."""
    return query(
        "SELECT * FROM filings_parsed WHERE ticker = %s AND is_earnings_release = TRUE ORDER BY filing_date",
        (ticker,),
    )


def get_income_statements(ticker: str) -> list[dict]:
    """Get income statements for a ticker."""
    return query(
        "SELECT * FROM income_statements WHERE ticker = %s ORDER BY fiscal_period",
        (ticker,),
    )


def get_stock_prices(ticker: str) -> list[dict]:
    """Get all stored stock prices for a ticker."""
    return query(
        "SELECT date, close FROM stock_prices WHERE ticker = %s ORDER BY date",
        (ticker,),
    )


def get_stock_prices_range(ticker: str, start_date: str, end_date: str) -> list[dict]:
    """Get stock prices for a ticker within a date range."""
    return query(
        "SELECT date, close FROM stock_prices WHERE ticker = %s AND date BETWEEN %s AND %s ORDER BY date",
        (ticker, start_date, end_date),
    )


def get_latest_filing_date(ticker: str) -> str | None:
    """Get the most recent filing date for a ticker in the DB."""
    rows = query(
        "SELECT MAX(filing_date) as latest FROM filings_parsed WHERE ticker = %s",
        (ticker,),
    )
    if rows and rows[0]["latest"]:
        return str(rows[0]["latest"])
    return None


def get_fiscal_prefix_for_ticker(ticker: str) -> str | None:
    """Get the dominant CY/FY prefix used in reported_quarter for a ticker."""
    rows = query(
        """SELECT
             SUM(CASE WHEN reported_quarter LIKE 'FY%' THEN 1 ELSE 0 END) as fy_count,
             SUM(CASE WHEN reported_quarter LIKE 'CY%' THEN 1 ELSE 0 END) as cy_count
           FROM filings_parsed
           WHERE ticker = %s AND reported_quarter IS NOT NULL AND is_earnings_release = TRUE""",
        (ticker,),
    )
    if not rows:
        return None
    fy = rows[0]["fy_count"] or 0
    cy = rows[0]["cy_count"] or 0
    if fy == 0 and cy == 0:
        return None
    return "FY" if fy > cy else "CY"


def get_revenue_metric_for_ticker(ticker: str) -> str | None:
    """Get the most common revenue_metric_name from parsed filings for a ticker."""
    rows = query(
        """SELECT revenue_metric_name, COUNT(*) as cnt
           FROM filings_parsed
           WHERE ticker = %s AND revenue_metric_name IS NOT NULL AND is_earnings_release = TRUE
           GROUP BY revenue_metric_name
           ORDER BY cnt DESC
           LIMIT 1""",
        (ticker,),
    )
    if rows and rows[0]["revenue_metric_name"]:
        return rows[0]["revenue_metric_name"]
    return None


def ticker_exists_in_db(ticker: str) -> bool:
    """Check if a ticker has any parsed filings in the DB."""
    rows = query(
        "SELECT 1 FROM filings_parsed WHERE ticker = %s AND is_earnings_release = TRUE LIMIT 1",
        (ticker,),
    )
    return len(rows) > 0


# ── Ingestion job tracking ─────────────────────────────────────────────

def create_ingestion_job(ticker: str) -> int:
    """Create a new ingestion job, return its ID."""
    rows = execute_returning(
        "INSERT INTO ingestion_jobs (ticker, status) VALUES (%s, 'pending') RETURNING id",
        (ticker,),
    )
    return rows[0]["id"]


def update_ingestion_job(job_id: int, **kwargs):
    """Update an ingestion job's fields (status, progress, message, error)."""
    sets = []
    params = []
    for k, v in kwargs.items():
        if k in ("status", "progress", "message", "error", "started_at", "finished_at"):
            sets.append(f"{k} = %s")
            params.append(v)
    if not sets:
        return
    params.append(job_id)
    execute(f"UPDATE ingestion_jobs SET {', '.join(sets)} WHERE id = %s", params)


def get_active_ingestion_jobs() -> list[dict]:
    """Get all pending/running ingestion jobs."""
    return query(
        "SELECT * FROM ingestion_jobs WHERE status IN ('pending', 'running') ORDER BY created_at"
    )


def get_latest_ingestion_job(ticker: str) -> dict | None:
    """Get the most recent ingestion job for a ticker."""
    rows = query(
        "SELECT * FROM ingestion_jobs WHERE ticker = %s ORDER BY created_at DESC LIMIT 1",
        (ticker,),
    )
    return rows[0] if rows else None
