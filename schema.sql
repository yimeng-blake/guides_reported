-- Earnings Guidance Analyzer — Neon Postgres Schema
-- Run this once to set up the database.

-- Dynamic watchlist: tickers the system tracks
CREATE TABLE IF NOT EXISTS watchlist (
    ticker              TEXT PRIMARY KEY,
    added_at            TIMESTAMPTZ DEFAULT now(),
    last_ingested_at    TIMESTAMPTZ
);

-- Parsed earnings filings (one row per earnings release)
CREATE TABLE IF NOT EXISTS filings_parsed (
    id                              SERIAL PRIMARY KEY,
    ticker                          TEXT NOT NULL,
    filing_date                     DATE NOT NULL,
    reported_quarter                TEXT,
    actual_revenue_millions         DOUBLE PRECISION,
    actual_non_gaap_op_margin_pct   DOUBLE PRECISION,
    revenue_metric_name             TEXT,
    next_q_target                   TEXT,
    next_q_rev_guide_low_millions   DOUBLE PRECISION,
    next_q_rev_guide_high_millions  DOUBLE PRECISION,
    next_q_op_margin_guide_pct      DOUBLE PRECISION,
    fy_target                       TEXT,
    fy_rev_guide_low_millions       DOUBLE PRECISION,
    fy_rev_guide_high_millions      DOUBLE PRECISION,
    fy_rev_growth_low_pct           DOUBLE PRECISION,
    fy_rev_growth_high_pct          DOUBLE PRECISION,
    fy_op_margin_guide_pct          DOUBLE PRECISION,
    fy_fcf_margin_guide_pct         DOUBLE PRECISION,
    fy_eps_guide_low                DOUBLE PRECISION,
    fy_eps_guide_high               DOUBLE PRECISION,
    is_earnings_release             BOOLEAN DEFAULT TRUE,
    s3_raw_path                     TEXT,
    content_hash                    TEXT,
    parse_model                     TEXT,
    parsed_at                       TIMESTAMPTZ DEFAULT now(),
    UNIQUE(ticker, filing_date, content_hash)
);

-- Quarterly income statements (from financialdatasets.ai)
CREATE TABLE IF NOT EXISTS income_statements (
    id                  SERIAL PRIMARY KEY,
    ticker              TEXT NOT NULL,
    fiscal_period       TEXT NOT NULL,
    revenue             DOUBLE PRECISION,
    gross_profit        DOUBLE PRECISION,
    operating_income    DOUBLE PRECISION,
    net_income          DOUBLE PRECISION,
    fetched_at          TIMESTAMPTZ DEFAULT now(),
    UNIQUE(ticker, fiscal_period)
);

-- Daily stock prices (for earnings reaction calculations)
CREATE TABLE IF NOT EXISTS stock_prices (
    id      SERIAL PRIMARY KEY,
    ticker  TEXT NOT NULL,
    date    DATE NOT NULL,
    close   DOUBLE PRECISION NOT NULL,
    UNIQUE(ticker, date)
);

-- Track ingestion jobs (for UI progress display)
CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id              SERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    status          TEXT DEFAULT 'pending',
    progress        INT DEFAULT 0,
    message         TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_filings_ticker ON filings_parsed(ticker, filing_date);
CREATE INDEX IF NOT EXISTS idx_income_ticker ON income_statements(ticker);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON stock_prices(ticker, date);
CREATE INDEX IF NOT EXISTS idx_jobs_ticker_status ON ingestion_jobs(ticker, status);
