#!/usr/bin/env python
# coding: utf-8

import os
import time
import logging
import yaml
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime

# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Config helpers

load_dotenv()

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")

def load_config() -> dict:
    """Load pipeline configuration from config.yaml."""
    with open(CONFIG_PATH, "r") as fh:
        cfg = yaml.safe_load(fh)
    log.info("Config loaded from %s", CONFIG_PATH)
    return cfg

def get_engine(cfg: dict):
    """Build a SQLAlchemy engine from environment variables."""
    url = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url, pool_pre_ping=True)

# Phase 1 — Schema setup

def setup_database_schema(engine, cfg: dict) -> None:
    log.info("--- Phase 1: Setting up Database Schema ---")
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS v_stock_analysis CASCADE;"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ticker_metadata (
                ticker       VARCHAR(10) PRIMARY KEY,
                company_name VARCHAR(100),
                sector       VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS raw_stocks (
                "Date"   DATE,
                "Ticker" VARCHAR(10),
                "Price"  NUMERIC,
                "Volume" BIGINT,
                PRIMARY KEY ("Date", "Ticker")
            );
        """))

        # Seed tickers from config, skipping existing rows
        existing = conn.execute(
            text("SELECT ticker FROM ticker_metadata")
        ).scalars().all()
        existing_set = set(existing)

        new_tickers = [
            t for t in cfg.get("tickers", [])
            if t["ticker"] not in existing_set
        ]
        if new_tickers:
            log.info("Seeding %d new ticker(s) into ticker_metadata", len(new_tickers))
            conn.execute(
                text("""
                    INSERT INTO ticker_metadata (ticker, company_name, sector)
                    VALUES (:ticker, :company_name, :sector)
                    ON CONFLICT (ticker) DO NOTHING
                """),
                new_tickers,
            )
        else:
            log.info("ticker_metadata already seeded — nothing to add.")

    log.info("Database schema setup complete.")

# Phase 2 — Extract, Transform, Load
 
def download_with_retry(ticker: str, start: str, end: str, retries: int = 3) -> pd.DataFrame:
    """Download ticker data from yfinance with exponential back-off."""
    for attempt in range(1, retries + 1):
        try:
            data = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if not data.empty:
                return data
            log.warning("%s: empty data on attempt %d", ticker, attempt)
        except Exception as exc:
            log.warning("%s: download error on attempt %d — %s", ticker, attempt, exc)
        if attempt < retries:
            sleep_secs = 2 ** attempt
            log.info("Retrying %s in %ds …", ticker, sleep_secs)
            time.sleep(sleep_secs)
    return pd.DataFrame()


def extract_and_load_stocks(engine, cfg: dict) -> None:
    """Download, transform, and upsert stock data; then rebuild analytical view."""
    log.info("--- Phase 2: Extracting, Transforming, and Loading Stock Data ---")

    today = datetime.now().strftime("%Y-%m-%d")
    start_date = cfg.get("start_date", "2021-01-01")

    # Pull ticker list from DB
    with engine.connect() as conn:
        tickers = conn.execute(text("SELECT ticker FROM ticker_metadata")).scalars().all()

    if not tickers:
        log.error("No tickers in ticker_metadata. Aborting ETL.")
        return

    log.info("Target tickers: %s", tickers)
    all_frames = []

    for ticker in tickers:
        log.info("Downloading %s …", ticker)
        data = download_with_retry(ticker, start=start_date, end=today)

        if data.empty:
            log.warning("Skipping %s — no data retrieved.", ticker)
            continue

        # Flatten MultiIndex columns
        data.columns = [
            col[0] if isinstance(col, tuple) else col for col in data.columns
        ]

        df_ticker = (
            data[["Close", "Volume"]]
            .reset_index()
            .rename(columns={"Date": "Date", "Close": "Price", "Volume": "Volume"})
        )
        df_ticker["Ticker"] = ticker
        df_ticker = df_ticker[["Date", "Ticker", "Price", "Volume"]]
        all_frames.append(df_ticker)

        time.sleep(cfg.get("download_sleep_secs", 1))

    if not all_frames:
        log.error("No data downloaded for any ticker. Aborting load.")
        return

    df = pd.concat(all_frames, ignore_index=True)
    df.dropna(subset=["Price"], inplace=True)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date  # strip timezone info

    # Upsert into raw_stocks
    log.info("Upserting %d rows into raw_stocks …", len(df))
    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw_stocks ("Date", "Ticker", "Price", "Volume")
                VALUES (:Date, :Ticker, :Price, :Volume)
                ON CONFLICT ("Date", "Ticker") DO UPDATE
                    SET "Price"  = EXCLUDED."Price",
                        "Volume" = EXCLUDED."Volume"
            """),
            rows,
        )
    log.info("Upsert complete.")

    # (Re)create analytical view
    _create_analytical_view(engine)
    log.info("SUCCESS: All data loaded and view created.")


def _create_analytical_view(engine) -> None:
    """Drop and recreate the v_stock_analysis view."""
    log.info("Rebuilding analytical view: v_stock_analysis")
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS v_stock_analysis CASCADE;"))
        conn.execute(text("""
            CREATE VIEW v_stock_analysis AS
            WITH calculations AS (
                SELECT
                    rs."Date",
                    rs."Ticker",
                    rs."Price",
                    rs."Volume",
                    m.sector,
                    m.company_name,
                    ROUND(
                        ((rs."Price" - LAG(rs."Price") OVER w)
                         / NULLIF(LAG(rs."Price") OVER w, 0)) * 100,
                        4
                    ) AS daily_return_pct,
                    AVG(rs."Price") OVER (
                        PARTITION BY rs."Ticker"
                        ORDER BY rs."Date"
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma_50,
                    AVG(rs."Price") OVER (
                        PARTITION BY rs."Ticker"
                        ORDER BY rs."Date"
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS sma_200,
                    MAX(rs."Price") OVER (
                        PARTITION BY rs."Ticker"
                        ORDER BY rs."Date"
                        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                    ) AS high_52week
                FROM raw_stocks rs
                LEFT JOIN ticker_metadata m ON rs."Ticker" = m.ticker
                WINDOW w AS (PARTITION BY rs."Ticker" ORDER BY rs."Date")
            )
            SELECT *,
                CASE
                    WHEN sma_50 > sma_200 THEN 'Bullish'
                    WHEN sma_50 < sma_200 THEN 'Bearish'
                    ELSE 'Neutral'
                END AS trend_signal,
                ROUND(
                    (("Price" - high_52week) / NULLIF(high_52week, 0)) * 100,
                    2
                ) AS pct_from_52wk_high
            FROM calculations;
        """))
    log.info("v_stock_analysis created successfully.")


if __name__ == "__main__":
    cfg = load_config()
    db_engine = get_engine(cfg)
    setup_database_schema(db_engine, cfg)
    extract_and_load_stocks(db_engine, cfg)