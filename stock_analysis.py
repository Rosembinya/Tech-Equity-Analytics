#!/usr/bin/env python
# coding: utf-8

# In[ ]:

# In[ ]:


import os
import time
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime

# Load variables from .env file
load_dotenv()

def get_engine():
    return create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

def setup_database_schema(engine):
    """Initializes the database schema by dropping existing tables/views and creating new ones with constraints."""
    print("--- Phase 1: Setting up Database Schema ---")
    with engine.connect() as conn:
        # 1. Terminate active sessions to allow dropping tables/views
        kill_sessions_sql = text(f"""
            SELECT pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE datname = '{os.getenv('DB_NAME')}' AND pid <> pg_backend_pid();
        """)
        
        try:
            conn.execute(kill_sessions_sql)
            # 2. Drop existing tables and views if they exist
            conn.execute(text("DROP VIEW IF EXISTS v_stock_analysis CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS raw_stocks CASCADE;"))
            
            # 3. Create tables with constraints
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ticker_metadata (
                    ticker VARCHAR(10) PRIMARY KEY,
                    company_name VARCHAR(100),
                    sector VARCHAR(50)
                );
                
                CREATE TABLE IF NOT EXISTS raw_stocks (
                    "Date" DATE,
                    "Ticker" VARCHAR(10),
                    "Price" NUMERIC,
                    "Volume" BIGINT,
                    PRIMARY KEY ("Date", "Ticker")
                );
            """))

            # 4. Seed ticker_metadata with Top 10 Tech Stocks if empty
            check_meta = conn.execute(text("SELECT COUNT(*) FROM ticker_metadata")).scalar()
            if check_meta == 0:
                print("Seeding ticker_metadata with top 10 tech stocks...")
                seed_data = [
                    {'t': 'AAPL', 'n': 'Apple Inc.', 's': 'Technology Hadware'},
                    {'t': 'MSFT', 'n': 'Microsoft Corp.', 's': 'Software'},
                    {'t': 'NVDA', 'n': 'NVIDIA Corp.', 's': 'Semiconductors'},
                    {'t': 'GOOGL', 'n': 'Alphabet Inc.', 's': 'Internet Services'},
                    {'t': 'AMZN', 'n': 'Amazon.com Inc.', 's': 'Consumer Discretionary'},
                    {'t': 'META', 'n': 'Meta Platforms Inc.', 's': 'Internet Services'},
                    {'t': 'TSLA', 'n': 'Tesla Inc.', 's': 'Consumer Discretionary'},
                    {'t': 'AVGO', 'n': 'Broadcom Inc.', 's': 'Semiconductors'},
                    {'t': 'ORCL', 'n': 'Oracle Corp.', 's': 'Software'},
                    {'t': 'TSM', 'n': 'Taiwan Semiconductor Manufacturing Co. Ltd.', 's': 'Semiconductors'}
                ]
                conn.execute(
                    text("INSERT INTO ticker_metadata (ticker, company_name, sector) VALUES (:t, :n, :s)"),
                    seed_data
                )
            
            conn.commit()
            print("Database schema setup complete.")
        except Exception as e:
            print(f"Schema Setup Error: {e}")
            conn.execute(text("ROLLBACK;"))

def extract_and_load_stocks(engine):
    """Extracts stock data for tickers in the database, transforms it, and loads it into the raw_stocks table. Also creates an analytical view."""
    print("\n--- Phase 2: Extracting, Transforming, and Loading Stock Data ---")
    
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        # Dynamic ticker list from the database
        with engine.connect() as conn:
            result = conn.execute(text("SELECT ticker FROM ticker_metadata"))
            TICKERS = [row[0] for row in result]

        if not TICKERS:
            print("No tickers found in ticker_metadata. Seeding failed or empty.")
            return

        print(f"Target Tickers: {TICKERS}")
        all_data = []
        
        for ticker in TICKERS:
            try:
                print(f"Downloading {ticker}...")
                data = yf.download(ticker, start="2021-01-01", end=today, progress=False)
                
                if not data.empty:
                    # Flatten MultiIndex and Clean
                    data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
                    df_ticker = data[['Close', 'Volume']].reset_index()
                    df_ticker['Ticker'] = ticker
                    df_ticker.columns = ['Date', 'Price', 'Volume', 'Ticker']
                    all_data.append(df_ticker)
                
            except Exception as e:
                print(f"Error downloading {ticker}: {e}")
            time.sleep(1) 
            
        if all_data:
            df = pd.concat(all_data, ignore_index=True)
            df.dropna(subset=['Price'], inplace=True)

            print(f"Uploading {len(df)} rows to 'raw_stocks'...")
            # Use method='multi' for batch inserts to improve performance
            df.to_sql('raw_stocks', engine, if_exists='append', index=False, method='multi')

            # 5. Create analytical view with calculations
            with engine.connect() as conn:
                print("Creating analytical view: v_stock_analysis")
                view_query = text("""
                CREATE VIEW v_stock_analysis AS
                WITH calculations AS (
                    SELECT 
                        rs."Date", rs."Ticker", rs."Price", rs."Volume",
                        m.sector, m.company_name,
                        ((rs."Price" - LAG(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date")) 
                          / NULLIF(LAG(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date"), 0)) * 100 AS daily_return_pct,
                        AVG(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date" ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
                        AVG(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date" ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma_200,
                        MAX(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date" ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high_52week
                    FROM raw_stocks rs
                    LEFT JOIN ticker_metadata m ON rs."Ticker" = m.ticker
                )
                SELECT *,
                    CASE WHEN sma_50 > sma_200 THEN 'Bullish' WHEN sma_50 < sma_200 THEN 'Bearish' ELSE 'Neutral' END AS trend_signal,
                    (("Price" - high_52week) / NULLIF(high_52week, 0)) * 100 AS pct_from_52wk_high
                FROM calculations;
                """)
                conn.execute(view_query)
                conn.commit()
            print("\nSUCCESS: All data loaded and view created.")

    except Exception as e:
        print(f"\nPipeline Error: {e}")

if __name__ == "__main__":
    db_engine = get_engine()
    setup_database_schema(db_engine) 
    extract_and_load_stocks(db_engine)