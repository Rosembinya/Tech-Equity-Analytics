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

# Load variables from .env file
load_dotenv()

def get_engine():
    return create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

def setup_database_schema(engine):
    """Cleans up old structures and builds the schema with Primary Keys."""
    print("Initializing database schema...")
    with engine.connect() as conn:
        # 1. Kill active sessions to prevent 'Table in use' errors
        kill_sessions_sql = text(f"""
            SELECT pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE datname = '{os.getenv('DB_NAME')}' AND pid <> pg_backend_pid();
        """)
        
        try:
            conn.execute(kill_sessions_sql)
            # 2. Drop in correct order (View first!)
            conn.execute(text("DROP VIEW IF EXISTS v_stock_analysis CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS raw_stocks CASCADE;"))
            
            # 3. Create Tables with Constraints
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
            conn.commit()
            print("Schema cleanup and table creation complete.")
        except Exception as e:
            print(f"Schema Setup Error: {e}")
            conn.execute(text("ROLLBACK;"))

def extract_and_load_stocks(engine):
    """Fetches data from yfinance and loads it into PostgreSQL."""
    try:
        # Dynamic ticker list from the database
        with engine.connect() as conn:
            result = conn.execute(text("SELECT ticker FROM ticker_metadata"))
            TICKERS = [row[0] for row in result]

        if not TICKERS:
            print("Error: No tickers found. Please seed 'ticker_metadata' table first.")
            return

        print(f"Starting data extraction for: {TICKERS}")
        all_data = []
        
        for ticker in TICKERS:
            try:
                print(f"Downloading {ticker}...")
                data = yf.download(ticker, start="2021-01-01", end="2026-01-17", progress=False)
                
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
            # Use 'append' instead of 'replace' because setup_database_schema already handled the table creation
            df.to_sql('raw_stocks', engine, if_exists='append', index=False, method='multi')

            # 4. Recreate the View with Analytics logic
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
            print("SUCCESS: Pipeline execution complete.")

    except Exception as e:
        print(f"\nPipeline Error: {e}")

if __name__ == "__main__":
    db_engine = get_engine()
    setup_database_schema(db_engine) # Build the structure
    extract_and_load_stocks(db_engine) # Load the data