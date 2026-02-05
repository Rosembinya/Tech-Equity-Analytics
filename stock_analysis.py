#!/usr/bin/env python
# coding: utf-8

# In[ ]:

# In[ ]:


import os
import time
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text

# In[ ]:

#Load variables from .env file
load_dotenv()

# Extract Data from yfinance
def extract_and_load_stocks():
    try:
        # Database connection
        engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

     # Dynamic ticker list from the database
        with engine.connect() as conn:
            result = conn.execute(text("SELECT ticker FROM ticker_metadata"))
            TICKERS = [row[0] for row in result]

        if not TICKERS:
            print("Error: No tickers found in ticker_metadata table.")
            return

        print(f"Starting data extraction for {len(TICKERS)} tickers: {TICKERS}")
        all_data = []
        
        # Loop through each ticker and download data
        for ticker in TICKERS:
            try:
                print(f"Downloading {ticker}...")
                data = yf.download(ticker, start="2021-01-01", end="2026-01-17", progress=False)
                
                if not data.empty:
                    # Flatten MultiIndex columns if present
                    data.columns = [
                        col[0] if isinstance(col, tuple) else col 
                        for col in data.columns
                    ]
                    
                    # Select relevant columns and add Ticker
                    df_ticker = data[['Close', 'Volume']].reset_index()
                    df_ticker['Ticker'] = ticker
                    
                    # Rename columns
                    df_ticker.columns = ['Date', 'Price', 'Volume', 'Ticker']
                    
                    all_data.append(df_ticker)
                    print(f"  Successfully processed {ticker}")
                else:
                    print(f"  Warning: No data found for {ticker}")
                
            except Exception as e:
                print(f"  Error downloading {ticker}: {e}")
                
            time.sleep(1) 
            
        if not all_data:
            print("Error: No data fetched.")
            return
        
       # Combine and Load ticker data
        df = pd.concat(all_data, ignore_index=True)
        df.dropna(subset=['Price'], inplace=True)

        # Clean up existing tables and views     
        with engine.connect() as conn:
            kill_sessions_sql = f"""
            SELECT pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE datname = '{os.getenv('DB_NAME')}' AND pid <> pg_backend_pid();
            """
            try:
                conn.execute(text(kill_sessions_sql))
                conn.execute(text("DROP VIEW IF EXISTS v_stock_analysis CASCADE;"))
                conn.execute(text("DROP TABLE IF EXISTS raw_stocks CASCADE;"))
                conn.commit()
            except Exception as e:
                print(f"Non-critical lock error: {e}")
                conn.execute(text("ROLLBACK;"))
            
        print(f"\nUploading {len(df)} rows to PostgreSQL...")
        df.to_sql('raw_stocks', engine, if_exists='replace', index=False, method='multi')
        
        # Recreate the view
        with engine.connect() as conn:
            print("Creating database view: v_stock_analysis")
            view_query = text("""
            CREATE VIEW v_stock_analysis AS
            WITH calculations AS (
                SELECT 
                    rs."Date",
                    rs."Ticker",
                    rs."Price",
                    rs."Volume",
                    m.sector,
                    m.company_name,
                    
                -- Daily Return Percentage
                    ((rs."Price" - LAG(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date")) 
                      / NULLIF(LAG(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date"), 0)) * 100 AS daily_return_pct,
                    
                -- Simple Moving Averages
                    AVG(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date" ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
                    AVG(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date" ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma_200,
                    
                -- 52-Week High
                    MAX(rs."Price") OVER (PARTITION BY rs."Ticker" ORDER BY rs."Date" ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high_52week
                FROM raw_stocks rs
                LEFT JOIN ticker_metadata m ON rs."Ticker" = m.ticker
            )
            SELECT 
                *,
                -- Trend Signal based on SMA Crossover
                CASE 
                    WHEN sma_50 > sma_200 THEN 'Bullish'
                    WHEN sma_50 < sma_200 THEN 'Bearish'
                    ELSE 'Neutral'
                END AS trend_signal,
                
                -- Percentage from 52-Week High
                (("Price" - high_52week) / NULLIF(high_52week, 0)) * 100 AS pct_from_52wk_high
            FROM calculations;
            """)
            conn.execute(view_query)
            conn.commit()
            
        print("SUCCESS: Data extraction and loading complete.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    extract_and_load_stocks()