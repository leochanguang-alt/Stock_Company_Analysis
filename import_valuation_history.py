import pandas as pd
import httpx
import os
from dotenv import load_dotenv
import numpy as np

def import_data():
    # Load credentials
    load_dotenv(dotenv_path='.env')
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found in .env")
        return

    # File path
    csv_path = "outputs/002508_mkt_cap_10y.csv"
    table_name = "stock_valuation_history"

    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return

    # 1. Read CSV
    print(f"Reading data from {csv_path}...")
    df = pd.read_csv(csv_path)

    # 2. Add fixed columns
    df['symbol'] = '002508'
    df['unit'] = 'bn'
    df['currency'] = 'cny'

    # 3. Add 'id' column (8-digit padded sequential index)
    df = df.reset_index()
    df['id'] = (df['index'] + 1).apply(lambda x: str(x).zfill(8))

    # 4. Prepare columns for database (Note the case-sensitive "Market_cap")
    df = df.rename(columns={'mkt_cap_billion_cny': 'Market_cap'})
    df = df[['id', 'symbol', 'date', 'Market_cap', 'unit', 'currency']]

    # 5. Convert to records and handle JSON compatibility
    df = df.replace([np.inf, -np.inf], np.nan)
    records = df.where(pd.notna(df), None).to_dict('records')

    print(f"Prepared {len(records)} records for insertion into {table_name}.")

    # 6. Insert into Supabase
    batch_size = 500
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"

    success_count = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            response = httpx.post(url, json=batch, headers=headers, timeout=60.0)
            if response.status_code in [200, 201]:
                success_count += len(batch)
                print(f"Successfully inserted batch {i//batch_size + 1}")
            else:
                print(f"Error in batch {i//batch_size + 1}: {response.status_code} - {response.text}")
                break
        except Exception as e:
            print(f"Exception in batch {i//batch_size + 1}: {e}")
            break

    print(f"\nTotal records successfully processed: {success_count}/{len(records)}")

if __name__ == "__main__":
    import_data()
