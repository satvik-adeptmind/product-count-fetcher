import streamlit as st
import pandas as pd
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
import time
import random
import nest_asyncio
nest_asyncio.apply()

# Streamlit Config
st.set_page_config(page_title="Product Count Fetcher", page_icon="📈", layout="wide")
st.title("🛍️ Product Count Fetcher for Adeptmind API")

st.markdown("""
Upload a CSV with keywords and fetch product counts from the Adeptmind Search API. (there's an change in backend, the kws with PC more than 100 will be shown as 100) will make appropriate changes later.
""")

# User Config
st.header("1. Configuration")
shop_id = st.text_input("Enter Shop ID (e.g., 'jacamo')", "jacamo")
environment = st.radio("Select Environment", ["prod", "staging"])

if environment == "prod":
    base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
else:
    base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"

# Upload CSV
st.header("2. Upload Your Keyword CSV")
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
df_processed = None

def find_keyword_column(df: pd.DataFrame) -> str | None:
    possible = ["keyword", "keywords"]
    for col in df.columns:
        if col.strip().lower() in possible:
            return col
    return None

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        if df.empty:
            st.warning("Empty file.")
        else:
            found_col = find_keyword_column(df)
            if found_col:
                df_processed = df.rename(columns={found_col: "Keyword"})
                df_processed = df_processed[["Keyword"]].copy().reset_index(drop=True)
                st.success(f"Detected '{found_col}' as keyword column.")
                st.dataframe(df_processed.head())
            else:
                st.error("Could not find keyword column.")
    except Exception as e:
        st.error(f"Error reading CSV: {e}")

# Async Logic

headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def search_query_async(row, session):
    query = row["Keyword"].strip()
    

    data = {
        "query": query,
        "size": 300,
        "include_fields": ["product_id"]
    }

    try:
        async with session.post(base_url, headers=headers, data=json.dumps(data)) as response:
            response.raise_for_status()
            response_json = await response.json()

            product_count = len(response_json.get("products", []))
            
            # This handles the case where the API times out internally
            if product_count == 0 and "timed_out_services" in response_json:
                raise asyncio.TimeoutError # This will trigger a retry

            return product_count

    except (asyncio.TimeoutError, aiohttp.ClientError):
        raise # Let tenacity handle the retry
    except Exception as e:
        st.error(f"Unexpected error for '{query}': {e}")
        return -1 # Mark as failed


async def wrapper(row, session):
    try:
        return await search_query_async(row, session)
    except Exception:
        return -1

async def process_data_chunk(data_chunk):
    results_chunk = []
    async with aiohttp.ClientSession() as session:
        tasks = [wrapper(row, session) for _, row in data_chunk.iterrows()]
        results_chunk = await asyncio.gather(*tasks)
    return results_chunk

async def main_async_fetcher(data_df):
    chunk_size = 500
    total_rows = len(data_df)
    all_results = []

    st.subheader("3. Fetching Product Counts")
    bar = st.progress(0)
    status = st.empty()

    for i, start in enumerate(range(0, total_rows, chunk_size)):
        end = min(start + chunk_size, total_rows)
        chunk = data_df.iloc[start:end]

        status.text(f"Processing chunk {i+1} ({start+1}-{end})...")
        chunk_results = await process_data_chunk(chunk)
        all_results.extend(chunk_results)

        bar.progress(end / total_rows)
        if end < total_rows:
            sleep_time = random.randint(1, 5)
            status.info(f"Sleeping {sleep_time}s...")
            time.sleep(sleep_time)

    bar.empty()
    status.empty()
    return all_results

# Run Button
st.header("4. Run Process")

if st.button("🚀 Fetch Product Counts"):
    if df_processed is not None and 'Keyword' in df_processed.columns:
        with st.spinner("Fetching counts..."):
            results = asyncio.run(main_async_fetcher(df_processed))
            df_processed['Product Count'] = results

            st.success("✅ Done!")
            df_output = pd.DataFrame({
                'Serial Number': range(1, 1 + len(df_processed)),
                'Keyword': df_processed['Keyword'],
                'Product Count': df_processed['Product Count']
            })

            st.subheader("5. Results")
            st.dataframe(df_output.head(100))

            failed = (df_output['Product Count'] == -1).sum()
            if failed > 0:
                st.warning(f"{failed} keywords failed (marked -1).")

            csv = df_output.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"{shop_id}_{environment}_product_counts.csv",
                mime="text/csv",
            )
    else:
        st.error("Upload a valid CSV with a keyword column first.")