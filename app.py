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
Upload a CSV with keywords and fetch product counts from the Adeptmind Search API.
""")

# User Config
st.header("1. Configuration")
shop_id = st.text_input("Enter Shop ID (e.g., 'brooksbrothers')", "brooksbrothers")
environment = st.radio("Select Environment", ["prod", "staging"], index=0)

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
            st.warning("The uploaded file is empty.")
        else:
            found_col = find_keyword_column(df)
            if found_col:
                df_processed = df.rename(columns={found_col: "Keyword"})
                df_processed = df_processed[["Keyword"]].copy().reset_index(drop=True)
                st.success(f"Detected '{found_col}' as the keyword column.")
                st.dataframe(df_processed.head())
            else:
                st.error("Could not find a 'keyword' or 'keywords' column in the uploaded file.")
    except Exception as e:
        st.error(f"An error occurred while reading the CSV file: {e}")

# Async Logic
headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def search_query_async(row, session, remove_unnecessary_fields=True):
    """
    Core async function to fetch product counts.
    Mirrors the logic of the original script, including the two-step fallback.
    """
    query = row["Keyword"].strip()
    data = {
        "query": query,
        "size": 300,
    }

    if remove_unnecessary_fields:
        data["include_fields"] = ["product_id"]

    async with session.post(base_url, headers=headers, data=json.dumps(data)) as response:
        response.raise_for_status()  # Raise an error for non-2xx responses (e.g., 4xx, 5xx)
        response_json = await response.json()

        products = response_json.get("products", [])
        prod_count = len(products)

        if prod_count > 0:
            return prod_count

        # If the API timed out internally, raise an exception to trigger a retry
        if "timed_out_services" in response_json:
            raise asyncio.TimeoutError("API service timed out internally.")

        # If we got 0 products and were only asking for IDs, try again asking for all fields
        if remove_unnecessary_fields:
            return await search_query_async(row=row, session=session, remove_unnecessary_fields=False)
        
        # If we still have 0 products after the fallback, the count is truly 0
        return 0


async def wrapper(row, session):
    """
    Wrapper to catch any exception that escapes the retry mechanism
    and mark the row as failed (-1).
    """
    try:
        return await search_query_async(row, session)
    except Exception:
        return -1 # Mark as failed

async def process_data_chunk(data_chunk):
    """Processes a chunk of the DataFrame asynchronously."""
    async with aiohttp.ClientSession() as session:
        tasks = [wrapper(row, session) for _, row in data_chunk.iterrows()]
        results_chunk = await asyncio.gather(*tasks)
    return results_chunk

async def main_async_fetcher(data_df):
    """
    Main orchestrator for fetching data in chunks.
    """
    chunk_size = 1000  # Matched with original script
    total_rows = len(data_df)
    all_results = []

    st.subheader("3. Fetching Product Counts")
    bar = st.progress(0, text="Initializing...")
    status = st.empty()

    for i, start in enumerate(range(0, total_rows, chunk_size)):
        end = min(start + chunk_size, total_rows)
        chunk = data_df.iloc[start:end]

        status_text = f"Processing chunk {i+1} of {-(total_rows // -chunk_size)} (rows {start+1}-{end})..."
        status.text(status_text)
        bar.progress(start / total_rows, text=status_text)

        chunk_results = await process_data_chunk(chunk)
        all_results.extend(chunk_results)

        bar.progress(end / total_rows, text=status_text)

        if end < total_rows:
            sleep_time = random.randint(5, 15) # Matched with original script
            status.info(f"Chunk processed. Sleeping for {sleep_time} seconds to be polite to the API...")
            time.sleep(sleep_time)

    bar.empty()
    status.empty()
    return all_results

# Run Button
st.header("4. Run Process")

if st.button("🚀 Fetch Product Counts"):
    if df_processed is not None and 'Keyword' in df_processed.columns:
        with st.spinner("Fetching counts... This may take a while for large files."):
            results = asyncio.run(main_async_fetcher(df_processed))
            df_processed['Product Count'] = results

            st.success("✅ Processing Complete!")
            df_output = pd.DataFrame({
                'Serial Number': range(1, 1 + len(df_processed)),
                'Keyword': df_processed['Keyword'],
                'Product Count': df_processed['Product Count']
            })

            st.subheader("5. Results")
            st.dataframe(df_output)

            failed = (df_output['Product Count'] == -1).sum()
            if failed > 0:
                st.warning(f"Number of failed keywords: {failed} (marked as -1).")

            csv = df_output.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv,
                file_name=f"{shop_id}_{environment}_product_counts.csv",
                mime="text/csv",
            )
    else:
        st.error("Please upload a valid CSV file with a 'keyword' column before fetching.")