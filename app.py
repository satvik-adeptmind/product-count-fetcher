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

# --- Core Logic (Async Fetching) ---
# This part is independent of the UI and can be reused by both tabs
headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def search_query_async(row, session, base_url, remove_unnecessary_fields=True):
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
        response.raise_for_status()
        response_json = await response.json()

        products = response_json.get("products", [])
        prod_count = len(products)

        if prod_count > 0:
            return prod_count

        if "timed_out_services" in response_json:
            raise asyncio.TimeoutError("API service timed out internally.")

        if remove_unnecessary_fields:
            return await search_query_async(row=row, session=session, base_url=base_url, remove_unnecessary_fields=False)
        
        return 0

async def wrapper(row, session, base_url):
    """
    Wrapper to catch any exception that escapes the retry mechanism
    and mark the row as failed (-1).
    """
    try:
        return await search_query_async(row, session, base_url)
    except Exception:
        return -1

async def process_data_chunk(data_chunk, base_url):
    """Processes a chunk of the DataFrame asynchronously."""
    async with aiohttp.ClientSession() as session:
        tasks = [wrapper(row, session, base_url) for _, row in data_chunk.iterrows()]
        results_chunk = await asyncio.gather(*tasks)
    return results_chunk

async def main_async_fetcher(data_df, base_url):
    """
    Main orchestrator for fetching data in chunks.
    """
    chunk_size = 1000
    total_rows = len(data_df)
    all_results = []

    st.subheader("Fetching Product Counts...")
    bar = st.progress(0, text="Initializing...")
    status = st.empty()

    for i, start in enumerate(range(0, total_rows, chunk_size)):
        end = min(start + chunk_size, total_rows)
        chunk = data_df.iloc[start:end]

        status_text = f"Processing chunk {i+1} of {-(total_rows // -chunk_size)} (rows {start+1}-{end})..."
        status.text(status_text)
        bar.progress(start / total_rows, text=status_text)

        chunk_results = await process_data_chunk(chunk, base_url)
        all_results.extend(chunk_results)

        bar.progress(end / total_rows, text=status_text)

        if end < total_rows:
            sleep_time = random.randint(5, 15)
            status.info(f"Chunk processed. Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)

    bar.empty()
    status.empty()
    return all_results

# --- UI Layout ---

# Configuration Section (Common for both tabs)
st.header("1. Configuration")
shop_id = st.text_input("Enter Shop ID (e.g., 'brooksbrothers')", "brooksbrothers")
environment = st.radio("Select Environment", ["prod", "staging"], index=0)

if environment == "prod":
    base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
else:
    base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"

# Create Tabs
tab1, tab2 = st.tabs(["📁 Upload CSV", "📋 Paste Keywords"])

# --- Tab 1: Upload CSV ---
with tab1:
    st.header("2. Upload Your Keyword CSV")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv", key="csv_uploader")
    
    # Session state to hold the dataframe from CSV
    if 'df_from_csv' not in st.session_state:
        st.session_state.df_from_csv = None

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
                    st.session_state.df_from_csv = df_processed
                    st.success(f"Detected '{found_col}' as the keyword column. Found {len(df_processed)} keywords.")
                    st.dataframe(df_processed.head())
                else:
                    st.error("Could not find a 'keyword' or 'keywords' column in the uploaded file.")
        except Exception as e:
            st.error(f"An error occurred while reading the CSV file: {e}")

# --- Tab 2: Paste Keywords ---
with tab2:
    st.header("2. Paste Your Keywords")
    st.markdown("Enter keywords, one per line.")
    
    # Session state to hold the dataframe from text area
    if 'df_from_paste' not in st.session_state:
        st.session_state.df_from_paste = None

    keyword_text = st.text_area("Keywords", height=250, placeholder="shirt\nblue pants\nred dress")
    
    if keyword_text:
        keywords = [kw.strip() for kw in keyword_text.split('\n') if kw.strip()]
        if keywords:
            df_paste = pd.DataFrame({"Keyword": keywords})
            st.session_state.df_from_paste = df_paste
            st.info(f"Detected {len(df_paste)} keywords.")
        else:
            st.session_state.df_from_paste = None


# --- Common Run and Display Logic ---

st.header("3. Run Process")

active_df = None
source = None

# Determine which dataframe to use based on user interaction
if st.session_state.df_from_csv is not None:
    active_df = st.session_state.df_from_csv
    source = 'csv'
elif st.session_state.df_from_paste is not None:
    active_df = st.session_state.df_from_paste
    source = 'paste'


if st.button("🚀 Fetch Product Counts", disabled=(active_df is None)):
    if active_df is not None:
        with st.spinner("Fetching counts... This may take a while."):
            results = asyncio.run(main_async_fetcher(active_df, base_url))
            active_df['Product Count'] = results

            st.success("✅ Processing Complete!")
            
            df_output = pd.DataFrame({
                'Serial Number': range(1, 1 + len(active_df)),
                'Keyword': active_df['Keyword'],
                'Product Count': active_df['Product Count']
            })

            st.subheader("4. Results")

            failed = (df_output['Product Count'] == -1).sum()
            if failed > 0:
                st.warning(f"Number of failed keywords: {failed} (marked as -1).")
            
            # Display logic for pasted keywords
            if source == 'paste':
                if len(df_output) > 20: # Display top/bottom for many keywords
                    st.markdown("Displaying top 10 and bottom 10 results.")
                    st.dataframe(pd.concat([df_output.head(10), df_output.tail(10)]))
                else:
                    st.dataframe(df_output)
                
                # Prepare data for clipboard, but hide it in an expander to avoid a huge scroll area
                with st.expander("📋 Show Copyable Text for All Results"):
                    copy_text = df_output[['Product Count']].to_csv(sep='\t', index=False, header=False)
                    st.code(copy_text, language="")
                    st.info("👆 You can copy the text above (it's formatted for easy pasting into sheets).")

            else: # Display logic for CSV upload
                st.dataframe(df_output)

            # Download button for both
            csv_data = df_output.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Full Results as CSV",
                data=csv_data,
                file_name=f"{shop_id}_{environment}_product_counts.csv",
                mime="text/csv",
                key=f"{source}_download"
            )
    else:
        st.error("Please provide keywords either by uploading a CSV or pasting them.")