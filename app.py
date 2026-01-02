import streamlit as st
import pandas as pd
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
import time
import random
import nest_asyncio
import string
import matplotlib.colors as mcolors
import re

nest_asyncio.apply()

# Streamlit Config
st.set_page_config(page_title="Product Count Fetcher & Utilities", page_icon="🛠️", layout="wide")
st.title("🛍️ Product Count Fetcher & Utilities")


def is_valid_keyword(keyword: str) -> bool:
    """
    Checks if a keyword contains only ASCII "normal text".
    """
    if not isinstance(keyword, str) or not keyword.strip():
        return False
    allowed_chars = string.ascii_letters + string.digits + string.whitespace + "'-" + '"'
    allowed_set = set(allowed_chars)
    for char in keyword:
        if char not in allowed_set:
            return False
    return True

def validate_url(url: str) -> tuple[bool, str]:
    """
    Validates a URL based on specific formatting rules.
    Returns a tuple: (is_valid: bool, reason: str).
    """
    url = url.strip()
    if not url:
        return (False, "URL is empty.")

    if not url.startswith("https://"):
        return (False, "Does not start with 'https://'.")

    if '--' in url:
        return (False, "Contains consecutive hyphens ('--').")

    for char in url:
        is_allowed_symbol = char in "-_.:/"
        if not (char.isalpha() or char.isdigit() or is_allowed_symbol):
            return (False, f"Contains invalid character: '{char}'.")

    return (True, "Valid")

# --- Color Logic Helper (UPDATED) ---
@st.cache_resource
def get_color_regex():
    """
    Builds and compiles the regex pattern for color detection based on CSS4 colors
    and specific fashion additions. Cached to improve performance.
    """
    # 1. BUILD THE COLOR LIST DYNAMICALLY
    base_colors = set(mcolors.CSS4_COLORS.keys())
    
    # UPDATED: Comprehensive Fashion/Sneaker Color List
    fashion_additions = {
        # Metals & Finishes
        'volt', 'metallic', 'iridescent', 'neon', 'platinum', 'gold', 'silver', 
        'bronze', 'copper', 'chrome', 'reflective', 'holographic',
        
        # Sneaker/Retail Specifics (The "Missing" ones)
        'chalk', 'gum', 'bone', 'sand', 'rust', 'clay', 'mint', 'peach', 'nude', 
        'berry', 'wine', 'mauve', 'lilac', 'mustard', 'olive', 'sage', 'taupe',
        'camel', 'cognac', 'ochre', 'terracotta', 'burgundy', 'maroon', 'navy',
        'cream', 'ivory', 'champagne', 'anthracite', 'charcoal', 'graphite',
        'infrared', 'solar', 'crystal', 'onyx', 'obsidian', 'emerald', 'sapphire',
        
        # Compound/Descriptive
        'multicolor', 'multi-color', 'rainbow', 'tie-dye', 'camo', 'camouflage',
        'off-white', 'off white', 'rose gold', 'baby blue', 'navy blue'
    }
    
    all_colors = base_colors.union(fashion_additions)

    # 2. CREATE OPTIMIZED REGEX
    # Sort by length (longest first) to match "navy blue" before "blue"
    sorted_colors = sorted(list(all_colors), key=len, reverse=True)
    
    # Escape special regex characters and join with OR operator |
    # We use \b to ensure we match "red" but not "scared"
    pattern_str = r'\b(' + '|'.join(re.escape(c) for c in sorted_colors) + r')\b'
    
    # Compile regex
    return re.compile(pattern_str, re.IGNORECASE)

# --- Core Logic (Async Fetching) ---
headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def search_query_async(row, session, base_url, remove_unnecessary_fields=True):
    query = row["Keyword"].strip()
    data = {"query": query, "size": 300}
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
    try:
        return await search_query_async(row, session, base_url)
    except Exception:
        return -1

async def process_data_chunk(data_chunk, base_url):
    async with aiohttp.ClientSession() as session:
        tasks = [wrapper(row, session, base_url) for _, row in data_chunk.iterrows()]
        return await asyncio.gather(*tasks)

async def main_async_fetcher(data_df, base_url):
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

# Configuration Section
st.sidebar.header("Configuration (Product Counter)")
shop_id = st.sidebar.text_input("Enter Shop ID", "brooksbrothers")
environment = st.sidebar.radio("Select Environment", ["prod", "staging"], index=0)

if environment == "prod":
    base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
else:
    base_url = f"https://search-pre-prod-dlp-adept-search.search-pre-prod.adeptmind.app/search?shop_id={shop_id}"

# --- Create Tabs ---
tab1, tab2, tab_kw_validator, tab_url_validator, tab_color_id = st.tabs([
    "📁 Product Counts (Upload CSV)", 
    "📋 Product Counts (Paste Keywords)", 
    "🔎 Keyword Validator",
    "🔗 URL Validator",
    "🎨 Color Identifier"
])

# --- Tab 1: Upload CSV ---
with tab1:
    st.header("Upload CSV to Fetch Product Counts")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv", key="csv_uploader")
    if 'df_from_csv' not in st.session_state: st.session_state.df_from_csv = None
    def find_keyword_column(df: pd.DataFrame) -> str | None:
        possible = ["keyword", "keywords"];
        for col in df.columns:
            if col.strip().lower() in possible: return col
        return None
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            if not df.empty:
                found_col = find_keyword_column(df)
                if found_col:
                    df_processed = df.rename(columns={found_col: "Keyword"})[["Keyword"]].copy().reset_index(drop=True)
                    st.session_state.df_from_csv = df_processed
                    st.success(f"Detected '{found_col}' as keyword column. Found {len(df_processed)} keywords.")
                    st.dataframe(df_processed.head())
                else: st.error("Could not find a 'keyword' or 'keywords' column.")
            else: st.warning("Uploaded file is empty.")
        except Exception as e: st.error(f"Error reading CSV: {e}")

# --- Tab 2: Paste Keywords ---
with tab2:
    st.header("Paste Keywords to Fetch Product Counts")
    if 'df_from_paste' not in st.session_state: st.session_state.df_from_paste = None
    keyword_text = st.text_area("Keywords", height=250, placeholder="shirt\nblue pants", key="pasted_keywords_fetcher")
    if keyword_text:
        keywords = [kw.strip() for kw in keyword_text.split('\n') if kw.strip()]
        if keywords:
            df_paste = pd.DataFrame({"Keyword": keywords}); st.session_state.df_from_paste = df_paste
            st.info(f"Detected {len(df_paste)} keywords.")
        else: st.session_state.df_from_paste = None

# --- Common Run and Display Logic (for Product Counter) ---
active_df = None; source = None
if st.session_state.df_from_csv is not None: active_df = st.session_state.df_from_csv; source = 'csv'
elif st.session_state.df_from_paste is not None: active_df = st.session_state.df_from_paste; source = 'paste'

if st.button("🚀 Fetch Product Counts", disabled=(active_df is None)):
    if active_df is not None:
        with st.spinner("Fetching counts..."):
            results = asyncio.run(main_async_fetcher(active_df, base_url))
            active_df['Product Count'] = results; st.success("✅ Processing Complete!")
            df_output = pd.DataFrame({'Serial Number': range(1, 1 + len(active_df)), 'Keyword': active_df['Keyword'], 'Product Count': active_df['Product Count']})
            st.subheader("Results")
            if (df_output['Product Count'] == -1).sum() > 0: st.warning(f"Failed keywords: {(df_output['Product Count'] == -1).sum()} (marked as -1).")
            st.dataframe(df_output)
            csv_data = df_output.to_csv(index=False).encode('utf-8')
            st.download_button(label="📥 Download Full Results as CSV", data=csv_data, file_name=f"{shop_id}_{environment}_product_counts.csv", mime="text/csv", key=f"{source}_download")
    else: st.error("Please provide keywords.")

# --- Keyword Validator Tab Logic ---
with tab_kw_validator:
    st.header("Keyword Validator Tool")
    st.markdown("This tool checks for keywords containing special characters. Paste keywords below to find any that are invalid.")
    validator_input_text = st.text_area("Paste keywords here to validate:", height=300, key="validator_input", placeholder="valid keyword\n-another- one\nkeyword with émoji 👍")
    if st.button("Validate Keywords", key="validator_button"):
        if validator_input_text:
            keywords_to_check = [kw.strip() for kw in validator_input_text.split('\n') if kw.strip()]
            rejected_keywords = [kw for kw in keywords_to_check if not is_valid_keyword(kw)]
            st.subheader("Validation Results")
            st.info(f"Total Processed: **{len(keywords_to_check)}** | Rejected: **{len(rejected_keywords)}**")
            if rejected_keywords:
                st.error(f"Found {len(rejected_keywords)} rejected keywords:")
                rejected_text = "\n".join(rejected_keywords)
                st.markdown("Click the copy icon in the top-right corner of the box below.")
                st.code(rejected_text, language="")
                rejected_df = pd.DataFrame({"keyword": rejected_keywords})
                st.download_button(label="📥 Download Rejected Keywords as CSV", data=rejected_df.to_csv(index=False).encode('utf-8'), file_name="rejected_keywords.csv", mime="text/csv", key="download_rejected_kw")
            else:
                st.success("🎉 All keywords are valid!")
        else: st.warning("Please paste some keywords to check.")

# --- URL Validator Tab Logic ---
with tab_url_validator:
    st.header("URL Validator Tool")
    st.markdown("Validates URLs: must start with `https://`, have no `--`, and only contain letters (any language), numbers, and symbols `-_.:/`.")
    url_validator_input = st.text_area("Paste URLs here to validate:", height=300, key="url_validator_input", placeholder="https://www.lenovo.com/buy/portátiles-con-pantalla-de-14-pulgadas-0agz00a_staging\nhttps://www.bad--url.com/page")
    if st.button("Validate URLs", key="url_validator_button"):
        if url_validator_input:
            urls_to_check = [u.strip() for u in url_validator_input.split('\n') if u.strip()]
            invalid_urls_data = []
            for url in urls_to_check:
                is_valid, reason = validate_url(url)
                if not is_valid:
                    invalid_urls_data.append({"URL": url, "Reason for Failure": reason})

            st.subheader("Validation Results")
            st.info(f"Total URLs Processed: **{len(urls_to_check)}** | Invalid URLs Found: **{len(invalid_urls_data)}**")

            if invalid_urls_data:
                st.error(f"Found {len(invalid_urls_data)} invalid URLs:")
                invalid_df = pd.DataFrame(invalid_urls_data)
                
                st.markdown("#### Detailed Breakdown of Invalid URLs")
                st.dataframe(invalid_df, use_container_width=True)

                st.markdown("---")
                st.markdown("#### Copyable List of Invalid URLs")
                st.markdown("Click the copy icon in the top-right corner of the box below to copy all invalid URLs.")
                
                copyable_urls = "\n".join(invalid_df["URL"])
                st.code(copyable_urls, language="")

                st.download_button(label="📥 Download Invalid URLs as CSV", data=invalid_df.to_csv(index=False).encode('utf-8'), file_name="invalid_urls.csv", mime="text/csv", key="download_invalid_urls")
            else:
                st.success("🎉 All URLs are valid!")
        else:
            st.warning("Please paste some URLs to check.")

# --- Color Identifier Tab Logic (Updated) ---
with tab_color_id:
    st.header("Color Keyword Identifier")
    st.markdown("Identifies if a keyword contains a color name (based on CSS4 colors + fashion additions like 'chalk', 'gum', 'bone').")
    
    color_input_text = st.text_area("Paste keywords here to identify colors:", height=300, key="color_input", placeholder="blue shirt\nblack pants\nrunning shoes\nmetallic gold bag")
    
    if st.button("Identify Colors", key="color_button"):
        if color_input_text:
            # 1. Prepare Data
            keywords_to_check = [kw.strip() for kw in color_input_text.split('\n') if kw.strip()]
            
            # 2. Get Regex (Cached)
            color_regex = get_color_regex()
            
            # 3. Process - Filter ONLY matches
            matched_keywords = [kw for kw in keywords_to_check if color_regex.search(kw)]
            
            # 4. Display Results
            st.subheader("Analysis Results")
            st.info(f"Total Processed: **{len(keywords_to_check)}** | Color Matches Found: **{len(matched_keywords)}**")
            
            if matched_keywords:
                st.success(f"Found {len(matched_keywords)} keywords containing color terms:")
                
                # Create DataFrame for display/download
                df_matches = pd.DataFrame(matched_keywords, columns=["Color Keywords"])
                
                # Show the dataframe
                st.dataframe(df_matches, use_container_width=True)
                
                # Copyable text block
                
                # 5. Download
                csv_color = df_matches.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Color Keywords as CSV",
                    data=csv_color,
                    file_name="color_keywords_only.csv",
                    mime="text/csv",
                    key="download_color_results"
                )
            else:
                st.warning("No color-related keywords found in the provided list.")
        else:
            st.warning("Please paste some keywords to identify.")