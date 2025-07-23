
import streamlit as st
import subprocess
import threading
import pandas as pd
import os
import time

CSV_FILE = "combat_sports_leads.csv"
SCRAPER_FILE = "super_scraper_threaded.py"

st.set_page_config(page_title="Martial Arts Leads Scraper", layout="wide")

st.title("🥋 Martial Arts Leads Scraper")
st.markdown("Enter your keywords and locations, then click **Start Scraping** to begin. Leads will be shown below.")

with st.form("input_form"):
    keywords = st.text_area("Enter Keywords (one per line)", "martial arts club\nboxing gloves shop")
    locations = st.text_area("Enter Locations (one per line)", "London\nCamden\nIslington")
    submitted = st.form_submit_button("🚀 Start Scraping")

status_placeholder = st.empty()

def run_scraper(keywords, locations):
    with open("keywords.txt", "w") as kf:
        kf.write("\n".join(keywords))
    with open("locations.txt", "w") as lf:
        lf.write("\n".join(locations))
    subprocess.run(["python", SCRAPER_FILE], stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)

if submitted:
    keyword_list = [k.strip() for k in keywords.split("\n") if k.strip()]
    location_list = [l.strip() for l in locations.split("\n") if l.strip()]
    if keyword_list and location_list:
        status_placeholder.info("⏳ Scraping in progress... This may take a few minutes.")
        threading.Thread(target=run_scraper, args=(keyword_list, location_list), daemon=True).start()
        time.sleep(5)
    else:
        st.warning("Please enter at least one keyword and one location.")

if os.path.exists(CSV_FILE):
    df = pd.read_csv(CSV_FILE)
    st.success(f"✅ {len(df)} leads found.")
    st.download_button("⬇️ Download CSV", df.to_csv(index=False), file_name="leads.csv")
    st.dataframe(df)
