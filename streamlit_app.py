import streamlit as st
import pandas as pd
import requests
import tldextract
import re
import dns.resolver
import phonenumbers
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== CONFIG =====
API_KEY = st.secrets["API_KEY"]
USER_AGENT = {"User-Agent": "Mozilla/5.0"}
CRAWL_LIMIT = 20
THREADS = 5
# ==================

visited_pages = set()
seen_domains = set()

def google_places_search_all(query):
    results = []
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&key={API_KEY}"

    while True:
        response = requests.get(url)
        data = response.json()

        if data.get("status") != "OK":
            break

        results.extend(data.get("results", []))

        token = data.get("next_page_token")
        if token:
            time.sleep(2)  # required delay
            url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?pagetoken={token}&key={API_KEY}"
        else:
            break

    return results

def get_place_details(place_id):
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,website,formatted_phone_number&key={API_KEY}"
    res = requests.get(url)
    return res.json().get("result", {})

def extract_emails(text):
    return list(set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)))

def extract_phones(text, region="GB"):
    found = []
    for match in phonenumbers.PhoneNumberMatcher(text, region):
        num = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
        found.append(num)
    return list(set(found))

def extract_social_links(soup):
    links = {"facebook": "", "instagram": "", "linkedin": "", "whatsapp": ""}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "facebook.com" in href and not links["facebook"]:
            links["facebook"] = href
        elif "instagram.com" in href and not links["instagram"]:
            links["instagram"] = href
        elif "linkedin.com" in href and not links["linkedin"]:
            links["linkedin"] = href
        elif "wa.me" in href or "whatsapp.com" in href:
            links["whatsapp"] = href
    return links

def crawl_site(url, domain):
    emails, phones, socials = [], [], {"facebook": "", "instagram": "", "linkedin": "", "whatsapp": ""}
    to_visit = [url]
    pages_crawled = 0

    while to_visit and pages_crawled < CRAWL_LIMIT:
        current = to_visit.pop(0)
        if current in visited_pages:
            continue
        visited_pages.add(current)
        pages_crawled += 1
        try:
            res = requests.get(current, headers=USER_AGENT, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")
            text = soup.get_text()
            emails += extract_emails(text)
            phones += extract_phones(text)
            for k, v in extract_social_links(soup).items():
                if not socials[k]:
                    socials[k] = v
            for a in soup.find_all("a", href=True):
                link = urljoin(current, a["href"])
                if domain in link and link not in visited_pages:
                    to_visit.append(link)
        except:
            continue
    return list(set(emails)), list(set(phones)), socials

def process_place(place, query):
    name = place.get("name")
    place_id = place.get("place_id")
    maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
    details = get_place_details(place_id)
    website = details.get("website", "")
    phone = details.get("formatted_phone_number", "")
    if not website:
        return None
    domain = tldextract.extract(website).top_domain_under_public_suffix
    if domain in seen_domains:
        return None
    seen_domains.add(domain)
    emails, phones, socials = crawl_site(website, domain)
    return {
        "Company": name,
        "Website": website,
        "Emails": ", ".join(emails),
        "Phones": ", ".join(phones) or phone,
        "WhatsApp": socials["whatsapp"],
        "Facebook": socials["facebook"],
        "Instagram": socials["instagram"],
        "LinkedIn": socials["linkedin"],
        "Maps Link": maps_url,
        "Query": query,
        "place_id": place_id  # used for de-duplication
    }

# ===== STREAMLIT UI =====
st.set_page_config(page_title="Martial Arts Leads Scraper", layout="wide")
st.title("🥋 Martial Arts Leads Scraper")

with st.form("input_form"):
    keywords = st.text_area("Enter Keywords (one per line)", "martial arts club\nboxing gloves shop")
    locations = st.text_area("Enter Locations (one per line)", "London\nCamden\nIslington")
    submitted = st.form_submit_button("🚀 Start Scraping")

if submitted:
    keyword_list = [k.strip() for k in keywords.splitlines() if k.strip()]
    location_list = [l.strip() for l in locations.splitlines() if l.strip()]
    queries = [f"{k} in {l}" for k in keyword_list for l in location_list]

    st.info("🔍 Scraping started...")
    all_results = []
    seen_place_ids = set()
    progress = st.progress(0)
    total = len(queries)

    for i, query in enumerate(queries):
        places = google_places_search_all(query)
        st.write(f"🔎 Query: {query} → Found {len(places)} places from Google")
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(process_place, place, query) for place in places]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    place_id = result.get("place_id")
                    if place_id and place_id not in seen_place_ids:
                        seen_place_ids.add(place_id)
                        all_results.append(result)
        progress.progress((i + 1) / total)

    df = pd.DataFrame(all_results)
    st.success(f"✅ Done! Found {len(df)} unique leads.")
    st.download_button("⬇️ Download CSV", df.to_csv(index=False), file_name="leads.csv")
    st.dataframe(df)
