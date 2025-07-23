
import requests
import csv
import time
import re
import tldextract
import urllib.parse
import dns.resolver
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from googlesearch import search
import phonenumbers
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== CONFIG ==========
API_KEY = "AIzaSyATDQWm8XiR-ajpLR3g3mDdATKngNTJI7U"
OUTPUT_CSV = "combat_sports_leads.csv"
USER_AGENT = {"User-Agent": "Mozilla/5.0"}
CRAWL_LIMIT = 50
THREADS = 5
# ============================

visited_pages = set()
seen_domains = set()
results_to_write = []

# SMART KEYWORDS
keywords = [
    "martial arts club", "boxing club", "kickboxing gym", "muay thai gym", "bjj academy", "mma training center",
    "self defense classes", "taekwondo school", "karate dojo", "combat sports academy",
    "boxing gloves store", "martial arts gear shop", "kickboxing equipment store", "mma gear shop",
    "combat sports supply store", "martial arts uniforms shop", "boxing apparel store", "muay thai gear london",
    "punching bags shop london", "martial arts shoes store",
    "combat sports gear wholesaler", "boxing equipment distributor", "martial arts supply wholesale",
    "martial arts gear bulk supplier", "boxing gloves wholesale london", "fight gear supplier", "bjj gear distributor",
    "martial arts retail supplier", "kickboxing gloves distributor", "mma accessories wholesaler",
    "custom boxing gloves manufacturer", "martial arts mat suppliers", "custom gym gear london",
    "fight event organizers london", "martial arts printing services", "kickboxing gym equipment provider"
]

locations = [
    "Camden", "Islington", "Hackney", "Kensington", "Chelsea", "Westminster", "Greenwich", "Lambeth", "Southwark",
    "Tower Hamlets", "Newham", "Barking", "Dagenham", "Croydon", "Bromley", "Lewisham", "Wandsworth", "Barnet",
    "Ealing", "Enfield", "Haringey", "Hammersmith", "Fulham", "Hounslow", "Redbridge"
]

def generate_queries():
    return [f"{kw} in {loc}" for kw in keywords for loc in locations]

def google_places_search_all(query):
    results = []
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={urllib.parse.quote(query)}&key={API_KEY}"
    while url:
        res = requests.get(url)
        data = res.json()
        results.extend(data.get("results", []))
        token = data.get("next_page_token")
        if token:
            time.sleep(2)
            url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?pagetoken={token}&key={API_KEY}"
        else:
            break
    return results

def get_place_details(place_id):
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,website,formatted_phone_number&key={API_KEY}"
    res = requests.get(url)
    return res.json().get("result", {})

def extract_emails(text):
    basic = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)
    obfuscated = re.findall(r"[a-zA-Z0-9_.+-]+\s?\[\s?at\s?\]\s?[a-zA-Z0-9-]+\s?\[\s?dot\s?\]\s?[a-zA-Z]+", text)
    deobfuscated = [re.sub(r"\s?\[\s?at\s?\]\s?", "@", e).replace("[dot]", ".") for e in obfuscated]
    return list(set(basic + deobfuscated))

def extract_phones(text, region="GB"):
    found = []
    for match in phonenumbers.PhoneNumberMatcher(text, region):
        num = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
        found.append(num)
    return list(set(found))

def check_mx(domain):
    try:
        mx = dns.resolver.resolve(domain, 'MX')
        return True if mx else False
    except:
        return False

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

def is_internal(url, domain):
    return domain in url

def crawl_site(url, domain, max_pages=50):
    emails, phones, socials, leadership, team_url, linkedins, pages_visited = [], [], {"facebook": "", "instagram": "", "linkedin": "", "whatsapp": ""}, "", "", [], []
    to_visit = [url]

    while to_visit and len(visited_pages) < max_pages:
        current = to_visit.pop(0)
        if current in visited_pages:
            continue
        visited_pages.add(current)
        try:
            res = requests.get(current, headers=USER_AGENT, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")
            text = soup.get_text()
            emails += extract_emails(text)
            phones += extract_phones(text)
            s = extract_social_links(soup)
            for k in socials:
                if not socials[k]:
                    socials[k] = s.get(k, "")
            if not leadership and any(x in text.lower() for x in ["ceo", "head coach", "chief coach", "founder"]):
                leadership = "; ".join(set(re.findall(r"(ceo|head coach|chief coach|founder).{0,100}", text.lower(), re.I)))
            for a in soup.find_all("a", href=True):
                href = a["href"]
                link = urljoin(current, href)
                if is_internal(link, domain) and link not in visited_pages:
                    to_visit.append(link)
                if any(x in link.lower() for x in ["team", "staff", "about", "coach"]) and not team_url:
                    team_url = link
                if 'linkedin.com' in link:
                    linkedins.append(link)
            pages_visited.append(current)
        except:
            continue

    return list(set(emails)), list(set(phones)), socials, leadership, team_url, list(set(linkedins)), pages_visited

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
    emails, phones, socials, leadership, team_link, linkedins, visited = crawl_site(website, domain, max_pages=CRAWL_LIMIT)
    valid_email = "N/A"
    if emails:
        domain_check = emails[0].split("@")[-1]
        valid_email = "✅" if check_mx(domain_check) else "❌"
    return [
        name, website, ", ".join(emails), valid_email, ", ".join(phones) or phone,
        socials.get("whatsapp", ""), socials.get("facebook", ""), socials.get("instagram", ""),
        socials.get("linkedin", ""), maps_url, leadership, team_link,
        ", ".join(linkedins), ", ".join(visited), query
    ]

# ============ MAIN ============

with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "Company", "Website", "Emails", "Valid Email?", "Phones", "WhatsApp",
        "Facebook", "Instagram", "LinkedIn", "Maps Link",
        "Leadership", "Team Page", "LinkedIn Links", "Pages Visited", "Query"
    ])

    queries = generate_queries()
    for query in tqdm(queries, desc="Processing keywords"):
        places = google_places_search_all(query)
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(process_place, place, query) for place in places]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    writer.writerow(result)

print(f"✅ Done! Leads saved to: {OUTPUT_CSV}")
