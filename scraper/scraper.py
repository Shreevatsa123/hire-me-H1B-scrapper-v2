import requests
from bs4 import BeautifulSoup
from datetime import datetime

def scrape_jobs(url):
    """Scrapes job postings based on keywords."""
    keywords = ["data engineer", "software engineer"]
    response = requests.get(url)

    if response.status_code != 200:
        print(f"❌ Failed to fetch {url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text().lower()

    job_listings = []
    for keyword in keywords:
        if keyword.lower() in text:
            job_entry = {
                "date_posting": datetime.today().strftime('%Y-%m-%d'),
                "job_posting_name": keyword,
                "description": f"Found '{keyword}' in {url}",
                "link": url
            }
            print(f"✅ Found job listing: {job_entry}")  # Debugging
            job_listings.append(job_entry)

    print(f"✅ Final scraped job listings: {job_listings}")  # Debugging
    return job_listings  # Ensure correct format
