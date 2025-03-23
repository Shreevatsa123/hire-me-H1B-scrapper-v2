import requests
from bs4 import BeautifulSoup

def scrape_jobs(url):
    keywords = ["data engineer", "software engineer"]

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch {url}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text().lower()

    for keyword in keywords:
        if keyword.lower() in text:
            print(f"Found '{keyword}' in {url}")
            # Store data logic (database, file, etc.)
            return

    print(f"No relevant jobs found at {url}")
