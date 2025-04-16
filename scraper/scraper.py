# scraper/scraper.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlencode
import time
import logging
import random # To add randomness to delays

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- !!! IMPORTANT: UPDATE THESE SELECTORS !!! ---
# Inspect the live careers.google.com/jobs/results/ page
# Use your browser's developer tools (Right-click -> Inspect)
# Find the elements containing the job card, title, and link, and get their CSS selectors.
# Example selectors (likely need changing):
# JOB_CARD_SELECTOR = 'li.gcVpJp' # Example: A list item holding a job posting
JOB_CARD_SELECTOR = 'ul li a[href^="/jobs/results/"]' # More plausible example targeting list items with job links
TITLE_SELECTOR = 'h2.gcVpJp' # Example: The H2 tag containing the title within the card
LINK_SELECTOR = 'a[href^="/jobs/results/"]' # Example: The link itself (might be the card or inside it)
# ---

# --- Constants ---
BASE_URL = "https://careers.google.com"
SEARCH_PATH = "/jobs/results/"
DEFAULT_KEYWORDS = ["data engineer", "software engineer"]
# How many pages of results to attempt to scrape. Reduce if causing issues.
MAX_PAGES_TO_SCRAPE = 3
# Delay between requests in seconds (be respectful!)
REQUEST_DELAY_SECONDS = 5


def scrape_google_careers(keywords=None, max_pages=MAX_PAGES_TO_SCRAPE):
    """
    Crawls Google Careers search results for specific keywords and extracts job listings.

    Args:
        keywords (list, optional): List of keywords to search for. 
                                    Defaults to DEFAULT_KEYWORDS.
        max_pages (int, optional): Maximum number of result pages to scrape. 
                                   Defaults to MAX_PAGES_TO_SCRAPE.

    Returns:
        list: A list of dictionaries, each representing a found job listing.
              Returns an empty list if scraping fails or no jobs are found.
    """
    if keywords is None:
        keywords = DEFAULT_KEYWORDS

    # Join keywords for the query parameter, e.g., "data+engineer,software+engineer"
    query_param = ",".join(k.replace(" ", "+") for k in keywords)
    
    current_page = 1
    scraped_jobs = []
    processed_links = set() # Keep track of links to avoid duplicates across pages
    
    # Start with the first page URL
    params = {'q': query_param} 
    next_page_url = BASE_URL + SEARCH_PATH + "?" + urlencode(params)


    logging.info(f"Starting scrape for keywords: {keywords}")

    while current_page <= max_pages and next_page_url:
        logging.info(f"Scraping page {current_page}: {next_page_url}")
        
        try:
            # Use headers to mimic a browser slightly
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(next_page_url, timeout=20, headers=headers) # Increased timeout
            response.raise_for_status() # Check for HTTP errors (4xx or 5xx)

            soup = BeautifulSoup(response.text, "lxml") # Using lxml parser

            # --- Find Job Cards ---
            # Using select for potentially multiple matches per page
            job_cards = soup.select(JOB_CARD_SELECTOR)
            logging.info(f"Found {len(job_cards)} potential job cards on page {current_page} using selector '{JOB_CARD_SELECTOR}'")

            if not job_cards and current_page == 1:
                 logging.warning(f"No job cards found on the first page. Check CSS selectors or website structure.")
                 # Optionally, check for "no results found" text if selector fails
                 # no_results_text = soup.find(text=re.compile("no results found", re.IGNORECASE))
                 # if no_results_text:
                 #      logging.info("Detected 'no results found' text on the page.")
                 #      break # Stop if no results explicitly stated

            page_found_count = 0
            for card in job_cards:
                # --- Extract Title ---
                title_element = card.select_one(TITLE_SELECTOR)
                title = title_element.get_text(strip=True) if title_element else "Title not found"

                # --- Extract Link ---
                # Sometimes the card itself is the link, sometimes it's nested
                link_element = card if card.name == 'a' and card.has_attr('href') else card.select_one(LINK_SELECTOR)
                
                if link_element and link_element.has_attr('href'):
                    relative_link = link_element['href']
                    # Ensure the link is absolute
                    absolute_link = urljoin(BASE_URL, relative_link)
                else:
                    absolute_link = "Link not found"
                    logging.warning(f"Could not find link using selector '{LINK_SELECTOR}' within a card. Title found: '{title}'")
                    continue # Skip if no link

                # --- Check Keywords and Duplicates ---
                title_lower = title.lower()
                if any(keyword in title_lower for keyword in keywords) and absolute_link != "Link not found":
                    if absolute_link not in processed_links:
                        job_entry = {
                            "date_posting": datetime.today().strftime('%Y-%m-%d'),
                            "job_posting_name": title,
                            "description": f"Found matching job: '{title}'",
                            "link": absolute_link
                        }
                        scraped_jobs.append(job_entry)
                        processed_links.add(absolute_link)
                        page_found_count += 1
                        # logging.debug(f"Added job: {job_entry}") # Use debug level for less verbose output
                    else:
                        logging.debug(f"Skipping duplicate link: {absolute_link}")


            logging.info(f"Found {page_found_count} new matching jobs on page {current_page}.")

            # --- Find Next Page Link (Highly website-dependent!) ---
            # This is a common pattern, but Google might use JS. UPDATE THIS SELECTOR!
            next_page_selector = 'a[aria-label="Next page"]' # Example selector - MUST BE VERIFIED
            next_page_element = soup.select_one(next_page_selector)

            if next_page_element and next_page_element.has_attr('href'):
                next_page_relative_link = next_page_element['href']
                next_page_url = urljoin(BASE_URL, next_page_relative_link)
                logging.info(f"Found next page link: {next_page_url}")
            else:
                logging.info(f"No next page link found using selector '{next_page_selector}'. Stopping pagination.")
                next_page_url = None # Stop the loop

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Failed to fetch or process {next_page_url}: {e}")
            next_page_url = None # Stop on error
        except Exception as e:
             logging.error(f"❌ An unexpected error occurred during scraping page {current_page}: {e}")
             next_page_url = None # Stop on unexpected error

        # --- Respectful Delay ---
        if next_page_url and current_page < max_pages :
             # Add a random element to the delay
             delay = REQUEST_DELAY_SECONDS + random.uniform(0.5, 2.0) 
             logging.info(f"Waiting for {delay:.2f} seconds before next request...")
             time.sleep(delay)

        current_page += 1
        # End of while loop

    logging.info(f"✅ Finished scraping. Found a total of {len(scraped_jobs)} unique matching jobs across {current_page - 1} pages.")
    return scraped_jobs

# Example usage (for testing directly)
# if __name__ == "__main__":
#     jobs = scrape_google_careers(keywords=["software engineer", "site reliability engineer"], max_pages=2)
#     if jobs:
#         print("\n--- Found Jobs ---")
#         for job in jobs:
#             print(f"  Title: {job['job_posting_name']}")
#             print(f"  Link: {job['link']}")
#             print("-" * 10)
#     else:
#         print("No matching jobs found or scraping failed.")