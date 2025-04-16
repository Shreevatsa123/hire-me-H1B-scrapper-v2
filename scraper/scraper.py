# scraper/scraper.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlencode
import time
import logging
import random
import yaml # <--- Import yaml

# Configure logging (if not already configured elsewhere)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Delay between requests remains important
REQUEST_DELAY_SECONDS = 5

# --- The Core Scraping Function (scrape_site) ---
# This function remains unchanged from the previous version
# It accepts parameters and performs the scraping for one site.
def scrape_site(company_name, base_url, search_path, keywords, location_query, selectors, max_pages):
    # ... (Keep the exact same code as in the previous response for scrape_site) ...
    # ... (It correctly accepts arguments like base_url, keywords, selectors etc.) ...
    """
    Crawls a specific job site based on provided configuration.
    (Code for this function is identical to the previous JSON example - omitted here for brevity)
    """
    
    scraped_jobs = []
    processed_links = set()
    current_page = 1

    # --- Construct initial Search URL ---
    query_params = {}
    # Keywords: Join keywords for the 'q' parameter, replacing spaces
    if keywords:
        query_params['q'] = ",".join(k.replace(" ", "+") for k in keywords)
    # Location: Add location if provided (parameter name 'location' is common, but might vary by site)
    if location_query:
        query_params['location'] = location_query.replace(" ", "+").replace(",", "%2C") # Basic encoding for location

    start_url = urljoin(base_url, search_path) + "?" + urlencode(query_params)
    next_page_url = start_url

    logging.info(f"[{company_name}] Starting scrape for keywords: {keywords} at location: '{location_query}'")
    logging.info(f"[{company_name}] Initial URL: {start_url}")
    
    # --- Selector Validation ---
    required_selectors = ['job_card', 'title', 'link', 'next_page']
    if not all(key in selectors for key in required_selectors):
        logging.error(f"[{company_name}] Missing required selectors in config. Found: {selectors.keys()}. Required: {required_selectors}")
        return [] # Cannot proceed without selectors
        
    job_card_selector = selectors['job_card']
    title_selector = selectors['title']
    link_selector = selectors['link']
    next_page_selector = selectors['next_page']

    while current_page <= max_pages and next_page_url:
        logging.info(f"[{company_name}] Scraping page {current_page}: {next_page_url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(next_page_url, timeout=20, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            job_cards = soup.select(job_card_selector)
            logging.info(f"[{company_name}] Found {len(job_cards)} potential job cards on page {current_page} using selector '{job_card_selector}'")

            if not job_cards and current_page == 1:
                 logging.warning(f"[{company_name}] No job cards found on the first page. Check CSS selectors ('{job_card_selector}') or website structure for {company_name}.")
                 break # Stop if nothing found on first page

            page_found_count = 0
            for card in job_cards:
                title_element = card.select_one(title_selector)
                title = title_element.get_text(strip=True) if title_element else "Title not found"

                # Try extracting link from card itself or nested element
                link_element = card if card.name == 'a' and card.has_attr('href') else card.select_one(link_selector)
                
                if link_element and link_element.has_attr('href'):
                    relative_link = link_element['href']
                    absolute_link = urljoin(base_url, relative_link)
                else:
                    absolute_link = "Link not found"
                    logging.warning(f"[{company_name}] Could not find link using selector '{link_selector}' within a card. Title found: '{title}'")
                    continue 

                # Check if keywords match (already filtered by query param, but good for refinement)
                title_lower = title.lower()
                if any(keyword.lower() in title_lower for keyword in keywords) and absolute_link != "Link not found":
                    if absolute_link not in processed_links:
                        job_entry = {
                            "date_posting": datetime.today().strftime('%Y-%m-%d'),
                            "job_posting_name": title,
                            "description": f"Found matching job: '{title}' at {company_name}",
                            "link": absolute_link,
                            "company": company_name # Add company name for context
                        }
                        scraped_jobs.append(job_entry)
                        processed_links.add(absolute_link)
                        page_found_count += 1
                    else:
                        logging.debug(f"[{company_name}] Skipping duplicate link: {absolute_link}")

            logging.info(f"[{company_name}] Found {page_found_count} new matching jobs on page {current_page}.")

            # Find Next Page Link
            next_page_element = soup.select_one(next_page_selector)
            if next_page_element and next_page_element.has_attr('href'):
                next_page_relative_link = next_page_element['href']
                # Ensure the next page link is absolute using the *original base URL*
                next_page_url = urljoin(base_url, next_page_relative_link) 
                logging.info(f"[{company_name}] Found next page link: {next_page_url}")
            else:
                logging.info(f"[{company_name}] No next page link found using selector '{next_page_selector}'. Stopping pagination.")
                next_page_url = None

        except requests.exceptions.RequestException as e:
            logging.error(f"[{company_name}] Failed to fetch or process {next_page_url}: {e}")
            next_page_url = None 
        except Exception as e:
             logging.error(f"[{company_name}] An unexpected error occurred during scraping page {current_page}: {e}")
             next_page_url = None 

        if next_page_url and current_page < max_pages :
             delay = REQUEST_DELAY_SECONDS + random.uniform(0.5, 2.0) 
             logging.info(f"[{company_name}] Waiting for {delay:.2f} seconds before next request...")
             time.sleep(delay)

        current_page += 1

    logging.info(f"[{company_name}] Finished scraping. Found {len(scraped_jobs)} unique matching jobs across {current_page - 1} pages.")
    return scraped_jobs


# --- Wrapper function updated for YAML ---
# Default path now points to .yaml file
def run_configured_scrapers(config_path='/opt/airflow/config/scraper_config.yaml', **context):
    """
    Reads scraper configuration from YAML and runs the scraper for each configured site.
    """
    all_scraped_jobs = []
    logging.info(f"Attempting to load configuration from: {config_path}") # Log path
    try:
        with open(config_path, 'r') as f:
            # Use yaml.safe_load instead of json.load
            config_data = yaml.safe_load(f) 
            if config_data is None: # Handle empty file case
                 logging.error(f"Configuration file at {config_path} is empty or invalid.")
                 return []
    except FileNotFoundError:
        logging.error(f"YAML Configuration file not found at {config_path}")
        raise
    except yaml.YAMLError as e: # Catch YAML parsing errors
        logging.error(f"Error parsing YAML configuration file at {config_path}: {e}")
        raise
    except Exception as e: # Catch other potential file reading errors
        logging.error(f"An unexpected error occurred reading config file {config_path}: {e}")
        raise

    
    if 'target_sites' not in config_data or not isinstance(config_data['target_sites'], list):
        logging.error("YAML Config file must contain a 'target_sites' list.")
        return [] 

    logging.info(f"Successfully loaded configuration for {len(config_data['target_sites'])} target sites.")

    for site_config in config_data['target_sites']:
        company = site_config.get('company_name', 'Unnamed Site')
        logging.info(f"Processing site config: {company}")
        
        base_url = site_config.get('base_url')
        search_path = site_config.get('search_path')
        keywords = site_config.get('keywords', [])
        location = site_config.get('location_query', '')
        selectors = site_config.get('selectors', {})
        max_pages = site_config.get('max_pages', 1)

        if not base_url or not search_path or not selectors:
             logging.warning(f"Skipping site '{company}' due to missing base_url, search_path, or selectors in config.")
             continue
        
        try: # Add try-except around individual site scraping
            site_jobs = scrape_site(
                company_name=company,
                base_url=base_url,
                search_path=search_path,
                keywords=keywords,
                location_query=location,
                selectors=selectors,
                max_pages=max_pages
            )
            all_scraped_jobs.extend(site_jobs)
        except Exception as e:
             logging.error(f"Error scraping site '{company}': {e}", exc_info=True) # Log traceback


        if len(config_data['target_sites']) > 1:
            time.sleep(random.uniform(2.0, 5.0)) 

    logging.info(f"Completed all configured scrapers. Total jobs found: {len(all_scraped_jobs)}")
    return all_scraped_jobs