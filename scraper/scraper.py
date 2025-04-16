# scraper/scraper.py

# --- START: Debugging Imports ---
print("--- scraper.py: Attempting imports ---")
try:
    import requests
    print("--- scraper.py: Imported requests ---")
    from bs4 import BeautifulSoup
    print("--- scraper.py: Imported BeautifulSoup ---")
    from datetime import datetime
    print("--- scraper.py: Imported datetime ---")
    from urllib.parse import urljoin, urlencode
    print("--- scraper.py: Imported urllib.parse ---")
    import time
    print("--- scraper.py: Imported time ---")
    import logging
    print("--- scraper.py: Imported logging ---")
    import random
    print("--- scraper.py: Imported random ---")
    import yaml
    print("--- scraper.py: Imported yaml ---")
    # Explicitly check lxml as it's used by BeautifulSoup
    import lxml
    print("--- scraper.py: Imported lxml ---")
except ImportError as e:
    print(f"--- scraper.py: FAILED IMPORT: {e} ---")
    # Re-raise the error so it doesn't proceed silently if an import fails
    raise e
print("--- scraper.py: All imports successful ---")
# --- END: Debugging Imports ---


# Configure logging (if not already configured elsewhere)
# Note: Logging might not be fully configured if 'import logging' failed above
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Delay between requests remains important
REQUEST_DELAY_SECONDS = 5

# --- The Core Scraping Function (scrape_site) ---
# Updated function definition to accept extra_params
def scrape_site(company_name, base_url, search_path, keywords, location_query, selectors, max_pages, extra_params=None):
    """
    Crawls a specific job site based on provided configuration,
    including optional extra query parameters and location extraction.
    """
    scraped_jobs = []
    processed_links = set()
    current_page = 1

    # --- Construct initial Search URL ---
    query_params = {}
    # Keywords: Join keywords for the 'q' parameter, replacing spaces
    if keywords:
        query_params['q'] = ",".join(k.replace(" ", "+") for k in keywords)
    # Location: Add location if provided
    if location_query:
        query_params['location'] = location_query.replace(" ", "+").replace(",", "%2C") # Basic encoding for location

    # --- NEW: Add extra query parameters from config ---
    if extra_params and isinstance(extra_params, dict):
        # Merge extra_params, allowing for list values for repeated keys
        # urlencode handles lists by repeating the key
        query_params.update(extra_params)
    # --- End NEW Section ---

    # Encode the parameters - doseq=True handles list values correctly
    encoded_params = urlencode(query_params, doseq=True)
    start_url = urljoin(base_url, search_path) + "?" + encoded_params
    next_page_url = start_url

    logging.info(f"[{company_name}] Starting scrape for keywords: {keywords} at location: '{location_query}' with extra params: {extra_params}")
    logging.info(f"[{company_name}] Initial URL: {start_url}")

    # --- Selector Validation ---
    # Consider adding 'location' here if it becomes mandatory
    required_selectors = ['job_card', 'title', 'link', 'next_page']
    if not all(key in selectors for key in required_selectors):
        logging.error(f"[{company_name}] Missing required selectors in config. Found: {selectors.keys()}. Required: {required_selectors}")
        return [] # Cannot proceed without selectors

    job_card_selector = selectors['job_card']
    title_selector = selectors['title']
    link_selector = selectors['link']
    next_page_selector = selectors['next_page']
    location_selector = selectors.get('location') # Get optional location selector

    while current_page <= max_pages and next_page_url:
        logging.info(f"[{company_name}] Scraping page {current_page}: {next_page_url}")

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(next_page_url, timeout=20, headers=headers)
            response.raise_for_status()

            # This is where lxml is used by BeautifulSoup
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

                # --- NEW: Extract Location using Selector ---
                location_text = "Location not found" # Default value
                if location_selector:
                    location_element = card.select_one(location_selector)
                    if location_element:
                        location_text = location_element.get_text(strip=True)
                    else:
                        # Don't log warning for every card, maybe just once per page or less frequently?
                        # logging.warning(f"[{company_name}] Could not find location using selector '{location_selector}' for job '{title}'")
                        pass # Keep default "Location not found"
                # --- End NEW Location Section ---


                # Check if keywords match (can be less strict now if specific filters are used)
                title_lower = title.lower()
                # Consider if this keyword check is still needed or should be optional if extra_params are used
                keywords_match = not keywords or any(keyword.lower() in title_lower for keyword in keywords)

                if keywords_match and absolute_link != "Link not found":
                    if absolute_link not in processed_links:
                        # --- Updated job_entry structure ---
                        job_entry = {
                            # "date_posting": datetime.today().strftime('%Y-%m-%d'), # Removed - handled by DB default or store_scraped_data
                            "job_posting_name": title,
                            "description": f"Found matching job: '{title}' at {company_name}", # Consider adding location here
                            "link": absolute_link,
                            "company": company_name,
                            "location": location_text # Add extracted location
                            # date_scrapped and last_seen_date will be handled in store_scraped_data.py
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
        # Catch BeautifulSoup FeatureNotFound error if lxml isn't installed
        except Exception as e: # Catch other potential errors too
             # Check if the error is related to the parser
             if "Couldn't find a suitable parser" in str(e) or isinstance(e, ImportError):
                 logging.error(f"[{company_name}] CRITICAL PARSER ERROR: Could not initialize BeautifulSoup parser (likely missing 'lxml'). Error: {e}", exc_info=True)
             else:
                 logging.error(f"[{company_name}] An unexpected error occurred during scraping page {current_page}: {e}", exc_info=True)
             next_page_url = None # Stop pagination on error

        if next_page_url and current_page < max_pages :
             delay = REQUEST_DELAY_SECONDS + random.uniform(0.5, 2.0)
             logging.info(f"[{company_name}] Waiting for {delay:.2f} seconds before next request...")
             time.sleep(delay)

        current_page += 1

    logging.info(f"[{company_name}] Finished scraping. Found {len(scraped_jobs)} unique matching jobs across {current_page - 1} pages.")
    return scraped_jobs


# --- Wrapper function updated for YAML and extra_params ---
def run_configured_scrapers(config_path='/opt/airflow/config/scraper_config.yaml', **context):
    """
    Reads scraper configuration from YAML and runs the scraper for each configured site.
    """
    all_scraped_jobs = []
    logging.info(f"Attempting to load configuration from: {config_path}") # Log path
    try:
        with open(config_path, 'r') as f:
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
        # --- NEW: Extract extra query parameters ---
        extra_params = site_config.get('extra_query_params', None) # Expecting a dict
        # --- End NEW Section ---


        if not base_url or not search_path or not selectors:
              logging.warning(f"Skipping site '{company}' due to missing base_url, search_path, or selectors in config.")
              continue

        try: # Add try-except around individual site scraping
            # --- Updated call to scrape_site ---
            site_jobs = scrape_site(
                company_name=company,
                base_url=base_url,
                search_path=search_path,
                keywords=keywords,
                location_query=location,
                selectors=selectors,
                max_pages=max_pages,
                extra_params=extra_params # Pass the new argument
            )
            all_scraped_jobs.extend(site_jobs)
        except Exception as e:
             logging.error(f"Error scraping site '{company}': {e}", exc_info=True) # Log traceback


        if len(config_data['target_sites']) > 1:
            # Consider adjusting delay if needed
            time.sleep(random.uniform(2.0, 5.0))

    logging.info(f"Completed all configured scrapers. Total jobs found: {len(all_scraped_jobs)}")
    # Return the list of job dictionaries
    return all_scraped_jobs