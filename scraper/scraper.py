# scraper/scraper.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlencode
import time
import logging
import yaml
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REQUEST_DELAY_SECONDS = 5

def scrape_site(company_name, base_url, search_path, global_roles, global_levels, location_query, selectors, max_pages, extra_params=None):
    scraped_jobs = []
    processed_links = set()

    # --- Construct initial Search URL ---
    # We now use a broader query to get more potential matches from the site
    search_terms = global_roles + global_levels
    query_params = {}
    if search_terms:
        query_params['q'] = " ".join(f'"{k}"' for k in search_terms)
    if location_query:
        query_params['location'] = location_query.replace(" ", "+").replace(",", "%2C")
    if extra_params and isinstance(extra_params, dict):
        query_params.update(extra_params)
    
    encoded_params = urlencode(query_params, doseq=True)
    start_url = urljoin(base_url, search_path) + "?" + encoded_params
    
    logging.info(f"[{company_name}] Starting scrape at URL: {start_url}")

    # --- Selenium Setup ---
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.get(start_url)
    except Exception as e:
        logging.error(f"[{company_name}] Failed to initialize Selenium WebDriver: {e}", exc_info=True)
        return []

    for current_page in range(1, max_pages + 1):
        logging.info(f"[{company_name}] Scraping page {current_page}...")
        
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_card'])))
            time.sleep(3)
            
            soup = BeautifulSoup(driver.page_source, "lxml")
            job_cards = soup.select(selectors['job_card'])
            logging.info(f"[{company_name}] Found {len(job_cards)} potential job cards on page {current_page}.")

            if not job_cards and current_page == 1:
                logging.warning(f"[{company_name}] No job cards found on the first page.")
                break

            page_found_count = 0
            for card in job_cards:
                title_element = card.select_one(selectors['title'])
                title = title_element.get_text(strip=True) if title_element else "Title not found"
                title_lower = title.lower()

                # --- NEW: Two-Part Filtering Logic ---
                # 1. Check if the title contains a required job level (e.g., "intern")
                if not any(level.lower() in title_lower for level in global_levels):
                    continue  # Skip if it's not an intern-level job

                # 2. Check if the title contains all words from at least one of the role phrases
                role_match = False
                for role_phrase in global_roles:
                    if all(word.lower() in title_lower for word in role_phrase.split()):
                        role_match = True
                        break  # Found a role match, no need to check other phrases
                
                if not role_match:
                    continue  # Skip if it matches the level but not the role
                # --- END NEW SECTION ---

                link_element = card.select_one(selectors['link'])
                absolute_link = urljoin(base_url, link_element['href']) if link_element and link_element.has_attr('href') else "Link not found"

                location_text = "Location not found"
                if selectors.get('location'):
                    location_element = card.select_one(selectors['location'])
                    if location_element:
                        location_text = location_element.get_text(strip=True)

                if absolute_link != "Link not found" and absolute_link not in processed_links:
                    job_entry = {
                        "job_posting_name": title,
                        "description": f"Found matching job: '{title}' at {company_name}",
                        "link": absolute_link,
                        "company": company_name,
                        "location": location_text
                    }
                    scraped_jobs.append(job_entry)
                    processed_links.add(absolute_link)
                    page_found_count += 1
            
            logging.info(f"[{company_name}] Found {page_found_count} new matching jobs on this page.")

            if current_page < max_pages and selectors.get('next_page'):
                try:
                    next_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, selectors['next_page'])))
                    driver.execute_script("arguments[0].click();", next_button)
                    logging.info(f"[{company_name}] Clicked 'next' or 'load more' button. Waiting...")
                    time.sleep(REQUEST_DELAY_SECONDS)
                except (TimeoutException, NoSuchElementException):
                    logging.info(f"[{company_name}] No 'next' or 'load more' button found. Stopping.")
                    break
        
        except TimeoutException:
            logging.warning(f"[{company_name}] Timed out waiting for job cards on page {current_page}.")
            break
        except Exception as e:
            logging.error(f"[{company_name}] An unexpected error occurred on page {current_page}: {e}", exc_info=True)
            break

    driver.quit()
    logging.info(f"[{company_name}] Finished scraping. Found {len(scraped_jobs)} unique jobs.")
    return scraped_jobs

def run_configured_scrapers(config_path='/opt/airflow/config/scraper_config.yaml', **context):
    all_scraped_jobs = []
    
    # --- Load keywords from the structured YAML file ---
    keywords_path = '/opt/airflow/config/keywords.yaml'
    global_roles = []
    global_levels = []
    logging.info(f"Attempting to load keywords from: {keywords_path}")
    try:
        with open(keywords_path, 'r') as f:
            keywords_data = yaml.safe_load(f)
            global_roles = keywords_data.get('roles', [])
            global_levels = keywords_data.get('levels', [])
            logging.info(f"Loaded {len(global_roles)} roles and {len(global_levels)} levels.")
    except FileNotFoundError:
        logging.error(f"Keywords file not found at {keywords_path}.")
    except yaml.YAMLError as e:
        logging.error(f"Error parsing keywords file: {e}")

    # Load site configurations
    logging.info(f"Attempting to load site configuration from: {config_path}")
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except (FileNotFoundError, yaml.YAMLError) as e:
        logging.error(f"Failed to load site configurations: {e}")
        return []

    if 'target_sites' not in config_data or not isinstance(config_data['target_sites'], list):
        logging.error("YAML Config must contain a 'target_sites' list.")
        return []

    for site_config in config_data['target_sites']:
        try:
            site_jobs = scrape_site(
                company_name=site_config.get('company_name'),
                base_url=site_config.get('base_url'),
                search_path=site_config.get('search_path'),
                global_roles=global_roles,
                global_levels=global_levels,
                location_query=site_config.get('location_query', ''),
                selectors=site_config.get('selectors', {}),
                max_pages=site_config.get('max_pages', 1),
                extra_params=site_config.get('extra_query_params')
            )
            all_scraped_jobs.extend(site_jobs)
        except Exception as e:
            logging.error(f"Error scraping site '{site_config.get('company_name')}': {e}", exc_info=True)

    logging.info(f"Completed all scrapers. Total jobs found: {len(all_scraped_jobs)}")
    return all_scraped_jobs