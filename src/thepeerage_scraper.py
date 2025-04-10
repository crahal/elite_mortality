import os
import csv
import requests
import time
import re
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging

# --- Global Constants ---
GLOBAL_SLEEP = 0.5       # Global sleep time in seconds after each successful fetch.
MAX_RETRIES = 10         # Maximum number of retries for each fetch.
BACKOFF_FACTOR = 60      # Base time (in seconds) for exponential backoff.

# --- Logger Configuration ---
log_dir = '../logging'
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
log_filename = os.path.join(log_dir, f"{timestamp}.log")
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("Logger configured successfully. Logging to file: %s", log_filename)


# --- Utility Function ---
def fetch_page(session, url, headers, max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR):
    """
    Fetches the content from the given URL with exponential backoff.

    Parameters:
        session (requests.Session): The session to use for the request.
        url (str): The URL to fetch.
        headers (dict): HTTP headers to include.
        max_retries (int): Maximum number of retries.
        backoff_factor (int): Base time (in seconds) to wait between retries.

    Returns:
        bytes or None: The content of the page if successful, otherwise None.
    """
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                return response.content
            else:
                logging.warning("Status code %s for URL %s. Retrying after delay.", response.status_code, url)
        except Exception as e:
            logging.error("Exception fetching %s: %s", url, e)
        time.sleep(backoff_factor * (attempt + 1))
    return None


# --- Person Scraper Functions ---
def parse_person(person, clarendonlist):
    """
    Parses a person div element and extracts the required information.

    Parameters:
        person (bs4.element.Tag): The BeautifulSoup object corresponding to a person.
        clarendonlist (list): List of school names to check in the narrative.

    Returns:
        dict: A dictionary containing the parsed data.
    """
    name_tag = person.find("h2", class_="sn sect-sn")
    if name_tag:
        for anchor in name_tag.find_all('a'):
            anchor.unwrap()
        for sup in name_tag.find_all('sup'):
            sup.decompose()
        parts = name_tag.get_text(separator=',').split(',')
        fullname = parts[0].strip() if parts else 'N/A'
        title = parts[1].strip() if len(parts) > 1 else 'N/A'
    else:
        fullname = 'N/A'
        title = 'N/A'

    info_block = person.find("div", class_="sinfo sect-ls")
    gender = 'N/A'
    person_id = 'N/A'
    born = 'N/A'
    died = 'N/A'
    if info_block:
        info_text = info_block.get_text(separator=',')
        for trait in info_text.split(','):
            trait = trait.strip().lower()
            if trait == 'm':
                gender = 'M'
            elif trait == 'f':
                gender = 'F'
            if '#' in trait:
                person_id = trait.replace('#', '').strip()
            if 'b.' in trait:
                born = trait.replace('b.', '').strip()
            if 'd.' in trait:
                died = trait.replace('d.', '').strip()

    narr_tag = person.find("div", class_="narr")
    narr = narr_tag.get_text(strip=True).replace('\xa0', '') if narr_tag else 'N/A'
    clarendon = 0
    for school in clarendonlist:
        if school.lower() in narr.lower():
            clarendon = 1
            break  # Deterministic output.
    narr_lower = narr.lower()
    if 'oxford' in narr_lower and 'cambridge' in narr_lower:
        oxbridge = 'both'
    elif 'oxford' in narr_lower and 'univ' in narr_lower:
        oxbridge = 'oxford'
    elif 'cambridge' in narr_lower and 'univ' in narr_lower:
        oxbridge = 'cambridge'
    else:
        oxbridge = 'N/A'
    last_edit_tag = person.find("span", class_="field-le-value")
    last_edit = last_edit_tag.get_text(strip=True) if last_edit_tag else 'N/A'
    sources = ';'.join([a.get_text() for a in person.find_all('a', href=True) if '.htm#s' in a.get('href', '')])

    ul_tags = person.find_all('ul')
    if ul_tags:
        li_texts = []
        for li in ul_tags[0].find_all('li'):
            match = re.search(r'#i(.*?)"', str(li))
            if match:
                li_texts.append(match.group(1))
        child = ';'.join(li_texts) if li_texts else 'N/A'
    else:
        child = 'N/A'

    return {
        'fullname': fullname,
        'title': title,
        'gender': gender,
        'id': person_id,
        'born': born,
        'died': died,
        'narr': narr,
        'clarendon': clarendon,
        'oxbridge': oxbridge,
        'child': child,
        'lastedit': last_edit,
        'sources': sources
    }


def build_full_dataset(rawpath, maxpersonpage, baseurl, headers, clarendonlist):
    """
    Builds the dataset by scraping each person page and writing the results to a TSV file.

    Parameters:
        rawpath (str): The directory where the output file will be stored.
        maxpersonpage (int): The maximum number of person pages to scrape.
        baseurl (str): The base URL for constructing page URLs.
        headers (dict): HTTP headers to include with requests.
        clarendonlist (list): List of schools to search for in narratives.
    """
    output_file = os.path.join(rawpath, 'entire_thepeerage.tsv')
    with open(output_file, 'w', newline='') as fileout:
        fieldnames = ['Page', 'ID', 'fullname', 'title', 'gender', 'born', 'died',
                      'narr', 'clarendon', 'oxbridge', 'child', 'lastedit', 'sources']
        writer = csv.DictWriter(fileout, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()

        session = requests.Session()
        for page in tqdm(range(1, maxpersonpage + 1), desc="Persons"):
            url = f"{baseurl}p{page}.htm"
            content = fetch_page(session, url, headers, max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR)
            if content is None:
                logging.error("Failed to fetch page %s after %s attempts. Skipping.", page, MAX_RETRIES)
                continue
            soup = BeautifulSoup(content, 'html.parser')
            time.sleep(GLOBAL_SLEEP)
            for person in soup.find_all("div", class_="itp"):
                person_data = parse_person(person, clarendonlist)
                person_data['Page'] = str(page)
                person_data['ID'] = person_data.pop('id')
                writer.writerow(person_data)
    logging.info("Completed building the full dataset.")


# --- British Peers Scraper ---
def build_british_peers(rawpath, baseurl, headers):
    """
    Scrapes British peers and orders, saving the results to a TSV file.

    Parameters:
        rawpath (str): The directory where the output file will be stored.
        baseurl (str): The base URL for constructing page URLs.
        headers (dict): HTTP headers for requests.
    """
    output_file = os.path.join(rawpath, 'british_peers_and_orders.tsv')
    logging.info("Starting to build British peers dataset: %s", output_file)

    with open(output_file, 'w', newline='') as fileout:
        writer = csv.writer(fileout, delimiter='\t', lineterminator='\n')
        writer.writerow(['type', 'id'])
        session = requests.Session()

        types_of_lord = [
            'marquess', 'duke', 'earl', 'viscount', 'baron',
            'baron_by_writ', 'life_peer', 'law_lord',
            'scot_law_lord', 'baronet', 'jacobite', 'feudal',
            'clan_chief'
        ]

        for typeoflord in tqdm(types_of_lord, desc="British Peers"):
            url = f"{baseurl}index_{typeoflord}.htm"
            logging.info("Fetching URL: %s", url)

            content = fetch_page(session, url, headers, max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR)
            if content is None:
                logging.error("Failed to fetch URL %s after %s attempts. Skipping.", url, MAX_RETRIES)
                continue

            soup = BeautifulSoup(content, 'lxml')
            time.sleep(GLOBAL_SLEEP)
            for element in soup.find_all(href=True):
                element_str = str(element)
                if '.htm#i' in element_str:
                    match = re.search(r'htm#i(.*?)"', element_str)
                    if match:
                        peer_id = match.group(1)
                        writer.writerow([typeoflord, peer_id])
                        logging.info("Scraped peer: type=%s, id=%s", typeoflord, peer_id)
    logging.info("Completed building British peers dataset.")


# --- Sources Scraper ---
def build_sourcedata(rawpath, maxsourcespage, baseurl, headers):
    """
    Scrapes sources, saving the results to a TSV file.

    Parameters:
        rawpath (str): The directory where the output file will be stored.
        maxsourcespage (int): The maximum number of source pages to scrape.
        baseurl (str): The base URL for constructing page URLs.
        headers (dict): HTTP headers for requests.
    """
    output_file = os.path.join(rawpath, 'sources.tsv')
    logging.info("Starting to build sources dataset: %s", output_file)
    with open(output_file, 'w', newline='') as fileout:
        writer = csv.writer(fileout, delimiter='\t', lineterminator='\n')
        writer.writerow(['Page', 'SourceID', 'Source'])
        session = requests.Session()
        for page in tqdm(range(1, maxsourcespage + 1), desc="Sources"):
            url = f"{baseurl}s{page}.htm"
            content = fetch_page(session, url, headers, max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR)
            if content is None:
                logging.error("Failed to fetch source page %s after %s attempts. Skipping.", page, MAX_RETRIES)
                continue
            soup = BeautifulSoup(content, 'html.parser')
            time.sleep(GLOBAL_SLEEP)
            for li in soup.find_all("li"):
                text = li.get_text(strip=True)
                if text.lower().startswith('[s'):
                    match = re.search(r"\[(\w+)\]", text)
                    if match:
                        sourceid = match.group(1)
                        writer.writerow([page, sourceid, text])
                        logging.info("Scraped source: page=%s, sourceID=%s", page, sourceid)
    logging.info("Completed building sources dataset.")


# --- Main Execution ---
if __name__ == "__main__":
    maxpersonpage = 75973  # as of 05/04/2025
    maxsourcespage = 166   # as of 06/12/2021
    baseurl = 'http://www.thepeerage.com/'
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/50.0.2661.102 Safari/537.36'
        )
    }
    clarendonlist = [
        'Charterhouse', 'Eton', 'Harrow', 'Merchant Taylor',
        'Rugby School', 'Shrewsbury School', 'St Paul’s School',
        'Westminster School', 'Winchester College'
    ]
    rawpath = '../data/thepeerage/raw'
    os.makedirs(rawpath, exist_ok=True)

    # Run the scrapers in sequence.
    build_full_dataset(rawpath, maxpersonpage, baseurl, headers, clarendonlist)
    build_british_peers(rawpath, baseurl, headers)
    build_sourcedata(rawpath, maxsourcespage, baseurl, headers)
    logging.info("Tada!")