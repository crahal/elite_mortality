import os
import csv
import requests
import time
import re
from datetime import datetime
from dataclasses import dataclass
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging

# --- Global Constants ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
DEFAULT_RAWPATH = os.path.join(PROJECT_ROOT, 'data', 'thepeerage', 'raw')
DEFAULT_LOG_DIR = os.path.join(PROJECT_ROOT, 'logging')

BASE_URL = 'https://www.thepeerage.com/'
GLOBAL_SLEEP = float(os.environ.get('THEPEERAGE_SLEEP', '0.1'))
DISCOVERY_SLEEP = float(os.environ.get('THEPEERAGE_DISCOVERY_SLEEP', '0.02'))
MAX_RETRIES = int(os.environ.get('THEPEERAGE_MAX_RETRIES', '4'))
BACKOFF_FACTOR = float(os.environ.get('THEPEERAGE_BACKOFF_FACTOR', '2'))
REQUEST_TIMEOUT = float(os.environ.get('THEPEERAGE_TIMEOUT', '30'))
DISCOVERY_CONSECUTIVE_MISSES = int(os.environ.get('THEPEERAGE_DISCOVERY_MISSES', '50'))
SOURCE_DISCOVERY_CONSECUTIVE_MISSES = int(os.environ.get('THEPEERAGE_SOURCE_DISCOVERY_MISSES', '15'))

MISSING_STATUS_CODES = {404, 410}
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass
class ScrapeStats:
    pages_attempted: int = 0
    pages_fetched: int = 0
    pages_missing: int = 0
    pages_failed: int = 0
    rows_written: int = 0
    highest_linked_person_page: int = 0
    highest_linked_source_page: int = 0


def configure_logging(log_dir=DEFAULT_LOG_DIR):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log_filename = os.path.join(log_dir, f"{timestamp}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logging.info("Logger configured successfully. Logging to file: %s", log_filename)
    return log_filename


def build_url(baseurl, path):
    return urljoin(baseurl, path)


def page_url(baseurl, prefix, page):
    return build_url(baseurl, f"{prefix}{page}.htm")


def polite_sleep(seconds):
    if seconds > 0:
        time.sleep(seconds)


def create_session(headers):
    session = requests.Session()
    session.headers.update(headers)
    return session


# --- Utility Function ---
def fetch_response(
    session,
    url,
    headers=None,
    method='GET',
    max_retries=MAX_RETRIES,
    backoff_factor=BACKOFF_FACTOR,
    timeout=REQUEST_TIMEOUT,
    log_missing=True
):
    """
    Fetches a URL with bounded retries.

    Missing pages are returned immediately and are not retried. This matters for
    thePeerage because there are valid gaps in the page-number sequence.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = session.request(
                method,
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True
            )
            if response.status_code == 200:
                return response
            if response.status_code in MISSING_STATUS_CODES:
                if log_missing:
                    logging.info("Missing URL %s returned status %s.", url, response.status_code)
                return response
            if response.status_code not in RETRY_STATUS_CODES:
                logging.error("Non-retryable status %s for URL %s.", response.status_code, url)
                return response
            if attempt == max_retries:
                logging.error(
                    "Retryable status %s for URL %s after %s attempts.",
                    response.status_code,
                    url,
                    max_retries
                )
                return response

            retry_after = response.headers.get('Retry-After')
            try:
                delay = float(retry_after) if retry_after else backoff_factor * attempt
            except ValueError:
                delay = backoff_factor * attempt
            logging.warning(
                "Retryable status %s for URL %s on attempt %s/%s. Sleeping %.1fs.",
                response.status_code,
                url,
                attempt,
                max_retries,
                delay
            )
        except requests.RequestException as e:
            if attempt == max_retries:
                logging.error("Exception fetching %s after %s attempts: %s", url, max_retries, e)
                return None
            delay = backoff_factor * attempt
            logging.warning(
                "Exception fetching %s on attempt %s/%s: %s. Sleeping %.1fs.",
                url,
                attempt,
                max_retries,
                e,
                delay
            )
        polite_sleep(delay)
    return None


def fetch_page(session, url, headers, max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR):
    response = fetch_response(
        session,
        url,
        headers=headers,
        method='GET',
        max_retries=max_retries,
        backoff_factor=backoff_factor
    )
    return response.content if response is not None and response.status_code == 200 else None


def page_exists(session, baseurl, prefix, page, headers):
    response = fetch_response(
        session,
        page_url(baseurl, prefix, page),
        headers=headers,
        method='HEAD',
        max_retries=2,
        log_missing=False
    )
    if response is None:
        return False
    if response.status_code == 405:
        response = fetch_response(
            session,
            page_url(baseurl, prefix, page),
            headers=headers,
            method='GET',
            max_retries=2,
            log_missing=False
        )
    return response is not None and response.status_code == 200


def extract_linked_page_numbers(soup, prefix):
    pattern = re.compile(r'(?:^|/)' + re.escape(prefix) + r'(\d+)\.htm', re.IGNORECASE)
    page_numbers = set()
    for tag in soup.find_all(href=True):
        match = pattern.search(tag.get('href', ''))
        if match:
            page_numbers.add(int(match.group(1)))
    return page_numbers


def parse_site_metadata(content):
    soup = BeautifulSoup(content, 'html.parser')
    text = soup.get_text(' ', strip=True)
    match = re.search(r'Site updated on\s+([^;]+);\s*([\d,]+)\s+people', text, re.IGNORECASE)
    if not match:
        return {'site_updated_on': None, 'people_count': None}
    return {
        'site_updated_on': match.group(1).strip(),
        'people_count': int(match.group(2).replace(',', ''))
    }


def collect_discovery_hints(session, baseurl, headers):
    hints = {
        'person_pages': set(),
        'source_pages': set(),
        'site_updated_on': None,
        'people_count': None,
        'home_last_modified': None
    }
    discovery_pages = ['', 'master_index.htm', 'ui11.htm', 'site_map.htm']

    for path in discovery_pages:
        url = build_url(baseurl, path)
        response = fetch_response(session, url, headers=headers, method='GET')
        if response is None or response.status_code != 200:
            logging.warning("Could not fetch discovery page: %s", url)
            continue
        soup = BeautifulSoup(response.content, 'html.parser')
        hints['person_pages'].update(extract_linked_page_numbers(soup, 'p'))
        hints['source_pages'].update(extract_linked_page_numbers(soup, 's'))
        if path == '':
            metadata = parse_site_metadata(response.content)
            hints.update(metadata)
            hints['home_last_modified'] = response.headers.get('Last-Modified')
        polite_sleep(GLOBAL_SLEEP)

    logging.info(
        "Discovery hints: site_updated_on=%s, people_count=%s, home_last_modified=%s, "
        "max_linked_person_page=%s, max_linked_source_page=%s",
        hints['site_updated_on'],
        hints['people_count'],
        hints['home_last_modified'],
        max(hints['person_pages']) if hints['person_pages'] else None,
        max(hints['source_pages']) if hints['source_pages'] else None
    )
    return hints


def find_existing_page_at_or_below(session, baseurl, prefix, start_page, headers, backtrack_limit=1000):
    lower_bound = max(1, start_page - backtrack_limit)
    for page in range(start_page, lower_bound - 1, -1):
        if page_exists(session, baseurl, prefix, page, headers):
            return page
        polite_sleep(DISCOVERY_SLEEP)
    if start_page != 1 and page_exists(session, baseurl, prefix, 1, headers):
        return 1
    return None


def discover_highest_existing_page(
    session,
    baseurl,
    prefix,
    headers,
    starting_guess,
    consecutive_misses=DISCOVERY_CONSECUTIVE_MISSES
):
    """
    Discover the highest existing pNN.htm/sNN.htm page from live HTTP status.

    The site has missing pages inside the valid range, so discovery scans forward
    until it sees a run of missing pages instead of stopping at the first 404.
    """
    starting_guess = max(1, int(starting_guess or 1))
    logging.info(
        "Discovering highest %s*.htm page from starting guess %s with stop run %s.",
        prefix,
        starting_guess,
        consecutive_misses
    )
    start_page = find_existing_page_at_or_below(session, baseurl, prefix, starting_guess, headers)
    if start_page is None:
        raise RuntimeError(f"Could not find an existing {prefix}*.htm page at or below {starting_guess}.")

    highest_existing = start_page
    page = start_page + 1
    missing_run = 0
    while missing_run < consecutive_misses:
        if page_exists(session, baseurl, prefix, page, headers):
            highest_existing = page
            missing_run = 0
        else:
            missing_run += 1
        page += 1
        polite_sleep(DISCOVERY_SLEEP)

    logging.info(
        "Highest live %s*.htm page discovered: %s. Stopped after %s consecutive missing pages.",
        prefix,
        highest_existing,
        consecutive_misses
    )
    return highest_existing


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
    narr = narr_tag.get_text(' ', strip=True).replace('\xa0', ' ') if narr_tag else 'N/A'
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
    source_ids = []
    for anchor in person.find_all('a', href=True):
        if re.search(r's\d+\.htm#s\d+', anchor.get('href', ''), re.IGNORECASE):
            source_id = anchor.get_text(strip=True)
            if source_id and source_id not in source_ids:
                source_ids.append(source_id)
    sources = ';'.join(source_ids)

    child_ids = []
    for anchor in person.select('.fams ul li a[href*="#i"]'):
        match = re.search(r'#i(\d+)', anchor.get('href', ''))
        if match and match.group(1) not in child_ids:
            child_ids.append(match.group(1))
    child = ';'.join(child_ids) if child_ids else 'N/A'

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


def build_full_dataset(rawpath, maxpersonpage, baseurl, headers, clarendonlist, session=None):
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
    logging.info("Starting person dataset: %s through page %s", output_file, maxpersonpage)
    stats = ScrapeStats()
    if session is None:
        session = create_session(headers)

    with open(output_file, 'w', newline='', encoding='utf-8') as fileout:
        fieldnames = ['Page', 'ID', 'fullname', 'title', 'gender', 'born', 'died',
                      'narr', 'clarendon', 'oxbridge', 'child', 'lastedit', 'sources']
        writer = csv.DictWriter(fileout, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()

        page = 1
        with tqdm(total=maxpersonpage, desc="Persons") as pbar:
            while page <= maxpersonpage:
                url = page_url(baseurl, 'p', page)
                response = fetch_response(
                    session,
                    url,
                    headers=headers,
                    max_retries=MAX_RETRIES,
                    backoff_factor=BACKOFF_FACTOR,
                    log_missing=False
                )
                stats.pages_attempted += 1
                if response is None:
                    stats.pages_failed += 1
                    logging.error("Failed to fetch person page %s after %s attempts. Skipping.", page, MAX_RETRIES)
                    page += 1
                    pbar.update(1)
                    continue
                if response.status_code in MISSING_STATUS_CODES:
                    stats.pages_missing += 1
                    logging.info("Person page %s is missing; skipping.", page)
                    page += 1
                    pbar.update(1)
                    continue
                if response.status_code != 200:
                    stats.pages_failed += 1
                    logging.error("Unexpected status %s for person page %s. Skipping.", response.status_code, page)
                    page += 1
                    pbar.update(1)
                    continue

                stats.pages_fetched += 1
                soup = BeautifulSoup(response.content, 'html.parser')
                linked_person_pages = extract_linked_page_numbers(soup, 'p')
                linked_source_pages = extract_linked_page_numbers(soup, 's')
                if linked_person_pages:
                    stats.highest_linked_person_page = max(
                        stats.highest_linked_person_page,
                        max(linked_person_pages)
                    )
                if linked_source_pages:
                    stats.highest_linked_source_page = max(
                        stats.highest_linked_source_page,
                        max(linked_source_pages)
                    )

                if stats.highest_linked_person_page > maxpersonpage:
                    old_maxpersonpage = maxpersonpage
                    maxpersonpage = stats.highest_linked_person_page
                    pbar.total = maxpersonpage
                    pbar.refresh()
                    logging.info(
                        "Extending person scrape from page %s to linked page %s.",
                        old_maxpersonpage,
                        maxpersonpage
                    )

                for person in soup.find_all("div", class_="itp"):
                    person_data = parse_person(person, clarendonlist)
                    person_data['Page'] = str(page)
                    person_data['ID'] = person_data.pop('id')
                    writer.writerow(person_data)
                    stats.rows_written += 1
                polite_sleep(GLOBAL_SLEEP)
                page += 1
                pbar.update(1)

    logging.info(
        "Completed person dataset: attempted=%s, fetched=%s, missing=%s, failed=%s, rows=%s, "
        "highest_linked_person_page=%s, highest_linked_source_page=%s",
        stats.pages_attempted,
        stats.pages_fetched,
        stats.pages_missing,
        stats.pages_failed,
        stats.rows_written,
        stats.highest_linked_person_page,
        stats.highest_linked_source_page
    )
    return stats


# --- British Peers Scraper ---
def build_british_peers(rawpath, baseurl, headers, session=None):
    """
    Scrapes British peers and orders, saving the results to a TSV file.

    Parameters:
        rawpath (str): The directory where the output file will be stored.
        baseurl (str): The base URL for constructing page URLs.
        headers (dict): HTTP headers for requests.
    """
    output_file = os.path.join(rawpath, 'british_peers_and_orders.tsv')
    logging.info("Starting to build British peers dataset: %s", output_file)
    stats = ScrapeStats()
    if session is None:
        session = create_session(headers)

    with open(output_file, 'w', newline='', encoding='utf-8') as fileout:
        writer = csv.writer(fileout, delimiter='\t', lineterminator='\n')
        writer.writerow(['type', 'id'])

        types_of_lord = [
            'marquess', 'duke', 'earl', 'viscount', 'baron',
            'baron_by_writ', 'life_peer', 'law_lord',
            'scot_law_lord', 'baronet', 'jacobite', 'feudal',
            'clan_chief'
        ]

        for typeoflord in tqdm(types_of_lord, desc="British Peers"):
            url = build_url(baseurl, f"index_{typeoflord}.htm")
            logging.info("Fetching URL: %s", url)

            response = fetch_response(
                session,
                url,
                headers=headers,
                max_retries=MAX_RETRIES,
                backoff_factor=BACKOFF_FACTOR
            )
            stats.pages_attempted += 1
            if response is None:
                stats.pages_failed += 1
                logging.error("Failed to fetch URL %s after %s attempts. Skipping.", url, MAX_RETRIES)
                continue
            if response.status_code in MISSING_STATUS_CODES:
                stats.pages_missing += 1
                logging.error("British peer index URL %s is missing. Skipping.", url)
                continue
            if response.status_code != 200:
                stats.pages_failed += 1
                logging.error("Unexpected status %s for URL %s. Skipping.", response.status_code, url)
                continue

            stats.pages_fetched += 1
            soup = BeautifulSoup(response.content, 'lxml')
            for element in soup.find_all(href=True):
                match = re.search(r'#i(\d+)', element.get('href', ''))
                if match:
                    peer_id = match.group(1)
                    writer.writerow([typeoflord, peer_id])
                    stats.rows_written += 1
            polite_sleep(GLOBAL_SLEEP)
    logging.info(
        "Completed British peers dataset: attempted=%s, fetched=%s, missing=%s, failed=%s, rows=%s",
        stats.pages_attempted,
        stats.pages_fetched,
        stats.pages_missing,
        stats.pages_failed,
        stats.rows_written
    )
    return stats


# --- Sources Scraper ---
def build_sourcedata(rawpath, maxsourcespage, baseurl, headers, session=None):
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
    stats = ScrapeStats()
    if session is None:
        session = create_session(headers)

    with open(output_file, 'w', newline='', encoding='utf-8') as fileout:
        writer = csv.writer(fileout, delimiter='\t', lineterminator='\n')
        writer.writerow(['Page', 'SourceID', 'Source'])

        page = 1
        with tqdm(total=maxsourcespage, desc="Sources") as pbar:
            while page <= maxsourcespage:
                url = page_url(baseurl, 's', page)
                response = fetch_response(
                    session,
                    url,
                    headers=headers,
                    max_retries=MAX_RETRIES,
                    backoff_factor=BACKOFF_FACTOR,
                    log_missing=False
                )
                stats.pages_attempted += 1
                if response is None:
                    stats.pages_failed += 1
                    logging.error("Failed to fetch source page %s after %s attempts. Skipping.", page, MAX_RETRIES)
                    page += 1
                    pbar.update(1)
                    continue
                if response.status_code in MISSING_STATUS_CODES:
                    stats.pages_missing += 1
                    logging.info("Source page %s is missing; skipping.", page)
                    page += 1
                    pbar.update(1)
                    continue
                if response.status_code != 200:
                    stats.pages_failed += 1
                    logging.error("Unexpected status %s for source page %s. Skipping.", response.status_code, page)
                    page += 1
                    pbar.update(1)
                    continue

                stats.pages_fetched += 1
                soup = BeautifulSoup(response.content, 'html.parser')
                linked_source_pages = extract_linked_page_numbers(soup, 's')
                if linked_source_pages:
                    stats.highest_linked_source_page = max(
                        stats.highest_linked_source_page,
                        max(linked_source_pages)
                    )
                if stats.highest_linked_source_page > maxsourcespage:
                    old_maxsourcespage = maxsourcespage
                    maxsourcespage = stats.highest_linked_source_page
                    pbar.total = maxsourcespage
                    pbar.refresh()
                    logging.info(
                        "Extending source scrape from page %s to linked page %s.",
                        old_maxsourcespage,
                        maxsourcespage
                    )

                for li in soup.find_all("li"):
                    text = li.get_text(' ', strip=True)
                    if text.lower().startswith('[s'):
                        match = re.search(r"\[(S\d+)\]", text, re.IGNORECASE)
                        if match:
                            sourceid = match.group(1).upper()
                            writer.writerow([page, sourceid, text])
                            stats.rows_written += 1
                polite_sleep(GLOBAL_SLEEP)
                page += 1
                pbar.update(1)
    logging.info(
        "Completed sources dataset: attempted=%s, fetched=%s, missing=%s, failed=%s, rows=%s, "
        "highest_linked_source_page=%s",
        stats.pages_attempted,
        stats.pages_fetched,
        stats.pages_missing,
        stats.pages_failed,
        stats.rows_written,
        stats.highest_linked_source_page
    )
    return stats


# --- Main Execution ---
if __name__ == "__main__":
    run_started = time.monotonic()
    log_filename = configure_logging()
    baseurl = BASE_URL
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/50.0.2661.102 Safari/537.36'
        ),
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }
    clarendonlist = [
        'Charterhouse', 'Eton', 'Harrow', 'Merchant Taylor',
        'Rugby School', 'Shrewsbury School', "St Paul's School",
        'Westminster School', 'Winchester College'
    ]
    rawpath = DEFAULT_RAWPATH
    os.makedirs(rawpath, exist_ok=True)

    logging.info("Starting thePeerage scrape. baseurl=%s rawpath=%s", baseurl, rawpath)
    session = create_session(headers)

    hints = collect_discovery_hints(session, baseurl, headers)
    people_count_guess = ((hints['people_count'] + 9) // 10) if hints['people_count'] else 1
    linked_person_guess = max(hints['person_pages']) if hints['person_pages'] else 1
    linked_source_guess = max(hints['source_pages']) if hints['source_pages'] else 1

    maxpersonpage = discover_highest_existing_page(
        session,
        baseurl,
        'p',
        headers,
        max(people_count_guess, linked_person_guess, 1),
        consecutive_misses=DISCOVERY_CONSECUTIVE_MISSES
    )
    maxsourcespage = discover_highest_existing_page(
        session,
        baseurl,
        's',
        headers,
        max(linked_source_guess, 1),
        consecutive_misses=SOURCE_DISCOVERY_CONSECUTIVE_MISSES
    )

    person_stats = build_full_dataset(rawpath, maxpersonpage, baseurl, headers, clarendonlist, session=session)
    build_british_peers(rawpath, baseurl, headers, session=session)

    required_source_page = max(maxsourcespage, person_stats.highest_linked_source_page)
    if required_source_page > maxsourcespage:
        logging.info(
            "Raising source page limit from %s to %s based on person-page citation links.",
            maxsourcespage,
            required_source_page
        )
    build_sourcedata(rawpath, required_source_page, baseurl, headers, session=session)

    duration = time.monotonic() - run_started
    logging.info(
        "Completed thePeerage scrape in %.1fs. site_updated_on=%s, people_count=%s, "
        "maxpersonpage=%s, maxsourcespage=%s, log_file=%s",
        duration,
        hints['site_updated_on'],
        hints['people_count'],
        maxpersonpage,
        required_source_page,
        log_filename
    )
    print(f"Scrape complete. Log file: {log_filename}")
