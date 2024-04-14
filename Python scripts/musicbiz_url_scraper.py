
"""
This script is designed to scrape company website links from the Music Business Association's
member community page (musicbiz.org). The extracted links are saved into a CSV file at scraping/scraped_data/company_data.

To run this script, simply run 'musicbiz_url_scraper.py' without any additional arguments.

Author: Joanna Lee
"""

import os
import sys
import csv
from utils import *

musicbiz_url = 'https://musicbiz.org/about/member-community/#member-companies'
script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def scrape_url(url):
    soup = get_website(url)
    links = [a['href'] for div in soup.find_all('div', class_='sponsor-logo-wrapper') for a in div.find_all('a', href=True)]
    return links


def scrape_url_batch(url):
    links = scrape_url(url)
    file_path = os.path.join(script_dir, "scraped_data", "company_data", "musicbiz_sites.csv")

    with open(file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['url'])
        for item in links:
            csvwriter.writerow([item])

    return links


def main():
    try:
        links = scrape_url_batch(musicbiz_url)
        print(f"[✓] Execution successful without any errors. {len(links)} links scraped and saved to musicbiz_data.csv.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()