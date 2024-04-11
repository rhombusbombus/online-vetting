
"""
Contact information scraping script. Extracts contact info from multiple different
sources in the following order (#4 and #5 are last resort methods):
    1. TrustPilot profile page
    2. Google business page
    3. Root directory of company website (regular html)
    4. Subdirectories of company website (regular html)
    5. Root directory of company website (dynamic html)

To run this script, simply edit the config file as necessary (or provide the path to a custom
config file as an argument when running the script) and run 'python contact_info_scraper.py'.

The extracted data is saved as CSV files at scraping/scraped_data/company_data.


Need to implement/improve:
    - ignore file extensions like '.png' when scraping emails
    - check fb profiles as well as social media profiles


Author: Joanna Lee
"""

import os
import sys
import json

import pandas as pd
import numpy as np

import re
import phonenumbers
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

import requests
import asyncio
from requests_html import AsyncHTMLSession

from alive_progress import alive_bar
from utils import *

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Config:
    """Loads in configuration settings for contact info scraping setup.
    
    Attributes:
        column_name (str): Default column name in CSV for names list.
        names_path (str): Path to the CSV file containing names to scrape.
        output_filepath (str): Directory path for saving scraped data.
    """
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            config = json.load(file)

        self.validate_config(config)

        self.names_path = os.path.join(parent_dir, config['names_path'])
        self.column_name = config['column_name']
        self.names_list = load_csv_list(self.names_path, self.column_name)
        self.output_filepath = os.path.join(parent_dir, config['output_filepath'])

    @staticmethod
    def validate_config(config):
        """Validates required fields in the configuration."""
        required_fields = ['names_path', 'column_name', 'output_filepath']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")


def trustpilot_contact_scraper(url):
    domain = extract_domain(url)
    trustpilot_page = f'https://www.trustpilot.com/review/{domain}'
    soup = get_website(trustpilot_page)
    email, phone, address = '', '', ''

    if soup.find('div', class_="errors_error404__tUqzU"):
        return None, None, None  

    ul_tag = soup.find('ul', class_="styles_contactInfoElements__YqQAJ") # Tag for review card

    if ul_tag:
        # Find the <a> tag for email
        a_tag = ul_tag.find('a', href=lambda href: href and 'mailto:' in href)
        if a_tag and 'mailto:' in a_tag['href']:
            email = a_tag['href'].replace('mailto:', '')

        # Find the <li> tag for phone
        li_tag = ul_tag.find('li', class_="styles_contactInfoElement__SxlS3")
        if li_tag:
            a_tag2 = li_tag.find('a', href=lambda href: href and 'tel:' in href)
            if a_tag2 and 'tel:' in a_tag2['href']:
                phone = a_tag2['href'].replace('tel:', '')
                
        # Find the <li> tag for address
        ul_tag2 = ul_tag.find('ul', class_="typography_body-m__xgxZ_ typography_appearance-default__AAY17 styles_contactInfoAddressList__RxiJI")
        if ul_tag2:
            address = ', '.join([li.text for li in ul_tag2.find_all('li')])

    return email, phone, address


def google_contact_scraper(url):
    phone, address = '', ''
    name = extract_company_name(url)
    params = {
        'q': name,
        'hl': 'en'
    }
    
    response = requests.get('https://www.google.com/search', params=params, headers=google_headers)
    soup = str(BeautifulSoup(response.content, 'lxml'))
    
    address = re.findall('<span class="LrzXr">(.*?)<\/span>', soup) 
    phone = re.findall('(?:<span aria-label[^>]+?)>([0-9()+\-\s]+)(?:<\/span>)', soup) 
        
    return phone, address


def extract_contacts_from_soup(soup):
    soup_str = str(soup)

    # Extract all links on the page
    # a_tags = list(soup.find_all("a"))
    # extracted_links = [a_tag.get('href') for a_tag in a_tags if a_tag.get('href')] 
    # extracted_links = list(set(extracted_links))

    # Extract emails
    email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Z|a-z]{2,}(?![\d.])\b'
    emails = list(set(re.findall(email_regex, str(soup))))

    # Extract phone numbers
    phone_regex = '\"tel\:\+*([\(\)\-0-9\ ]{1,})\"'
    phone_regex2 = '(?:\+1[\s]?)?(?:1[\s-]?)?\(?\d{3}\)?[\s-]\d{3}[\s-]\d{4}'
    phone_matches = re.findall(phone_regex, soup_str) 
    phone_matches2 = re.findall(phone_regex2, soup_str)
    phones = list(set(phone_matches + phone_matches2))

    return emails, phones


def site_scraper(url): 
    # Extract the HTML 
    print(f"Currently scraping: {url}")
    soup = get_website(url)
    emails, phones = extract_contacts_from_soup(soup)
    return emails, phones


async def get_website_async(url):
    """ Fetch HTML content of a website, including dynamic JS content.
    """
    asession = AsyncHTMLSession() 
    r = await asession.get(url, headers=headers) 
    await r.html.arender(timeout=30) # Wait max 20 sec for JavaScript to render
    html = r.html.raw_html 
    await asession.close()
    return BeautifulSoup(html, 'lxml')


async def site_scraper_async(url): 
    print(f"Currently scraping dynamic HTML: {url}")
    soup = await get_website_async(url)
    emails, phones = extract_contacts_from_soup(soup)
    return emails, phones


def get_first_level_directories(url):
    soup = get_website(url)
    links = set()
    domain = urlparse(url).netloc.replace('www.', '', 1)

    for link in soup.find_all('a', href=True):
        full_link = urljoin(url, link['href'])  # Resolve relative URLs
        parsed_link = urlparse(full_link)
        path_segments = [segment for segment in parsed_link.path.split('/') if segment]

        # Check if the link is a first sub-level directory
        parsed_domain = parsed_link.netloc.replace('www.', '', 1)
        if (parsed_domain == domain) and (len(path_segments) == 1):
            links.add(f"{parsed_link.scheme}://{parsed_domain}/{path_segments[0]}")

    return links


async def pipeline(urls):
    email_dict, phone_dict, address_dict = {}, {}, {}
    keywords = ["contact", "about", "faq", "help", "privacy", "terms"]
    
    with alive_bar(total=len(urls), spinner=None) as bar:
        for url in urls:
            # Scraping TrustPilot profile
            emails_list, phones_list, address_list = trustpilot_contact_scraper(url)
            emails_list = [emails_list]
            phones_list = [phones_list]
            address_list = [address_list]
            email_dict.update({url:emails_list})
            phone_dict.update({url:phones_list})
            address_dict.update({url:address_list})

            # Scraping Google listings
            phones_list, address_list = google_contact_scraper(url)
            if phone_dict[url]:
                phone_dict[url].extend(phones_list)
            else:
                phone_dict[url] = phones_list
            if address_dict[url]:
                address_dict[url].extend(address_list)
            else:
                address_dict[url] = address_list

            # Scraping the root directory
            emails_list, phones_list = site_scraper(url)
            if phone_dict[url]:
                phone_dict[url].extend(phones_list)
            else:
                phone_dict[url] = phones_list
            if email_dict[url]:
                email_dict[url].extend(emails_list)
            else:
                email_dict[url] = emails_list
            
            # Scraping the sub directories
            if all(x is None or x == "" for x in email_dict[url]):
                subdirs = get_first_level_directories(url)
                subdirs = [link for link in subdirs if any(keyword in link for keyword in keywords)]
                for subdir in subdirs:
                    if not all(x is None or x == "" for x in email_dict[url]):
                        break
                    emails_list, phones_list = site_scraper(subdir)
                    if phone_dict[url]:
                        phone_dict[url].extend(phones_list)
                    else:
                        phone_dict[url] = phones_list
                    if email_dict[url]:
                        email_dict[url].extend(emails_list)
                    else:
                        email_dict[url] = emails_list

            # If no emails found, fallback to slower scraping method
            if all(x is None or x == "" for x in email_dict[url]):
                emails_list, phones_list = await site_scraper_async(url)
                if phone_dict[url]:
                    phone_dict[url].extend(phones_list)
                else:
                    phone_dict[url] = phones_list
                if email_dict[url]:
                    email_dict[url].extend(emails_list)
                else:
                    email_dict[url] = emails_list
            bar()

    new_email_dict = {k: [v] if v else [np.nan] for k, v in email_dict.items()}
    new_phone_dict = {k: [v] if v else [np.nan] for k, v in phone_dict.items()}
    new_address_dict = {k: [v] if v else [np.nan] for k, v in address_dict.items()}

    email_df = pd.DataFrame.from_dict(new_email_dict, orient='index', columns=['emails'])
    phone_df = pd.DataFrame.from_dict(new_phone_dict, orient='index', columns=['phone numbers'])
    address_df = pd.DataFrame.from_dict(new_address_dict, orient='index', columns=['addresses'])

    # Cleaning and formatting
    email_df.loc[:, 'emails'] = email_df['emails'].apply(lambda email_list: [item for item in email_list if item and item.strip()])
    email_df.loc[:, 'emails'] = email_df['emails'].apply(lambda email_list: list(set(email_list)))

    phone_df.loc[:, 'phone numbers'] = phone_df['phone numbers'].apply(lambda phone_list: [item for item in phone_list if item and item.strip()])
    phone_df.loc[:, 'phone numbers'] = phone_df['phone numbers'].apply(lambda phone_list: [phonenumbers.format_number(phonenumbers.parse(number, 'US'), phonenumbers.PhoneNumberFormat.E164)for number in phone_list])
    phone_df.loc[:, 'phone numbers'] = phone_df['phone numbers'].apply(lambda phone_list: list(set(phone_list)))

    address_df.loc[:, 'addresses'] = address_df['addresses'].apply(lambda address_list: [item for item in address_list if item and item.strip()])
    address_df.loc[:, 'addresses'] = address_df['addresses'].apply(lambda address_list: list(set(address_list)))

    base_df = pd.DataFrame({"website": urls, "name": extract_company_name_batch(urls)}, index=urls)

    return base_df.join(email_df).join(phone_df).join(address_df)


async def main(config_file):
    try:
        config = Config(config_file)

        df = await pipeline(config.names_list)
        savepath = os.path.join(parent_dir, config.output_filepath)
        df.to_csv(savepath, index=False)
        print(f"[✓] Execution successful without any errors. {len(df)} websites scraped.")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    

if __name__ == "__main__":
    # Default configuration file path
    default_config_path = os.path.join(parent_dir, "python_scripts", "contact_info_scraper_config.json")
    
    # Check if the user has provided a custom config file
    if len(sys.argv) >= 2:
        config_file_path = sys.argv[1]
    else:
        print(f"No configuration file provided. Using default configuration: {default_config_path}")
        config_file_path = default_config_path

    asyncio.run(main(config_file_path))
    
