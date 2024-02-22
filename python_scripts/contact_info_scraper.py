
"""
Contact information scraping script.
"""

import os
import sys
import re
import csv
import requests
import pandas as pd
import numpy as np
import asyncio
import phonenumbers
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession
from urllib.parse import urljoin, urlparse
from alive_progress import alive_bar
from typing import ContextManager, Optional
from utils import *


# Get absolute path of script
script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

google_headers = {
    'authority': 'www.google.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'de,de-DE;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,fr;q=0.5,de-CH;q=0.4,es;q=0.3',
    'cache-control': 'no-cache',
    'dnt': '1',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Not_A Brand";v="99", "Microsoft Edge";v="109", "Chromium";v="109"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"109.0.1518.78"',
    'sec-ch-ua-full-version-list': '"Not_A Brand";v="99.0.0.0", "Microsoft Edge";v="109.0.1518.78", "Chromium";v="109.0.5414.120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"10.0.0"',
    'sec-ch-ua-wow64': '?0',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
}


def trustpilot_contact_scraper(url):
    domain = re.findall(r'https?:\/\/(?:www\.)?([a-zA-Z0-9.-]+)',  url)[0]
    trustpilot_page = f'https://www.trustpilot.com/review/{domain}'
    soup = get_website(trustpilot_page)

    # Checking if profile exists or if we land on a 404 page
    if soup.find('div', class_="errors_error404__tUqzU"):
        return None, None, None  # Return None if profile doesn't exist

    email, phone, address = '', '', ''

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
            address_lines = [li.text for li in ul_tag2.find_all('li')]
            address = ', '.join(address_lines)

    return email, phone, address


def google_contact_scraper(url):
    name = re.findall(r'[\w]+://(?:www.)?(.+?).[\w]+/',  url)[0]
    
    params = {
        'q': name,
        'hl': 'en'
    }

    phone, address = '', ''

    response = requests.get('https://www.google.com/search', params=params, headers=google_headers)
    html_text = response.content
    soup = str(BeautifulSoup(html_text, 'lxml'))

    address_regex = '<span class="LrzXr">(.*?)<\/span>'
    phone_regex = '(?:<span aria-label[^>]+?)>([0-9()+\-\s]+)(?:<\/span>)'
    
    address = re.findall(address_regex, soup) 
    phone = re.findall(phone_regex, soup) 
        
    return phone, address


def extract_soup(soup):
    soup_str = str(soup)

    # Extract all links on the page
    a_tags = list(soup.find_all("a"))
    extracted_links = [a_tag.get('href') for a_tag in a_tags if a_tag.get('href')] 
    extracted_links = list(set(extracted_links))

    # Extract emails
    email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Z|a-z]{2,}(?![\d.])\b'
    #emails_in_tags = [re.findall(email_regex, link) for link in extracted_links]
    #emails_in_tags = sum(emails_in_tags, [])
    emails = re.findall(email_regex, str(soup))
    emails = list(set(emails))

    # Extract phone numbers
    phone_regex = '\"tel\:\+*([\(\)\-0-9\ ]{1,})\"'
    phone_regex2 = '(?:\+1[\s]?)?(?:1[\s-]?)?\(?\d{3}\)?[\s-]\d{3}[\s-]\d{4}'
    phone_matches = re.findall(phone_regex, soup_str) 
    phone_matches2 = re.findall(phone_regex2, soup_str)
    phones = list(set(phone_matches + phone_matches2))

    return emails, phones


def site_scraper(url): 
    # Extract the HTML 
    #print(f"Currently scraping: {url}")
    soup = get_website(url)
    emails, phones = extract_soup(soup)
    return emails, phones


async def get_website_async(url):
    """ Fetch HTML content of a website, including dynamically loaded JavaScript content.
    """
    asession = AsyncHTMLSession() 
    r = await asession.get(url, headers=headers) 
    await r.html.arender(timeout=30) # Wait max 20 sec for JavaScript to render
    html = r.html.raw_html 
    await asession.close()
    soup = BeautifulSoup(html, 'lxml')
    return soup


async def site_scraper_async(url): 
    #print(f"Currently scraping dynamic HTML: {url}")
    soup = await get_website_async(url)
    emails, phones = extract_soup(soup)
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


async def main(args):

    if len(args) > 0:
        filepath = args[0]
    else:
        filepath = None
    if len(args) > 1:
        output_name = args[1]
    else:
        output_name = None

    try:
        while True:
            try:
                if not filepath:
                    filepath = input("Enter the path to a .csv file containing urls: ")
                filepath = str(filepath)
                url_list = []
                with open(filepath, mode='r', encoding='utf-8') as file:
                    reader = csv.reader(file)
                    next(reader, None)  # Skip header
                    for row in reader:
                        url_list.append(str(row[0]))
                break
            except ValueError:
                print("That's not a valid path. Please try again.")

        while True:
            try:
                if not output_name:
                    output_name = input("Enter the desired name of output file (don't include .csv): ")
                output_name = str(output_name)
                break
            except Exception as e:
               print(f"Error: {e}", file=sys.stderr)

        df = await pipeline(url_list)
        savepath = os.path.join(script_dir, "scraped_data", "company_data", f"{output_name}.csv")
        df.to_csv(savepath, index=False)
        print(f"[✓] Execution successful without any errors. {len(df)} websites scraped.")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    

if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
    
