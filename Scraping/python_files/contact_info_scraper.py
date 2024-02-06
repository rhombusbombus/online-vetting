#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Contact information scraping script.
"""

import sys
import logging
import os
import re
import json
import scrapy
import requests
import pandas as pd
import warnings
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from urllib.parse import urlparse
from googlesearch import search

logging.getLogger('scrapy').propagate = False
warnings.filterwarnings('ignore')

# Get absolute path of script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Import necessary data files
filepath = os.path.join(script_dir, "../company_data/music_services.csv")
if os.path.exists(filepath):
    # Load in the CSV file as a list
    music_services = pd.read_csv(filepath)
    music_services = music_services['music_services'].tolist()
else:
    raise ValueError("Missing music.services.csv")


class ContactInfoSpider(scrapy.Spider):
    
    name = 'scraper'
    emails_per_site = {}
    numbers_per_site = {}
    
    def __init__(self, start_urls, *args, **kwargs):
        """Instantiate a new spider object.
        """
        super(ContactInfoSpider, self).__init__(*args, **kwargs)
        self.start_urls = start_urls
         

    def parse(self, response):
        """Extract links from the top-level directory of the website and parse them.
        """
        
        if 'depth' in response.meta and response.meta['depth'] > 0:
            return  # Do not proceed if this is not the main page.
        
        company_name = extract_company_name(response.url)
        links = LxmlLinkExtractor(allow=company_name).extract_links(response)
        
        if links:
            for link in links:
                yield scrapy.Request(url=link.url, callback=self.parse_link, meta={'link': response.url})
        else:
            self.emails_per_site[response.url] = []
            self.numbers_per_site[response.url] = []
    
    
    def parse_link(self, response):
        """ Extract specific elements/info from html with Regex expression.
        """        
        html_text = str(response.text)
        
        # Find all matches to email and phone Regex expressions.
        email_matches, phone_matches = extract_email_phone(html_text)
        
        # Add emails and phone numbers to class-level attribute.
        parent_link = response.meta['link'] 
        
        if parent_link in self.emails_per_site:
            self.emails_per_site[parent_link].extend(email_matches)
        else:
            self.emails_per_site[parent_link] = email_matches

        if parent_link in self.numbers_per_site:
            self.numbers_per_site[parent_link].extend(phone_matches)
        else:
            self.numbers_per_site[parent_link] = phone_matches
    
    
def get_contact_info(start_urls):
    """Main function that runs the entire script.
    """
    # First, check html of the main page of the website 
    emails_per_site = {}
    numbers_per_site = {}
    
    for website in music_services:
        response = requests.get(website, headers=headers)
        html_text = response.content
        soup = str(BeautifulSoup(html_text, 'html.parser'))
        email_matches, phone_matches = extract_email_phone(soup)
        emails_per_site[website] = email_matches
        numbers_per_site[website] = phone_matches
    
    # Begin crawl
    process = CrawlerProcess({'USER_AGENT': 'Mozilla/5.0'})
    process.crawl(ContactInfoSpider, start_urls=start_urls)
    ContactInfoSpider.emails_per_site.update(emails_per_site)
    ContactInfoSpider.numbers_per_site.update(numbers_per_site)
    process.start()

    # Remove duplicates
    for website, emails in ContactInfoSpider.emails_per_site.items():
        ContactInfoSpider.emails_per_site[website] = list(set(emails))
    for website, numbers in ContactInfoSpider.numbers_per_site.items():
        ContactInfoSpider.numbers_per_site[website] = list(set(numbers))

    # Create dataframe
    data1 = {'website': [], 'email': []}
    data2 = {'website': [], 'phone number': []}
    
    for website, emails in ContactInfoSpider.emails_per_site.items():
        data1['website'].append(website)
        data1['email'].append(emails)
        
    for website, numbers in ContactInfoSpider.numbers_per_site.items():
        data2['website'].append(website)
        data2['phone number'].append(numbers)
        
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
    
    # Merge the dataframes
    df = pd.merge(df1, df2, on='website', how='outer')
    
    return df


def extract_company_name(url):
    """ Extract the company domain from the website URL using regex.
    """
    match = re.search(r'[\w]+://(?:www.)?(.+?).[\w]+/', url)
    if match:
        return match.group(1)
    else:
        return ""
    
    
def extract_email_phone(html_text):
    email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Z|a-z]{2,}(?![\d.])\b'
    phone_regex = '\"tel\:\+*([\(\)\-0-9\ ]{1,})\"'
    phone_regex2 = '(?:\+1[\s]?)?(?:1[\s-]?)?\(?\d{3}\)?[\s-]\d{3}[\s-]\d{4}'
    email_matches = re.findall(email_regex, html_text) 
    phone_matches = re.findall(phone_regex, html_text) 
    phone_matches2 = re.findall(phone_regex2, html_text)
    phone_matches = phone_matches + phone_matches2
    return email_matches, phone_matches
    
    
def add_company_name_column(df):
    """ Add a column named "generic email" to the DataFrame.
    """
    is_domain_email = []
    company_names = []

    for website, emails in zip(df['website'], df['email']):
        name = extract_company_name(website)
        company_names.append(name)
        if emails:
            not_generic = any(name in email for email in emails)
            is_domain_email.append(1 if not_generic else 0)
        else:
            is_domain_email.append(0)

    df['name'] = company_names
    df['is domain email'] = is_domain_email
    return df


def clean_phone_number(numbers):
    result = [re.sub(r'\D', '', number) for number in numbers]
    result = list(set(result))
    return result


headers = {
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

def local_listing_query(df):
    results = []
    
    for name in df["name"]:
        params = {
            'q': name,
            'hl': 'en'
        }

        response = requests.get('https://www.google.com/search', params=params, headers=headers)
        html_text = response.content
        soup = str(BeautifulSoup(html_text, 'html.parser'))

        address_regex = '<span class="LrzXr">(.*?)<\/span>'
        phone_regex = '(?:<span aria-label[^>]+?)>([0-9()+\-\s]+)(?:<\/span>)'
        
        address_match = re.findall(address_regex, soup) 
        phone_match = re.findall(phone_regex, soup) 
        
        results.append((name, address_match, phone_match))
        
    # Merging input dataframe with new dataframe
    new_df = pd.DataFrame(results, columns=['name', 'address', 'phone number 2'])
    merged_df = pd.merge(df, new_df, on='name')
    
    # Combining the phone numbers retrieved from company site w/ the one on Google listing
    merged_df['phone number'] = merged_df['phone number'] + merged_df['phone number 2']
    merged_df = merged_df.drop("phone number 2", axis=1)
    merged_df['phone number'] = merged_df['phone number'].apply(clean_phone_number)
        
    return merged_df


def pipeline():
    """ Web Scraping pipeline
    """
    df = get_contact_info(music_services)
    df = add_company_name_column(df)

    save_path = os.path.join(script_dir, "../company_data/company_names.csv")
    df['name'].to_csv(save_path, index=False)

    save_path2 = os.path.join(script_dir, "../company_data/contact_info.csv")
    df = local_listing_query(df)
    df.to_csv(save_path2, index=False)

    return df


def main(args):
    try:
        df = pipeline()
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        
    else:
        print("Execution successful without any errors.")
        
    finally:
        print("Execution complete, performing cleanup.")

if __name__ == "__main__":
    main(sys.argv[1:])
