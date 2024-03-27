
"""
TrustPilot Review scraper script.
"""

import sys
import os
import re
import time
import pandas as pd
import numpy as np
from random import randint
import requests
from bs4 import BeautifulSoup
from alive_progress import alive_bar
from typing import ContextManager, Optional
from utils import *


# Get absolute path of script
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Total: 376+163+338+297+330+264+3438
# mostly real: musicvertising, dittomusic
# mostly fake: everyone else
#company_list = ["yourownmusic.co.uk", "topmusicmarketingagency.com", "mimexpress.com", "repostexchange.com", "festingervault.com", "musicvertising.com", "www.dittomusic.com"]
company_list = ["www.fortunecoins.com"]


headers = {
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


def spinner(title: Optional[str] = None) -> ContextManager:
    """
    Context manager to display a spinner while a long-running process is running.

    Usage:
        with spinner("Fetching data..."):
            fetch_data()

    Args:
        title: The title of the spinner. If None, no title will be displayed.
    """
    return alive_bar(monitor=None, stats=None, title=title, elapsed=False, 
                     bar=None, spinner='classic', enrich_print=False)


def get_website(url):
    """Fetches HTML content of site and returns it as a BeautifulSoup object.
    """
    response = requests.get(url, headers=headers)
    html_text = response.content
    soup = BeautifulSoup(html_text, 'lxml')
    return soup


def extract_review_info(soup):
    """Returns review attributes extracted from HTML tags.
    """
    review_cards = soup.find_all('div', class_="styles_reviewCardInner__EwDq2")
    dict = {}

    for card in review_cards:
        # Extract full name of user
        find_tag = card.find('span', class_="typography_heading-xxs__QKBS8 typography_appearance-default__AAY17", attrs={"data-consumer-name-typography": "true"})
        reviewer_name = find_tag.text if find_tag else np.nan

        # Extract users' total number of posts
        find_tag = card.find('span', class_="typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l", attrs={"data-consumer-reviews-count-typography": "true"})
        num_reviews = int(find_tag.text.split(' ')[0]) if find_tag else np.nan
    
        # Extract users' profile links
        find_tag = card.find('a', class_="link_internal__7XN06 link_wrapper__5ZJEx styles_consumerDetails__ZFieb", attrs={'name': "consumer-profile", 'data-consumer-profile-link':'true'})
        profile_link = ('https://www.trustpilot.com' + find_tag['href']) if find_tag else np.nan
        
        # Extract users' countries
        find_tag = card.find('div', class_="typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua")
        country = find_tag.text if find_tag else np.nan
        
        # Extract users' ratings
        find_tag = card.find('div', class_="styles_reviewHeader__iU9Px")
        star_rating = find_tag['data-service-review-rating'] if find_tag else np.nan
        
        # Extract review titles
        find_tag = card.find('h2', class_="typography_heading-s__f7029 typography_appearance-default__AAY17")
        review_title = find_tag.text if find_tag else np.nan
        
        # Extract review text bodies
        find_tag = card.find('p', class_="typography_body-l__KUYFJ typography_appearance-default__AAY17 typography_color-black__5LYEn")
        review_content = find_tag.text if find_tag else np.nan
        
        # Extract date of ratings
        find_tag = card.find('time', attrs={'data-service-review-date-time-ago':'true'})
        date_of_rating = find_tag['datetime'] if find_tag else np.nan
        
        # Extract date of experience
        find_tag = card.find('p', class_="typography_body-m__xgxZ_ typography_appearance-default__AAY17", attrs={'data-service-review-date-of-experience-typography':'true'})
        date_of_experience = find_tag.text.split(':', 1)[1].strip() if find_tag else np.nan
    
        # Extract direct links to reviews
        link_obj = card.find('a', class_="link_internal__7XN06 typography_appearance-default__AAY17 typography_color-inherit__TlgPO link_link__IZzHN link_notUnderlined__szqki")
        review_link = 'https://www.trustpilot.com' + link_obj['href']
        review_id = re.sub('/reviews/', '', link_obj['href'])

        attributes = [star_rating, review_title, review_content, date_of_rating, date_of_experience, reviewer_name, num_reviews, country, review_link, profile_link]
        dict[review_id] = attributes
        cols = ['star_rating', 'review_title', 'review_content', 'date_of_rating', 'date_of_experience', 'reviewer_name', 'num_reviews', 'country', 'review_link', 'profile_link']

    return pd.DataFrame.from_dict(dict, orient='index', columns=cols)
    

def collect_reviews(company_list, pages_to_scrape):
    all_reviews = pd.DataFrame()
        
    for company in company_list:
        start_time = time.time()
        total_collected = 0
        print('\n')
        print(f"Starting scraping of {company}'s TrustPilot Reviews.")
        
        df = pd.DataFrame()
            
        for curr_page in range(1, pages_to_scrape):
            base_url = f'https://www.trustpilot.com/review/{company}?page={curr_page}&sort=recency'
            output_filepath = os.path.join(parent_dir, "scraped_data", "trustpilot_data", "reviews", "fake_reviews", f"{company}.csv")
            soup = get_website(base_url)

            # Checking if page exists or if we land on a 404 page
            if soup.find('div', class_="errors_error404__tUqzU"):
                break
    
            new_reviews = extract_review_info(soup)

            # Check if any review in new_reviews is already in all_reviews
            if df.empty:
                df = new_reviews
            elif not df.empty and not new_reviews[~new_reviews.index.isin(df.index)].empty:
                new_reviews = new_reviews[~new_reviews.index.isin(df.index)]
                df = pd.concat([df, new_reviews])
            else:
                # If no new reviews, stop fetching more pages
                time.sleep(randint(2, 5))
                break

            total_collected += len(new_reviews)
            df.to_csv(output_filepath)
            time.sleep(randint(2, 5))
                
        end_time = time.time()
        duration_requests = end_time - start_time
        print(f"Finished scraping {company}.")
        print(f"Collected {total_collected} new reviews.")
        print(f"Total time: {round(duration_requests, 2)} seconds.\n")
        print('\n')  
        all_reviews = pd.concat([all_reviews, df])
    return all_reviews


def main():
    with spinner(title='In progress...'):
        collect_reviews(company_list, 150)


if __name__ == "__main__":
    try:
        main()
        print("[✓] Execution successful without any errors.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        print("[✓] Execution complete, performing cleanup.")
        