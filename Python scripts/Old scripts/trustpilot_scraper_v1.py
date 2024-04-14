
"""
TrustPilot Review scraper script.

To run this script, simply run 'trustpilot_scraper.py' and respond to the prompts.

The extracted data is saved as CSV files at scraping/scraped_data/trustpilot_data.


Author: Joanna Lee
"""

import sys
import os
import re
import time
import pandas as pd
import numpy as np
from random import randint
from utils import *


# Get absolute path of script
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Import necessary data files
filepath = os.path.join(parent_dir, "scraped_data", "company_data", "music_services.csv")
if os.path.exists(filepath):
    music_services = pd.read_csv(filepath)
    music_services = music_services['music_services'].tolist()
else:
    raise ValueError("Missing music_services.csv")


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
    

def collect_reviews(website_list, pages_to_scrape):
    all_reviews = pd.DataFrame()
        
    for site in website_list:
        start_time = time.time()
        total_collected = 0
        company_name = extract_company_name(site)
        print(f"\nStarting scraping of {company_name}'s TrustPilot Reviews.")
        filepath = os.path.join(parent_dir, "scraped_data", "trustpilot_data", "reviews", f"{company_name}.csv")
        
        if os.path.exists(filepath):
            df = pd.read_csv(filepath, index_col=0)  
        else:
            df = pd.DataFrame()
            
        for curr_page in range(1, pages_to_scrape):
            domain = extract_domain(site)
            base_url = f'https://www.trustpilot.com/review/{domain}?page={curr_page}&sort=recency'
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
            df.to_csv(filepath)
            time.sleep(randint(2, 5))
                
        end_time = time.time()
        duration_requests = end_time - start_time
        print(f"Finished scraping {company_name}.")
        print(f"Collected {total_collected} new reviews.")
        print(f"Total time: {round(duration_requests, 2)} seconds.\n\n")
        all_reviews = pd.concat([all_reviews, df])
    return all_reviews


def main():
    with spinner(title='In progress...'):
        collect_reviews(music_services, 200)


if __name__ == "__main__":
    try:
        main()
        print("[✓] Execution successful without any errors.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        print("[✓] Execution complete, performing cleanup.")
        