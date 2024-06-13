
"""
TrustPilot Review scraper script.

To run this script, create a new configuration file (.json) and then run the command
'python trustpilot_scraper.py <config path>'. If no path is provided, default settings will be used.

The extracted data is saved as CSV files at scraping/scraped_data/trustpilot_data.


Author: Joanna Lee
"""

import sys
import os
import re
import time
import json
import pandas as pd
import numpy as np
from random import randint
from utils import *

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Config:
    """Loads in configuration settings for TrustPilot scraping setup.
    
    Attributes:
        column_name (str): Default column name in CSV for names list.
        names_path (str): Path to the CSV file containing names to scrape.
        output_folder (str): Directory path for saving scraped data.
        n_pages (int): Number of pages to scrape per site.
    """
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            config = json.load(file)

        self.validate_config(config)

        self.names_path = os.path.join(parent_dir, config['names_path'])
        self.column_name = config['column_name']
        self.names_list = load_csv_list(self.names_path, self.column_name)
        self.output_folder = os.path.join(parent_dir, config['output_folder'])
        self.n_pages = config['n_pages']

    @staticmethod
    def validate_config(config):
        """Validates required fields in the configuration."""
        required_fields = ['names_path', 'column_name', 'output_folder', 'n_pages']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")


def extract_review_info(soup, company_name):
    """Extracts TrustPilot review data from a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): BeautifulSoup object containing HTML of the current page.

    Returns:
        DataFrame containing extracted review data.
    """
    review_cards = soup.find_all('div', class_="styles_reviewCardInner__EwDq2")
    dict = {}

    for card in review_cards:
        # Extract full name of user
        find_tag = card.find('span', class_="typography_heading-xxs__QKBS8 typography_appearance-default__AAY17", attrs={"data-consumer-name-typography": "true"})
        reviewer_name = find_tag.text if find_tag else ""

        # Extract users' total number of posts
        find_tag = card.find('span', class_="typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l", attrs={"data-consumer-reviews-count-typography": "true"})
        num_reviews = int(find_tag.text.split(' ')[0]) if find_tag else np.nan
    
        # Extract users' profile links
        find_tag = card.find('a', class_="link_internal__7XN06 link_wrapper__5ZJEx styles_consumerDetails__ZFieb", attrs={'name': "consumer-profile", 'data-consumer-profile-link':'true'})
        profile_link = ('https://www.trustpilot.com' + find_tag['href']) if find_tag else ""
        
        # Extract users' countries
        find_tag = card.find('div', class_="typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua")
        country = find_tag.text if find_tag else ""
        
        # Extract users' ratings
        find_tag = card.find('div', class_="styles_reviewHeader__iU9Px")
        star_rating = find_tag['data-service-review-rating'] if find_tag else np.nan
        
        # Extract review titles
        find_tag = card.find('h2', class_="typography_heading-s__f7029 typography_appearance-default__AAY17")
        review_title = find_tag.text if find_tag else ""
        
        # Extract review text bodies
        find_tag = card.find('p', class_="typography_body-l__KUYFJ typography_appearance-default__AAY17 typography_color-black__5LYEn")
        review_content = find_tag.text if find_tag else ""

        if (review_title != "") and (review_content != ""):
            review_content = review_title + ': ' + review_content
        elif (review_title != ""):
            review_content = review_title

        
        # Extract date of ratings
        find_tag = card.find('time', attrs={'data-service-review-date-time-ago':'true'})
        date_of_rating = find_tag['datetime'] if find_tag else np.nan
        #print(date_of_rating)
        
        # Extract date of experience
        find_tag = card.find('p', class_="typography_body-m__xgxZ_ typography_appearance-default__AAY17", attrs={'data-service-review-date-of-experience-typography':'true'})
        date_of_experience = find_tag.text.split(':', 1)[1].strip() if find_tag else np.nan
    
        # Extract direct links to reviews
        link_obj = card.find('a', class_="link_internal__7XN06 typography_appearance-default__AAY17 typography_color-inherit__TlgPO link_link__IZzHN link_notUnderlined__szqki")
        review_link = 'https://www.trustpilot.com' + link_obj['href']
        review_id = re.sub('/reviews/', '', link_obj['href'])

        
        metadata = {
            "ExperienceDate": date_of_experience,
            "AuthorName": reviewer_name,
            "AuthorCountry": country,
            "AuthorReviews": num_reviews,
            "ProfileLink": profile_link,
            "ReviewLink": review_link
        }
        attributes = [company_name, date_of_rating, review_content, star_rating, 'Trustpilot', metadata]
        dict[review_id] = attributes
    
    # Creating dataframe
    main_columns = ['CompanyName', 'ReviewDate', 'ReviewContent', 'StarRating', 'ReviewType', 'Metadata']
    df = pd.DataFrame.from_dict(dict, orient='index', columns=main_columns)

    # Reformatting/cleaning dataframe
    df = df[df["ReviewContent"] != '']
    df = df.dropna(subset=['ReviewContent'])
    df['ReviewDate'] = pd.to_datetime(df['ReviewDate'], utc=True).dt.tz_convert(None)

    return df
    

def collect_reviews(config):
    """Collects reviews based on the configuration provided.

    Args:
        config (Config): Configuration object containing scraping settings.

    Returns:
        DataFrame of all collected reviews.
    """
    all_reviews = pd.DataFrame()
        
    for site in config.names_list:
        company_name = extract_company_name(site)
        print(f"\nStarting scraping of {company_name}'s TrustPilot Reviews.")

        start_time = time.time()
        total_collected = 0
        output_filepath = os.path.join(config.output_folder, f"{company_name}.csv")
        
        if os.path.exists(output_filepath):
            df = pd.read_csv(output_filepath, index_col=0)
        else:
            df = pd.DataFrame()
            
        for curr_page in range(1, config.n_pages + 1):
            base_url = f'https://www.trustpilot.com/review/{extract_domain(site)}?page={curr_page}&sort=recency'
            soup = get_website(base_url)

            # Exit loop if page does not exist
            if soup.find('div', class_="errors_error404__tUqzU"):
                break
    
            new_reviews = extract_review_info(soup, company_name)

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
        print(f"Finished scraping {company_name}.")
        print(f"Collected {total_collected} new reviews.")
        print(f"Total time: {round(duration_requests, 2)} seconds.\n\n")
        all_reviews = pd.concat([all_reviews, df])
    return all_reviews


def main(config_file):
    try:
        config = Config(config_file)

        with spinner(title='In progress...'):
            collect_reviews(config)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        print("[✓] Execution complete.")


if __name__ == "__main__":
    # Default configuration file path
    default_config_path = os.path.join(parent_dir, "Python scripts", "trustpilot_scraper_config.json")
    
    # Check if the user has provided a custom config file
    if len(sys.argv) >= 2:
        config_file_path = sys.argv[1]
    else:
        print(f"No configuration file provided. Using default configuration: {default_config_path}")
        config_file_path = default_config_path

    main(config_file_path)
    
        