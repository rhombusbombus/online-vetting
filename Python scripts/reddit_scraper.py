
"""
This script is designed to scrape all Reddit posts and comments that mention a company name
listed in the 'music_services.csv' file. 

The Reddit content is queried using the PullPush API (https://pullpush.io/#docs), which is a 
database of archived Reddit posts (most of which were originally collected by Pushshift before 
Reddit shut down public access to its official API in July 2023). Data after July 2023 is 
manually scraped by community members each month and only contains data from the top 1000 
most popular subreddits. Thus, data from smaller subreddits must be manually scraped. 

Be extremely careful of rate-limiting (depending on how much data it retrieves, you can get 
blocked after making just 1 request!! In that case, your only option is to wait it out...)

To run this script, create a new configuration file (.json) and then run the command
'python reddit_scraper.py <config path>'. If no path is provided, default settings will be used.

The extracted data is saved as CSV files at scraping/scraped_data/reddit_data.


Author: Joanna Lee
"""


import sys
import os
import time
import json
import requests
import pandas as pd
from random import randint
from typing import List
from utils import *

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Config:
    """Loads in configuration settings for Reddit scraping setup.
    
    Attributes:
        column_name (str): Default column name in CSV for names list.
        names_path (str): Path to the CSV file containing names to scrape.
        comments_output_folder (str): Directory path for saving scraped comment data.
        submissions_output_folder (str): Directory path for saving scraped post data.
        fetch_newest (bool): Indicate which direction in time to start scraping.
        n_stop (int): Max number of comments/posts to scrape per site.
        choice (int): 0 - Comments only, 1 - Posts only, 2 - Both comments and posts
    """
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            config = json.load(file)

        self.validate_config(config)

        self.names_path = os.path.join(parent_dir, config['names_path'])
        self.column_name = config['column_name']
        self.names_list = extract_company_name_batch(load_csv_list(self.names_path, self.column_name))
        self.comments_output_folder = os.path.join(parent_dir, config['comments_output_folder'])
        self.submissions_output_folder = os.path.join(parent_dir, config['submissions_output_folder'])
        self.fetch_newest = config['fetch_newest']
        self.n_stop = config['n_stop']
        self.choice = config['choice']

    @staticmethod
    def validate_config(config):
        """Validates required fields in the configuration."""
        required_fields = ['names_path', 'column_name', 'comments_output_folder', 'submissions_output_folder', 'fetch_newest', 'n_stop', 'choice']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")


def scrape_data(names_list: List[str], output_folder: str, n_stop: int, fetch_newest: bool, data_type: str):
    """ 
    Scrapes Reddit comments and saves them to a CSV file. See https://pullpush.io/#docs for
    more information about the metadata collected.

    Automatically picks up from where it left off previously. New data can be easily fetched
    without having to scrape the old data all over again.

    Args:
        names_list: List of names to iterate through.
        output_folder: Folder where output CSVs will be saved.
        n_stop: Stop collecting more comments after n_stop comments have been collected in total.
        fetch_newest: If True, attempts to fetch all comments made since the time the script was last run.
        If False, attempts to fetch all comments made before the oldest comment currently in our data set
        (essentially the reverse direction in time).
        data_type: Indicates whether the data is comments or posts.
    """
    print(f"\n############################\nSTARTING {data_type.upper()} SCRAPER\n############################\n")
    for name in names_list:
        output_path = os.path.join(output_folder, f"{name}.csv")

        if os.path.exists(output_path):
            df = pd.read_csv(output_path)
        else:
            df = pd.DataFrame()
        
        while len(df) <= n_stop and n_stop != 0:        
            merge_df = pd.DataFrame()
            for name_variation in get_name_variations(name):
                request_url = prepare_request(df, name_variation, fetch_newest, data_type)
                request_object = requests.get(request_url).json()
                time.sleep(randint(10, 30))  # Wait between 10-30 sec between each request
                new_df = pd.DataFrame.from_dict(request_object['data'])
                new_df['search_term'] = name_variation
                merge_df = pd.concat([merge_df, new_df], ignore_index=True)
                
            if not merge_df.empty:
                df = pd.concat([df, merge_df], ignore_index=True).drop_duplicates(subset='id', keep="last")
                df.to_csv(output_path, index=False)
                print(f'[IN-PROGRESS] {len(merge_df)} {data_type} collected from {name}. {len(df)} collected in total.')
            else:
                print(f"[✓] No new data for {name}. {len(df)} collected in total.")
                break

        print(f'[✓] Max {data_type} collected from {name}. {len(df)} collected in total.\n')
    return


def prepare_request(df, name_variation, fetch_newest, data_type):
    if df.empty or df[df['search_term'] == name_variation].empty:
        bookmark = int(time.time())
    else:
        bookmark = df[df['search_term'] == name_variation]['created_utc'].astype(int)
        if fetch_newest:
            bookmark = bookmark.sort_values(ascending=False).iloc[0]
        else:
            bookmark = bookmark.sort_values(ascending=True).iloc[0]

    endpoint = "comment" if data_type == "comments" else "submission"
    direction = "after" if fetch_newest else "before"
    request_url = f"https://api.pullpush.io/reddit/search/{endpoint}/?q={name_variation}&{direction}={bookmark}&size=100"
    return request_url


def main(config_file):
    try:
        config = Config(config_file)

        with spinner(title='In progress...'):
            if config.choice in [0, 2]:
                scrape_data(config.names_list, config.comments_output_folder, config.n_stop, config.fetch_newest, "comments")
            if config.choice in [1, 2]:
                scrape_data(config.names_list, config.submissions_output_folder, config.n_stop, config.fetch_newest, "posts")

    except Exception as e:
        if str(e) == "Expecting value: line 1 column 1 (char 0)":
            print("ERROR: Server request was blocked due to a high frequency of requests made within a short period of time. Try again in a few minutes or edit the waiting logic to avoid overloading the server.", file=sys.stderr)
        else:
            print(f"Error: {e}", file=sys.stderr)
        
    finally:
        print("[✓] Execution complete, performing cleanup.")


if __name__ == "__main__":
    # Default configuration file path
    default_config_path = os.path.join(parent_dir, "Python scripts", "reddit_scraper_config.json")
    
    # Check if the user has provided a custom config file
    if len(sys.argv) >= 2:
        config_file_path = sys.argv[1]
    else:
        print(f"No configuration file provided. Using default configuration: {default_config_path}")
        print()
        config_file_path = default_config_path
    
    main(config_file_path)