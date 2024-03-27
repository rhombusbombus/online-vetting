
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

To run this script, simply run 'reddit_scraper.py' and respond to the prompts.

The extracted data is saved as CSV files at scraping/scraped_data/reddit_data.


Author: Joanna Lee
"""


import sys
import os
import time
import requests
import pandas as pd
from random import randint
from typing import List
from utils import *


class Config:
    def __init__(self, args):
        self.parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.names_path = os.path.join(self.parent_dir, "scraped_data", "company_data", "music_services.csv")
        self.comments_output_folder = os.path.join(self.parent_dir, "scraped_data", "reddit_data", "comments")
        self.submissions_output_folder = os.path.join(self.parent_dir, "scraped_data", "reddit_data", "submissions")
        self.fetch_newest = False
        self.n_stop = 1000 
        self.column_name = 'music_services'
        self.names_list = load_csv_list(self.names_path, 'music_services')

        self.choice = None


    def set_choice(self, choice: int):
        self.choice = choice

    def update_list_path(self, names_path: str, column_name: str):
        self.names_list = load_csv_list(names_path, column_name)

    def update_comments_path(self, comments_folder: str):
        self.comments_output_folder = comments_folder

    def update_submissions_path(self, submissions_folder: str):
        self.submissions_output_folder = submissions_folder

    def update_fetch_newest(self, fetch_newest: bool):
        self.fetch_newest = fetch_newest

    def update_n_stop(self, n_stop: int):
        self.n_stop = n_stop


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
    return bookmark, request_url


def get_user_input(prompt, valid_choices, error_message):
    """
    Prompts the user for input and validates against a set of valid choices.

    Args:
        prompt (str): The prompt message displayed to the user.
        valid_choices (list): A list of valid input choices.
        error_message (str): The message displayed for an invalid input.

    Returns:
        The validated user input.
    """
    while True:
        try:
            user_input = int(input(prompt))
            if user_input not in valid_choices:
                raise ValueError(error_message)
            return user_input
        except ValueError as e:
            print(f"{e} Please try again.")


def main(args):
    try:
        config = Config(args)

        choice = get_user_input(prompt="Enter 0 for comment data, 1 for post data, or 2 for both: ", 
                                valid_choices=[0, 1, 2], 
                                error_message="Please enter 0, 1, or 2.")
        config.set_choice(choice)

        use_defaults = get_user_input(prompt="Enter 0 to proceed with default configuration or 1 to customize configuration: ", 
                                      valid_choices=[0, 1], 
                                      error_message="Please enter 0 or 1.")

        if use_defaults == 1:
            n_stop = get_user_input(prompt="Max number of posts/comments to collect: ", 
                                    valid_choices=range(1, 100000), 
                                    error_message="Please enter a valid number.")
            config.update_n_stop(n_stop)

            names_path = input("Enter path of names list (.CSV): ")
            column_name = input("Enter name of column that will be extracted: ")
            config.update_list_path(names_path, column_name)

            if config.choice in [0, 2]:
                comments_output_folder = input("Desired output folder for comment data: ")
                config.update_comments_path(comments_output_folder)
            
            if config.choice in [1, 2]:
                submissions_output_folder = input("Desired output folder for post data: ")
                config.update_submissions_path(submissions_output_folder)

            fetch_choice = get_user_input(prompt="Enter 0 to only fetch newest data, otherwise enter 1: ", 
                                          valid_choices=[0, 1], 
                                          error_message="Please enter 0 or 1.")
            fetch_newest = True if fetch_choice == 0 else False
            config.update_fetch_newest(fetch_newest)

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
    main(sys.argv[1:])