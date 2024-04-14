
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
from typing import Optional
from utils import *


# Get absolute path of script
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Default configuration
names_path = os.path.join(parent_dir, "scraped_data", "company_data", "music_services.csv")
names_list = load_csv_list(names_path, 'music_services')
comments_output_folder = os.path.join(parent_dir, "scraped_data", "reddit_data", "comments")
submissions_output_folder = os.path.join(parent_dir, "scraped_data", "reddit_data", "submissions")
fetch_newest = False


def comment_scrape(names_list: list, output_folder: str, n_stop: int = 1000, fetch_newest: bool = False) -> None: 
    """ Scrapes Reddit comments and saves them to a CSV file. See https://pullpush.io/#docs for
    more information about the metadata collected. Uses the '/reddit/search/comment' endpoint.

    Automatically picks up from where it left off previously. New data can be easily fetched
    without having to scrape the old data all over again.

    Args:
        names_list: Path to CSV file containing list of names to iterate through.
        output_folder: Folder where output CSVs will be saved.
        n_stop: Stop collecting more comments after n_stop comments have been collected in total.
        fetch_newest: If True, attempts to fetch all comments made since the time the script was last run.
        If False, attempts to fetch all comments made before the oldest comment currently in our data set
        (essentially the reverse direction in time).
    """
    print('\n') 
    print("############################")
    print("STARTING COMMENT SCRAPER\n")
    print("############################")
    print('\n')

    for name in names_list:
        output_path = os.path.join(output_folder, f"{name}.csv")

        if os.path.exists(output_path):
            df = pd.read_csv(output_path)
        else:
            df = pd.DataFrame()
            
        while (len(df) <= n_stop) and (n_stop != 0):        
            merge_df = pd.DataFrame()

            # Iterate thru each variation of the company's name
            for name_variation in get_name_variations(name):
                # Defining a pointer that keeps track of the oldest/newest post we've collected so far
                if (not df.empty) and (not df[df['search_term'] == name_variation].empty):
                    bookmark = df[df['search_term'] == name_variation]['created_utc'].astype(int)
                    if fetch_newest:
                        bookmark = bookmark.sort_values(ascending=False).iloc[0]
                    else:
                        bookmark = bookmark.sort_values(ascending=True).iloc[0]
                else:
                    bookmark = int(time.time())

                if fetch_newest:
                    request_url = f"https://api.pullpush.io/reddit/search/comment/?q={name_variation}&after={bookmark}&size=100"
                else:
                    request_url = f"https://api.pullpush.io/reddit/search/comment/?q={name_variation}&before={bookmark}&size=100"

                request_object = requests.get(request_url).json()
                time.sleep(randint(10, 30))  # Wait between 10-30 sec between each request
                new_df = pd.DataFrame.from_dict(request_object['data'])
                new_df['search_term'] = name_variation  # Keep track of the keyword used in the query
                merge_df = pd.concat([merge_df, new_df], ignore_index=True, axis=0)
                
            if not merge_df.empty:
                df = pd.concat([df, merge_df], ignore_index=True, axis=0)
                df = df.drop_duplicates(subset='id', keep="last")
                df.to_csv(output_path, index=False)
                print(f'[IN-PROGRESS] {len(merge_df)} comments collected from {name}. {len(df)} collected in total.')
            else:
                print(f"[✓] No new data for {name}. {len(df)} collected in total.")
                break
            
        print(f'[✓] Max comments collected from {name}. {len(df)} collected in total.')
        print('\n')
    return

                
def submission_scrape(names_list: list, output_folder: str, n_stop: int = 500, fetch_newest: bool = False) -> None: 
    """ Scrapes Reddit posts and saves them to a CSV file. See https://pullpush.io/#docs for
    more information about the metadata collected. Uses the '/reddit/search/submission' endpoint.

    Automatically picks up from where it left off previously. New data can be easily fetched
    without having to scrape the old data all over again.

    Args:
        names_list: Path to CSV file containing list of names to iterate through.
        output_folder: Folder where output CSVs will be saved.
        n_stop: Stop collecting more posts after n_stop posts have been collected in total.
        fetch_newest: If True, attempts to fetch all posts made since the time the script was last run.
        If False, attempts to fetch all posts made before the oldest post currently in our data set
        (essentially the reverse direction in time).
    """ 
    print('\n')
    print("############################")
    print("STARTING POST SCRAPER")
    print("############################")
    print('\n')
    for name in names_list:
        output_path = os.path.join(output_folder, f"{name}.csv")

        if os.path.exists(output_path):
            df = pd.read_csv(output_path)
        else:
            df = pd.DataFrame()
            
        while (len(df) <= n_stop and (n_stop != 0)):        
            merge_df = pd.DataFrame()
            for name_variation in get_name_variations(name):
                if (not df.empty) and (not df[df['search_term'] == name_variation].empty):
                    bookmark = df[df['search_term'] == name_variation]['created_utc'].astype(int)
                    if fetch_newest:
                        bookmark = bookmark.sort_values(ascending=False).iloc[0]
                    else:
                        bookmark = bookmark.sort_values(ascending=True).iloc[0]
                else:
                    bookmark = int(time.time())

                if fetch_newest:
                    request_url = f"https://api.pullpush.io/reddit/search/submission/?q={name_variation}&after={bookmark}&over_18=false&size=100"
                else:
                    request_url = f"https://api.pullpush.io/reddit/search/submission/?q={name_variation}&before={bookmark}&over_18=false&size=100"

                request_object = requests.get(request_url).json()
                time.sleep(randint(10, 30))
                new_df = pd.DataFrame.from_dict(request_object['data'])
                new_df['search_term'] = name_variation
                merge_df = pd.concat([merge_df, new_df], ignore_index=True, axis=0)
                
            if not merge_df.empty:
                df = pd.concat([df, merge_df], ignore_index=True, axis=0)
                df = df.drop_duplicates(subset='id', keep="last")
                df.to_csv(output_path, index=False)
                print(f'[IN-PROGRESS] {len(merge_df)} posts collected from {name}. {len(df)} collected in total.')
            else:
                print(f"[✓] No new data for {name}. {len(df)} collected in total.")
                break

        print(f'[✓] Max posts collected from {name}. {len(df)} collected in total.')
        print('\n')
    return


def main(args): 
    try:
        while True:
            try:
                choice = input("Enter 0 for comment data, 1 for post data, or 2 for both: ")
                choice = int(choice)
                if choice not in [0, 1, 2]:
                    raise ValueError("Please enter 0, 1, or 2.")
                break
            except ValueError as e:
                print(f"{e} Please try again.")

        while True:
            try:
                use_defaults = input("Enter 0 to proceed with default configuration or 1 to customize configuration: ")
                use_defaults = int(use_defaults)
                if use_defaults not in [0, 1]:
                    raise ValueError("Please enter 0 or 1.")
                break
            except ValueError as e:
                print(f"{e} Please try again.")

        while use_defaults == 1:
            while True:
                try:
                    n_stop = input("Max number of posts/comments to collect: ")
                    n_stop = int(n_stop)
                    break
                except ValueError as e:
                    print(f"{e} Please try again.")
            while True:
                try:
                    names_path = input("Enter path of names list (.CSV): ")
                    names_path = str(names_path)
                    break
                except ValueError as e:
                    print(f"{e} Please try again.")
            while True:
                try:
                    col_name = input("Enter name of column that will be extracted: ")
                    col_name = str(col_name)
                    names_list = load_csv_list(names_path, col_name)
                    break
                except ValueError as e:
                    print(f"{e} Please try again.")
            while choice == 0 or choice == 2:
                try:
                    comments_output_folder = input("Desired output folder for comment data: ")
                    comments_output_folder = str(comments_output_folder)
                    break
                except ValueError as e:
                    print(f"{e} Please try again.")
            while choice == 1 or choice == 2:
                try:
                    submissions_output_folder = input("Desired output folder for post data: ")
                    submissions_output_folder = str(submissions_output_folder)
                    break
                except ValueError as e:
                    print(f"{e} Please try again.")
            while True:
                try:
                    fetch_choice = input("Enter 0 to only fetch newest data, otherwise enter 1: ")
                    fetch_choice = int(fetch_choice)
                    if fetch_choice not in [0, 1]:
                        raise ValueError("Please enter 0 or 1.")
                    if fetch_choice == 0:
                        fetch_newest = True
                    else:
                        fetch_newest = False
                    break
                except ValueError as e:
                    print(f"{e} Please try again.")
            break

        with spinner(title='In progress...'):
            if (choice == 0):
                comment_scrape(names_list=names_list, output_folder=comments_output_folder, n_stop=n_stop, fetch_newest=fetch_newest)
            elif (choice == 1):
                submission_scrape(names_list=names_list, output_folder=submissions_output_folder, n_stop=n_stop, fetch_newest=fetch_newest)
            elif (choice == 2):
                comment_scrape(names_list=names_list, output_folder=comments_output_folder, n_stop=n_stop, fetch_newest=fetch_newest)
                submission_scrape(names_list=names_list, output_folder=submissions_output_folder, n_stop=n_stop, fetch_newest=fetch_newest)
            else:
                raise ValueError("Missing music.services.csv")
        
    except Exception as e:
        if str(e) == "Expecting value: line 1 column 1 (char 0)":
            print("ERROR: Server request was blocked due to a high frequency of requests made within a short period of time. Try again in a few minutes or edit the waiting logic to avoid overloading the server.", file=sys.stderr)
        else:
            print(f"Error: {e}", file=sys.stderr)
        
    else:
        print("[✓] Execution successful without any errors.")
        
    finally:
        print("[✓] Execution complete, performing cleanup.")


if __name__ == "__main__":
    main(sys.argv[1:])
