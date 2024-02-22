
"""
Reddit scraper script.
"""

import sys
import os
import time
import requests
import pandas as pd
from random import randint
from alive_progress import alive_bar
from typing import ContextManager, Optional
from utils import *


# Get absolute path of script
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Import necessary data files
filepath = os.path.join(parent_dir, "scraped_data", "company_data", "music_services.csv")
if os.path.exists(filepath):
    music_services = pd.read_csv(filepath)
    music_services = music_services['music_services'].tolist()
else:
    raise ValueError("Missing file: music_services.csv")

# Extract company names from urls
company_names = extract_company_name_batch(music_services)


def spinner(title: Optional[str] = None) -> ContextManager:
    """ Context manager to display a spinner while a long-running process is running.

    Usage:
        with spinner("Fetching data..."):
            fetch_data()

    Args:
        title: The title of the spinner. If None, no title will be displayed.
    """
    return alive_bar(monitor=None, stats=None, title=title, elapsed=False, 
                     bar=None, spinner='classic', enrich_print=False)


def comment_scrape(n_stop): 
    print('\n') 
    print("############################")
    print("STARTING COMMENT SCRAPER\n")
    print("############################")
    print('\n')

    for company in company_names:
        data_path = os.path.join(parent_dir, "scraped_data", "reddit_data", "comments", f"{company}.csv")

        if os.path.exists(data_path):
            df = pd.read_csv(data_path)
        else:
            df = pd.DataFrame()
            
        # Keep collecting posts until we collected n_stop total posts
        while (len(df) <= n_stop) and (n_stop != 0):        
            merge_df = pd.DataFrame()

            # Iterate thru each variation of the company's name to capture as many posts as possible
            for name_variation in get_name_variations(company):
                # Defining 'bookmark', a pointer that keeps track of the oldest post we've collected so far
                if (not df.empty) and (not df[df['search_term'] == name_variation].empty):
                    bookmark = df[df['search_term'] == name_variation]['created_utc'].astype(int).sort_values(ascending=True).iloc[0]
                else:
                    bookmark = int(time.time())

                request_url = f"https://api.pullpush.io/reddit/search/comment/?q={name_variation}&before={bookmark}&size=100"
                request_object = requests.get(request_url).json()
                time.sleep(randint(10, 30))  # Wait between 10-30 sec between each request
                new_df = pd.DataFrame.from_dict(request_object['data'])
                new_df['search_term'] = name_variation  # Keep track of the keyword used in the query
                merge_df = pd.concat([merge_df, new_df], ignore_index=True, axis=0)
                
            if not merge_df.empty:
                df = pd.concat([df, merge_df], ignore_index=True, axis=0)
                df = df.drop_duplicates(subset='id', keep="last")
                df.to_csv(data_path, index=False)
                print(f'[IN-PROGRESS] {len(merge_df)} comments collected from {company}. {len(df)} collected in total.')
            else:
                print(f"[✓] No new data for {company}. {len(df)} collected in total.")
                break
            
        print(f'[✓] Max comments collected from {company}. {len(df)} collected in total.')
        print('\n')
    return

                
def submission_scrape(n_stop):  
    print('\n')
    print("############################")
    print("STARTING POST SCRAPER")
    print("############################")
    print('\n')
    for company in company_names:
        data_path = os.path.join(parent_dir, "scraped_data", "reddit_data", "submissions", f"{company}.csv")

        if os.path.exists(data_path):
            df = pd.read_csv(data_path)
        else:
            df = pd.DataFrame()
            
        while (len(df) <= n_stop and (n_stop != 0)):        
            merge_df = pd.DataFrame()
            for name_variation in get_name_variations(company):
                if (not df.empty) and (not df[df['search_term'] == name_variation].empty):
                    bookmark = df[df['search_term'] == name_variation]['created_utc'].astype(int).sort_values(ascending=True).iloc[0]
                else:
                    bookmark = int(time.time())
                request_url = f"https://api.pullpush.io/reddit/search/submission/?q={name_variation}&before={bookmark}&over_18=false&size=100"
                request_object = requests.get(request_url).json()
                time.sleep(randint(10, 30))
                new_df = pd.DataFrame.from_dict(request_object['data'])
                new_df['search_term'] = name_variation
                merge_df = pd.concat([merge_df, new_df], ignore_index=True, axis=0)
                
            if not merge_df.empty:
                df = pd.concat([df, merge_df], ignore_index=True, axis=0)
                df = df.drop_duplicates(subset='id', keep="last")
                df.to_csv(data_path, index=False)
                print(f'[IN-PROGRESS] {len(merge_df)} posts collected from {company}. {len(df)} collected in total.')
            else:
                print(f"[✓] No new data for {company}. {len(df)} collected in total.")
                break

        print(f'[✓] Max posts collected from {company}. {len(df)} collected in total.')
        print('\n')
    return


def main(args): 
    try:
        while True:
            try:
                choice = input("Enter <0> for comment data, <1> for post data and <2> for both: ")
                choice = int(choice)
                break
            except ValueError:
                print("That's not an integer. Please try again.")

        while True:
            try:
                n_stop = input("Max number of posts/comments to collect: ")
                n_stop = int(n_stop)
                break
            except ValueError:
                print("That's not an integer. Please try again.")

        with spinner(title='In progress...'):
            if (choice == 0):
                comment_scrape(n_stop)
            elif (choice == 1):
                submission_scrape(n_stop)
            elif (choice == 2):
                comment_scrape(n_stop)
                submission_scrape(n_stop)
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
