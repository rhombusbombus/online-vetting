#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reddit scraper script.
"""

import sys
import os
import re
import scrapy
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from urllib.parse import urlparse
from googlesearch import search

# Get absolute path of script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Import necessary data files
data_path = "../company_data/company_names.csv"
full_path = os.path.join(script_dir, data_path)
if os.path.exists(full_path):
    df = pd.read_csv(full_path)
    company_names = df[df.columns[0]].tolist()
else:
    raise ValueError("company_names.csv does not exist in the current directory.")


def get_name_variations(name):
    blacklist = ['one-submit']
    
    if name not in blacklist:
        variations = [name, name.replace('-', ' ')] # Handle variations of the company name
        if name == 'playlistpush':
            variations += ['playlist push']
        elif name == 'omarimc':
            variations += ['omari mc']
        elif name == 'starlightpr1':
            variations += ['starlight pr']
        elif name == 'planetarygroup':
            variations += ['planetary group']
        elif name == 'indiemusicacademy':
            variations += ['indie music academy']
        return list(set(variations)) # Remove duplicates
    else:
        return list(name)


def comment_scrape(n_stop):  
    counter = 0
    for company in company_names:
        data_path = f'../reddit_data/comments/{company}.csv'
        main_path = os.path.join(script_dir, data_path)

        if os.path.exists(data_path):
            df = pd.read_csv(data_path)
            bookmark = df['created_utc'].astype(int).sort_values(ascending=True).iloc[0]
        else:
            df = pd.DataFrame()
            bookmark = int(time.time())
            
        if (len(df) <= n_stop) and (len(df) != 0):        
            merge_df = pd.DataFrame()
            for name_variation in get_name_variations(company):
                request_url = f"https://api.pullpush.io/reddit/search/comment/?q={name_variation}&before={bookmark}"
                request_object = requests.get(request_url).json()
                new_df = pd.DataFrame.from_dict(request_object['data'])
                merge_df = pd.concat([merge_df, new_df], ignore_index=True, axis=0)
                
            if not merge_df.empty:
                result_df = pd.concat([df, merge_df], ignore_index=True, axis=0)
                result_df.to_csv(main_path, index=False)
                print(f'[IN-PROGRESS] {len(merge_df)} comments collected from {company}. {len(result_df)} collected in total.')
            else:
                print(f"[DONE] No new data for {company}. {len(df)} collected in total.")
                counter+=1
        else:
            print(f'[DONE] Max comments collected from {company}. {len(df)} collected in total.')
            counter+=1
    return counter


def iterate_comment_scrape(n_stop):
    counter = 0
    while counter < len(company_names):
        counter = comment_scrape(n_stop)
        # Wait for a random amount of time between 5 and 30 seconds
        wait_time = random.uniform(5, 30)
        time.sleep(wait_time)
    print("PROCESS DONE")

                
def submission_scrape(n_stop):  
    counter = 0
    for company in company_names:
        data_path = f'../reddit_data/submissions/{company}.csv'
        main_path = os.path.join(script_dir, data_path)

        if os.path.exists(main_path):
            df = pd.read_csv(main_path)
            bookmark = df['created_utc'].astype(int).sort_values(ascending=True).iloc[0]
        else:
            df = pd.DataFrame()
            bookmark = int(time.time())
            
        if (len(df) <= n_stop and (len(df) != 0)):        
            merge_df = pd.DataFrame()
            for name_variation in get_name_variations(company):
                request_url = f"https://api.pullpush.io/reddit/search/submission/?q={name_variation}&before={bookmark}&over_18=false&size=100"
                request_object = requests.get(request_url).json()
                new_df = pd.DataFrame.from_dict(request_object['data'])
                merge_df = pd.concat([merge_df, new_df], ignore_index=True, axis=0)
                
            if not merge_df.empty:
                result_df = pd.concat([df, merge_df], ignore_index=True, axis=0)
                result_df.to_csv(main_path, index=False)
                print(f'[IN-PROGRESS] {len(merge_df)} posts collected from {company}. {len(result_df)} collected in total.')
            else:
                print(f"[DONE] No new data for {company}. {len(df)} collected in total.")
                counter +=1
        else:
            print(f'[DONE] Max posts collected from {company}. {len(df)} collected in total.')
            counter +=1
    return counter


def iterate_submission_scrape(n_stop):
    counter = 0
    while counter < len(company_names):
        counter = submission_scrape(n_stop)
        # Wait for a random amount of time between 10 and 60 seconds
        wait_time = random.uniform(10, 60)
        time.sleep(wait_time)
    print("PROCESS DONE")


def main(args): 
    try:
        while True:
            try:
                choice = input("Enter <0> for comment data, <1> for post data and <2> for both.")
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
        print(f"Error: {e}", file=sys.stderr)
        
    else:
        print("Execution successful without any errors.")
        
    finally:
        print("Execution complete, performing cleanup.")

if __name__ == "__main__":
    main(sys.argv[1:])
