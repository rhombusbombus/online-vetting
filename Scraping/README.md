## There are three folders: 
- **python_files:** This folder contains all of the scraping scripts written in python. You don't need to run any of them unless you want to collect more data (Note: The scripts won't run if music_services.csv is missing. If you want to add more websites to the list, make sure to edit this file). 

- **company_data:** Contains all company data sets. All the files (except for music_services.csv) are automatically updated whenever the script is run. If you want to add more company websites, edit music_services.csv, save, and then run contact_info_scraper.py again to update the other data sets. Pls don't edit any files in here and make copies of the data sets as needed.

- **reddit_data:** Contains all of the reddit data collected by the scraping tool. It is organized into two folders: "comments" and "submissions". All the files in this folder are automatically updated when the reddit script is run. Pls don't edit any files in here and make copies of the data sets as needed.

## How to run the scripts:
1. Simply run "python contact_info_scraper.py" or "python reddit_scraper.py" and answer the questions if prompted. 
2. The reddit scraper will first ask if you want to scrape comments only, posts only, or both. It'll also ask you how many comments/posts in total that you want to scrape. 
    - Currently, I have collected the past 1000 most recent comments/posts for each company. Most of the companies on the list have far fewer than 1000 comments/posts on Reddit so there probably won't be anything new to scrape yet. 
    - Be careful when running the reddit scraper, your server might timeout from making too many requests at once! Either wait like 15-20 min before running it again, edit the code and increase the wait time, or install the "scrapy-useragents" python package.
3. If you get any errors:
    1. Check the file/folder structure and ensure that nothing is missing or out of place. The script depends on absolute paths so it might break if a file is missing. If you're not sure, redownload the whole thing again.
    2. Make sure all the required packages are installed (check the requirements.txt file). 
    3. It's probably a timeout error. If you get an error that an object is empty or None, that's also due to a timeout error...

## Still need to do:
- Implement logic such that the scripts will run weekly/bi-weekly to check for any new posts/comments.
- Find a way to circumvent the timeout errors by either improving the waiting logic or by using a proxy-switching service (not free...).