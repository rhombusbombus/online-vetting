## General information:

This repo contains most of the files and data sets for the online vetting project. Included are web scraping scripts written in Python and data sets formatted as .csv files.

Assume that the data sets have not been cleaned (unless they've been labelled as such). All data sets are up-to-date and are ready to be used for analysis. The python scripts do not need to be run unless you're trying to collect newer data or if you want to add other companies/websites.

I'm in the process of uploading more data sets that I've compiled/collected but will need to compress them to save storage space.

---

## Directory structure:

- scraping\
    - GPT_generated_data\
    - scraped_data\
        - company_data\
        - reddit_data\
        - trustpilot_data\
    - python_scripts\

---

## How to run Python scripts:

1. Clone the entire repo to your computer. The scripts depend on a specific file structure in order to run properly and if anything is out of place, it might fail to run. If you want to save space, feel free to delete any of the .csv files that are not in the ```\company_data``` folder (do not delete the folders!!)
2. Navigate to the ```\python_scripts``` folder and install the necessary libraries from the ```requirements.txt``` file by running:
    ```
    $ pip install -r requirements.txt
    ```
3. As an example, run any of the scripts by calling
    ```
    python reddit_scraper.py
    ```