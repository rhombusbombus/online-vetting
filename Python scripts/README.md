# Python scripts

## General info:
- This folder contains most of the scraping scripts. Included are scrapers specifically written for Reddit, TrustPilot, and Yelp. A 3rd party scraper was used to collect the Google business reviews because those were pretty difficult to extract without an API.
- The scripts are constantly improved/updated so there is a small chance something might break that previously worked before (lol). These are usually small mistakes such as different file paths or filenames (which also change constantly) so feel free to directly edit the ```.py``` files as needed.
- The scripts read input parameters from a configuration file instead of taking in user-inputted arguments. If the configuration files are missing or renamed, the scripts might not work.

---

## How to run Python scripts:

1. Clone the entire repo to your computer. The scripts depend on a specific file structure in order to run properly and if anything is out of place, it might fail to run. If you want to save space, feel free to delete any of the .csv files that are not in the ```Scraped data\company_data``` folder (do not delete the folders!!)
2. Open Terminal/CMD/shell, navigate to the ```\Python scripts``` folder, and install the necessary libraries from the ```requirements.txt``` file by running the following command (you must have Python installed in order for this to work):
    ```
    $ pip install -r requirements.txt
    ```
3. Edit the values in the configuration file as necessary. The configuration files should be named similarly to the main scripts but with a ```.json``` file extension instead. For example, if you wanted to run ```reddit_scraper.py```, you would need to edit ```reddit_scraper_config.json```.
4. Run the script by calling
    ```
    python reddit_scraper.py
    ```