# Scraped data

## General info:
- Will eventually be moved to an SQL database and removed from GitHub
- This folder contains all of the scraped data for both online services and live music venues.
- Some of the folders are further organized into ```clean```, ```original```, and ```train``` folders. The ```original``` folder contains all of the raw/unmodified data. The ```clean``` folder contains data sets with most of the irrelevant data removed/redacted. Ignore the ```train``` folder for now.

---

## Folder structure:
- ```company_data```: contains business contact information and various other business-related data
- ```google_business_reviews```: Google business review data (for live music venues only)
- ```merged```: All reviews merged together into a single file for use with Azure AI studio
- ```reddit_data```: Reddit comments and posts data (for online services only)
- ```trustpilot_data```: TrustPilot reviews (for online services only)
- ```yelp_data```: Yelp reviews (for live music venues only)