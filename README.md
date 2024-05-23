# A fork of [steam scraper](https://github.com/prncc/steam-scraper/)

## Description

This is a fork of [steam scraper](https://github.com/prncc/steam-scraper/). Key differences:
- Updated to later version
- Output is now stored in SQLite database
- Additional fields are being scraped (ie. description)
- Added script for fetching News from API
- Added script for minimizing SQLite dataset


# Steam Scraper

This repository contains [Scrapy](https://github.com/scrapy/scrapy) spiders for **crawling products** and **scraping all user-submitted reviews** from the [Steam game store](https://steampowered.com).
A few scripts for more easily managing and deploying the spiders are included as well.

This repository contains code accompanying the *Scraping the Steam Game Store* article published on the [Scrapinghub blog](https://blog.scrapinghub.com/2017/07/07/scraping-the-steam-game-store-with-scrapy/) and the [Intoli blog](https://intoli.com/blog/steam-scraper/).

## Installation

After cloning the repository with
```bash
git clone git@github.com:lkrsnik/steam-scraper.git
```
start and activate a Python 3.6+ virtualenv with
```bash
cd steam-scraper
virtualenv -p python3 venv
. venv/bin/activate
```
Install Python requirements via:
```bash
pip install -r requirements.txt
```

By the way, on macOS you can install Python 3.6 via [homebrew](https://brew.sh):
 ```bash
 brew install python3
```
On Ubuntu you can use [instructions posted on askubuntu.com](https://askubuntu.com/questions/865554/how-do-i-install-python-3-6-using-apt-get).

## Crawling the Products

The purpose of `ProductSpider` is to discover product pages on the [Steam product listing](http://store.steampowered.com/search/?sort_by=Released_DESC) and extract useful metadata from them.
A neat feature of this spider is that it automatically navigates through Steam's age verification checkpoints.
You can initiate the multi-hour crawl with
```bash
mkdir output
scrapy crawl products --logfile=output/products_all.log --loglevel=INFO -s JOBDIR=output/products_all_job -s HTTPCACHE_ENABLED=False -a sqlite_path=output/db.sqlite3
```
When it completes you should have metadata for all games (products) on Steam stored in db.sqlite3. 

## Extracting the Reviews

The purpose of `ReviewSpider` is to scrape all user-submitted reviews of a particular product from the [Steam community portal](http://steamcommunity.com/). 
By default, it scrapes reviews of products, where column `reviews_scraped` is empty (`NULL`) and `n_reviews` is larger than `10`.

```bash
scrapy crawl reviews --logfile=output/reviews_all.log --loglevel=INFO -s JOBDIR=output/reviews -s HTTPCACHE_ENABLED=False -a sqlite_path=output/db.sqlite3
```

If you want to scrape all reviews, the whole job takes a few days with Steam's generous rate limits.

## Obtaining news
The repository also includes a script that gives you an option to add news of all projects to the database. This is done by accessing Steam API and not scraping.

```bash
python -m scripts.get_news_api --sqlite_path output/db.sqlite3
```

## Minimizing database
If you manage to get complete database, but would like to get a sample database from it, you may use `minimize_dataset.py` script.

```bash
python -m scripts.minimize_dataset --sqlite_path output/db.sqlite3 --minimized_sqlite_path output/db_mini.psql --size 1000
```
