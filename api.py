import urllib.request
import json
import argparse
import time
import logging
from steam.sqlite import Database
from tqdm import tqdm
logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser(prog='NewsAPIScraper', description='The that saves news from API to database.')
    parser.add_argument("--sqlite_path", default='output/db.psql', type=str,
                        help="Path to database file.")
    args = parser.parse_args()
    db = Database(args.sqlite_path, False)
    reviews_scraped = db.get_products_without_news()
    tags = db.get_tags()

    for i, product_id in tqdm(enumerate(reviews_scraped), total=len(reviews_scraped)):
        url_string = f"http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid={product_id}&count=20000&maxlength=20000&format=json"
        try:
            with urllib.request.urlopen(url_string) as url:
                data = json.load(url)
                for item in data['appnews']['newsitems']:
                    db.add_news(item, tags)
                db.commit()
        except urllib.error.HTTPError:
            logging.log(logging.INFO, f'Could not access: {url_string}')
        time.sleep(0.1)


if __name__ == "__main__":
    start_time = time.time()
    main()
    logging.log(logging.INFO, 'Total:')
    logging.log(logging.INFO, '--- %s seconds ---' % (time.time() - start_time))
