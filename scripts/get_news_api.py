import sys
import urllib.request
import json
import argparse
import time
import logging
from steam.sqlite import Database
from tqdm import tqdm
logging.basicConfig(level=logging.INFO)


def parse_args(args):
    parser = argparse.ArgumentParser(prog='NewsAPIScraper',
                                     description='A script that saves news from API to database.')
    parser.add_argument("--sqlite_path", default='output/db.sqlite3', type=str,
                        help="Path to database file.")
    parser.add_argument("--api_key", default=None, type=str,
                        help="API key. It is not necessary for the script to work.")
    return parser.parse_args(args)


def main():
    args = parse_args(sys.argv[1:])
    db = Database(args.sqlite_path, False)
    reviews_scraped = db.get_products_without_news()
    tags = db.get_tags()

    for i, product_id in tqdm(enumerate(reviews_scraped), total=len(reviews_scraped)):
        if args.api_key:
            url_string = f"http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?key={args.api_key}&appid={product_id}&count=20000&maxlength=20000&format=json"
        else:
            url_string = f"http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid={product_id}&count=20000&maxlength=20000&format=json"
        try:
            with urllib.request.urlopen(url_string) as url:
                data = json.load(url)
                for item in data['appnews']['newsitems']:
                    db.add_news(item, tags)
                db.commit()
        except urllib.error.HTTPError:
            logging.warning(logging.WARNING, f'Could not access: {url_string}')
        time.sleep(0.1)


if __name__ == "__main__":
    start_time = time.time()
    main()
    logging.log(logging.INFO, 'Total:')
    logging.log(logging.INFO, '--- %s seconds ---' % (time.time() - start_time))
