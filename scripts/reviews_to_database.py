import argparse
import configparser
import json
import os
import time
from pathlib import Path
import datetime as dt

from steam.sqlite import Database


def parse_args():
    parser = argparse.ArgumentParser()

    ## Required parameters
    parser.add_argument("--input_folder", default=None, type=str, help="The input file/folder.")
    parser.add_argument("--sqlite_path", default=None, type=str, help="The output file.")
    args = parser.parse_args()

    return args


def read_reviews(input_folder):
    files = sorted(os.listdir(input_folder), reverse=True)

    for file in files:
        if file.endswith(".jl") and not file.endswith('reviews.jl'):
            os.path.join(input_folder, file)
            yield from read_review(os.path.join(input_folder, file))


def read_review(path):
    print(f'READING REVIEWS... path - {path}')

    with open(path, 'r') as p_f:
        print('Path opened!')
        reviews_raw = p_f.readlines()
        print('Lines_read!')
        for review_raw in reviews_raw:
            # review_raw = reviews_raw.split('\n')
            yield json.loads(review_raw), path[-13:-3]

def main():
    args = parse_args()
    db = Database(args.sqlite_path, False)
    review_ids = db.get_review_ids()
    rscrape_ids = db.get_rscrape_ids()
    user_ids = db.get_user_ids()
    # add all reviews
    reviews = enumerate(read_reviews(args.input_folder))
    start_time = dt.datetime.today().timestamp()
    it = 0
    seconds = 0
    commits_number = 0
    for i, (item, date) in reviews:
        db.add_review(item, date + ' 00:00:00', review_ids, rscrape_ids, user_ids)

        if i % 10000 == 0:
            commits_number += 1
            db.commit()

        it += 1
        if (dt.datetime.today().timestamp() - start_time) > 1:
            seconds += 1
            print(f'{it/seconds} it/s {commits_number} number of commits')
            commits_number = 0

            start_time = dt.datetime.today().timestamp()
            if seconds == 10:
                seconds = 0
                it = 0



    with open('../data/reviews/raw/already_processed.txt', 'r') as rf:
        product_ids = {}
        processed_product_ids = set()
        for line in rf:
            processed_product_ids.add(int(line[:-1]))
            # db.add_review_scraped(int(line[:-1]))

        for k, v in rscrape_ids.items():
            if k[0] not in product_ids:
                product_ids[k[0]] = []
            product_ids[k[0]].append(k[1])

        for k, v in product_ids.items():
            if k not in processed_product_ids:
                for page in sorted(v)[-3:]:
                    db.update_rscrape_fails(rscrape_ids[(k, page)])
        db.commit()
        # TODO Add stuff to product table
        # TODO Go over review and set status to FAILED when necessary [page -3?]!



if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Total:")
    print("--- %s seconds ---" % (time.time() - start_time))
