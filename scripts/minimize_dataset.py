import random
import argparse
import time
from steam.sqlite import Database
random.seed(10)


def copy_table(db, db_mini, fetch_sql, insert_sql, ids=None, keys=None):
    if ids:
        data = db.execute(fetch_sql, ids).fetchall()
    else:
        data = db.execute(fetch_sql).fetchall()
    for row in data:
        row_dict = dict(row)
        if keys:
            new_row_dict = {}
            for k in keys:
                new_row_dict[k] = row_dict[k]
            row_dict = new_row_dict
        columns = ', '.join(row_dict.keys())
        placeholders = ', '.join('?' * len(row_dict))

        insert_sql = insert_sql.format(columns, placeholders)
        db_mini.execute(insert_sql, list(row_dict.values()))
    db_mini.commit()


def main():
    parser = argparse.ArgumentParser(prog='DBMinimizer', description='The script minimizes database.')
    parser.add_argument("--sqlite_path", default='output/db.psql', type=str,
                        help="Path to database file.")
    parser.add_argument("--minimized_sqlite_path", default='output/db_mini.psql', type=str,
                        help="Path to database file.")
    parser.add_argument("--size", default=10000, type=int,
                        help="Number of products to be used (overall about 120k).")
    args = parser.parse_args()
    db_mini = Database(args.minimized_sqlite_path, True)
    db = Database(args.sqlite_path, False, True)
    products = [p_id[0] for p_id in db.execute(
        "SELECT id FROM product;",
        ()).fetchall()]
    selected_products = random.sample(products, args.size)
    product_placeholders = ', '.join('?' * len(selected_products))

    # copy product table
    fetch_sql = f'SELECT * FROM product WHERE id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO product ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products)

    # copy news
    fetch_sql = f'SELECT * FROM news WHERE product_id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO news ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products)

    # copy news_ntag
    fetch_sql = f'SELECT * FROM news_ntag LEFT JOIN news ON news.id == news_id WHERE news.product_id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO news_ntag ({}) VALUES ({})'
    keys = dict(db.execute(f'SELECT * FROM news_ntag LIMIT 1;').fetchone()).keys()
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products, keys=keys)

    # copy ntag (COMPLETE)
    fetch_sql = f'SELECT * FROM ntag;'
    insert_sql = 'INSERT INTO ntag ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql)

    # copy genre (COMPLETE)
    fetch_sql = f'SELECT * FROM genre;'
    insert_sql = 'INSERT INTO genre ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql)

    # copy spec (COMPLETE)
    fetch_sql = f'SELECT * FROM spec;'
    insert_sql = 'INSERT INTO spec ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql)

    # copy tag (COMPLETE)
    fetch_sql = f'SELECT * FROM tag;'
    insert_sql = 'INSERT INTO tag ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql)

    # copy product_genre
    fetch_sql = f'SELECT * FROM product_genre WHERE product_id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO product_genre ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products)

    # copy product_spec
    fetch_sql = f'SELECT * FROM product_spec WHERE product_id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO product_spec ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products)

    # copy product_tag
    fetch_sql = f'SELECT * FROM product_tag WHERE product_id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO product_tag ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products)

    # copy review
    fetch_sql = f'SELECT * FROM review WHERE product_id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO review ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products)

    # copy rscrape
    fetch_sql = f'SELECT * FROM rscrape WHERE product_id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO rscrape ({}) VALUES ({})'
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products)

    # copy rscrape_review
    fetch_sql = f'SELECT * FROM rscrape_review LEFT JOIN rscrape ON rscrape.id == rscrape_id WHERE rscrape.product_id IN ({product_placeholders});'
    insert_sql = 'INSERT INTO rscrape_review ({}) VALUES ({})'
    keys = dict(db.execute(f'SELECT * FROM rscrape_review LIMIT 1;').fetchone()).keys()
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products, keys=keys)

    # copy user
    fetch_sql = f'SELECT * FROM user LEFT JOIN review ON review.user_id == user.id WHERE review.product_id IN ({product_placeholders});'
    insert_sql = 'INSERT OR IGNORE INTO user ({}) VALUES ({})'
    keys = dict(db.execute(f'SELECT * FROM user LIMIT 1;').fetchone()).keys()
    copy_table(db, db_mini, fetch_sql, insert_sql, selected_products, keys=keys)


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Total:")
    print("--- %s seconds ---" % (time.time() - start_time))