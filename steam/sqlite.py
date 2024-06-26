"""
Tools for handling memory/actual database.
"""
import logging
import sqlite3
import os
import datetime as dt


class Product:
    def __init__(self):
        pass

    def create_product_tables(self):
        self.init("""CREATE TABLE genre (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE)
                    """)

        self.init("""CREATE TABLE spec (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE)
                    """)

        self.init("""CREATE TABLE tag (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE)
                    """)

        # create necessary tables
        self.init("""CREATE TABLE product (
                    id INTEGER PRIMARY KEY,
                    url TEXT,
                    news_url TEXT,
                    reviews_url TEXT,
                    title TEXT,
                    developer TEXT,
                    publisher TEXT,
                    release_date TEXT,
                    description_about TEXT,
                    app_name TEXT,
                    discount_price REAL,
                    price REAL,
                    early_access BOOLEAN,
                    sentiment TEXT,
                    n_reviews INTEGER,
                    metascore REAL,
                    description_reviews TEXT,
                    reviews_scraped DATETIME DEFAULT NULL,
                    Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """)

        self.init("""CREATE TABLE product_genre (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER,
                    genre_id INTEGER,
                    FOREIGN KEY(product_id) REFERENCES product(id),
                    FOREIGN KEY(genre_id) REFERENCES genre(id)
                    )
                    """)

        self.init("""CREATE TABLE product_spec (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER,
                    spec_id INTEGER,
                    FOREIGN KEY(product_id) REFERENCES product(id),
                    FOREIGN KEY(spec_id) REFERENCES spec(id)
                    )
                    """)

        self.init("""CREATE TABLE product_tag (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER,
                    tag_id INTEGER,
                    FOREIGN KEY(product_id) REFERENCES product(id),
                    FOREIGN KEY(tag_id) REFERENCES tag(id)
                    )
                    """)

    def add_product(self, item):
        """ Inserts a match to database. """
        product_cursor = self.execute("SELECT * FROM product WHERE id=?", (item['id'],))
        product = product_cursor.fetchone()

        if not product:
            # populate all items that are to be inserted
            item_dict = dict(item)
            all_keys = ['id', 'url', 'news_url', 'reviews_url', 'title', 'developer', 'publisher', 'release_date', 'description_about', 'app_name', 'discount_price', 'price', 'early_access', 'sentiment', 'n_reviews', 'metascore', 'description_reviews', 'reviews_scraped']
            for key in all_keys:
                if key not in item_dict:
                    item_dict[key] = None

            # insert product
            query = """
            INSERT INTO product (id, url, news_url, reviews_url, title, developer, publisher, release_date, description_about, app_name, discount_price, price, early_access, sentiment, n_reviews, metascore, description_reviews, reviews_scraped) 
            VALUES (:id, :url, :news_url, :reviews_url, :title, :developer, :publisher, :release_date, :description_about, :app_name, :discount_price, :price, :early_access, :sentiment, :n_reviews, :metascore, :description_reviews, :reviews_scraped)
            """
            product_cursor = self.execute(query, item_dict)
            product_id = product_cursor.lastrowid

            # handle genres
            if 'genres' in item and item['genres']:
                genres_dict = {g: g_id for g_id, g in self.execute("SELECT * FROM genre", ()).fetchall()}
                for genre in item['genres']:
                    # add new genres to database
                    if genre not in genres_dict:
                        query = """
                        INSERT INTO genre (name) 
                        VALUES (?)
                        """
                        genre_cursor = self.execute(query, (genre,))
                        genre_id = genre_cursor.lastrowid
                    else:
                        genre_id = genres_dict[genre]
                    # link products with genres
                    query = """
                    INSERT INTO product_genre (product_id, genre_id) 
                    VALUES (?, ?)
                    """
                    self.execute(query, (product_id, genre_id,))

            # handle specs
            if 'specs' in item and item['specs']:
                specs_dict = {g: g_id for g_id, g in self.execute("SELECT * FROM spec", ()).fetchall()}
                for spec in item['specs']:
                    # add new specs to database
                    if spec not in specs_dict:
                        query = """
                        INSERT INTO spec (name) 
                        VALUES (?)
                        """
                        spec_cursor = self.execute(query, (spec,))
                        spec_id = spec_cursor.lastrowid
                    else:
                        spec_id = specs_dict[spec]
                    # link products with specs
                    query = """
                    INSERT INTO product_spec (product_id, spec_id) 
                    VALUES (?, ?)
                    """
                    self.execute(query, (product_id, spec_id,))

            # handle tags
            if 'tags' in item and item['tags']:
                tags_dict = {g: g_id for g_id, g in self.execute("SELECT * FROM tag", ()).fetchall()}
                for tag in item['tags']:
                    # add new tags to database
                    if tag not in tags_dict:
                        query = """
                        INSERT INTO tag (name) 
                        VALUES (?)
                        """
                        tag_cursor = self.execute(query, (tag,))
                        tag_id = tag_cursor.lastrowid
                    else:
                        tag_id = tags_dict[tag]
                    # link products with tags
                    query = """
                    INSERT INTO product_tag (product_id, tag_id) 
                    VALUES (?, ?)
                    """
                    self.execute(query, (product_id, tag_id,))

            self.commit()

    def get_product_ids(self):
        query = """
        SELECT id 
        FROM product
        """
        return {p_id[0] for p_id in self.execute(query, ()).fetchall()}

    def init(self, *args, **kwargs):
        pass

    def commit(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        pass


class Review:
    def __init__(self):
        pass

    def create_review_tables(self):
        # self.new = True
        self.init("""CREATE TABLE rscrape (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT,
                    url TEXT,
                    page INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    product_id INTEGER,
                    FOREIGN KEY(product_id) REFERENCES product(id))
                    """)

        self.init("""CREATE TABLE user (
                    id TEXT PRIMARY KEY,
                    username TEXT,
                    products INTEGER)
                    """)

        self.init("""CREATE TABLE review (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recommended TEXT,
                    date TEXT,
                    text TEXT,
                    hours REAL,
                    found_awarding TEXT,
                    early_access BOOLEAN,
                    found_helpful INTEGER,
                    found_funny INTEGER,
                    compensation TEXT,
                    product_id INTEGER,
                    user_id TEXT,
                    FOREIGN KEY(product_id) REFERENCES product(id),
                    FOREIGN KEY(user_id) REFERENCES user(id))
                    """)

        self.init("""CREATE TABLE rscrape_review (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rscrape_id INTEGER,
                    review_id INTEGER,
                    FOREIGN KEY(rscrape_id) REFERENCES rscrape(id),
                    FOREIGN KEY(review_id) REFERENCES review(id))
                    """)

    def delete_partially_processed_reviews(self):
        """ Deletes partially scraped reviews. Cleans review, rscrape and rscrape_review tables. """
        query = """
        DELETE FROM rscrape 
        WHERE product_id IN (
            SELECT id 
            FROM product 
            WHERE (reviews_scraped=='1' OR reviews_scraped IS NULL) AND n_reviews IS NOT NULL
        )
        """
        self.execute(query)

        query = """
        DELETE FROM rscrape_review 
        WHERE rscrape_id IN (
            SELECT id FROM rscrape 
            WHERE product_id IN ( 
                SELECT id 
                FROM product 
                WHERE (reviews_scraped=='1' OR reviews_scraped IS NULL) AND n_reviews IS NOT NULL
            )
        )
        """
        self.execute(query)

        query = """
        DELETE FROM review 
        WHERE product_id IN (
            SELECT id FROM product 
            WHERE (reviews_scraped=='1' OR reviews_scraped IS NULL) AND n_reviews IS NOT NULL
        )
        """
        self.execute(query)

        query = """
        UPDATE product SET reviews_scraped=NULL 
        WHERE (reviews_scraped=='1' OR reviews_scraped IS NULL) AND n_reviews IS NOT NULL
        """
        self.execute(query)

        self.commit()

    def get_last_urls_from_partially_processed_products(self):
        """ Return a list with urls of or unfinished products that have reviews (10+). """
        query = """
        SELECT url 
        FROM (
            SELECT url, MAX(timestamp) AS latest_timestamp 
            FROM rscrape 
            WHERE product_id IN (
                SELECT id 
                FROM (
                    SELECT product.id, reviews_url, COUNT(review.id) AS scraped_review_n 
                    FROM product LEFT JOIN review ON product.id == review.product_id 
                    WHERE reviews_scraped IS NULL AND NOT n_reviews IS NULL 
                    GROUP BY product.id 
                    HAVING scraped_review_n > 0
                )
            )
            GROUP BY rscrape.product_id
        )
        """
        return [url[0] for url in self.execute(query, ()).fetchall()]

    def get_products_with_unprocessed_reviews(self):
        """ Return a list with unprocessed or unfinished products that have reviews (10+). """
        query = """
        SELECT product.id, reviews_url, COUNT(review.id) AS scraped_review_n 
        FROM product LEFT JOIN review ON product.id == review.product_id 
        WHERE reviews_scraped IS NULL AND NOT n_reviews IS NULL GROUP BY product.id HAVING scraped_review_n == 0
        """
        return [(p_id, reviews_url) for p_id, reviews_url, _ in self.execute(query, ()).fetchall()]

    def get_review_ids(self):
        query = """
        SELECT id, product_id, user_id 
        FROM review
        """
        review_cursor = self.execute(query)
        return {(p_id, u_id): r_id for r_id, p_id, u_id in review_cursor.fetchall()}

    def get_rscrape_ids(self):
        query = """
        SELECT id, product_id, page 
        FROM rscrape
        """
        rscrape_cursor = self.execute(query)
        return {(p_id, page): r_id for r_id, p_id, page in rscrape_cursor.fetchall()}

    def update_rscrape_fails(self, rscrape_id):
        query = """
        UPDATE rscrape
        SET status=?
        WHERE id=?
        """
        self.execute(query, ('FAILED', rscrape_id))

    def get_user_ids(self):
        query = """
        SELECT id 
        FROM user
        """
        user_cursor = self.execute(query)
        return {user_id[0] for user_id in user_cursor.fetchall()}

    def add_review_scraped(self, product_id, redirected=False):
        reviews_scraped = str(dt.datetime.today()) if not redirected else 'REDIRECTED'
        query = """
        UPDATE product
        SET reviews_scraped=?
        WHERE id=?
        """
        self.execute(query, (reviews_scraped, product_id))

    def add_review(self, item, date, review_ids, rscrape_ids, user_ids):
        """ Inserts a match to database. """
        if 'product_id' not in item or 'user_id' not in item:
            return

        if (int(item['product_id']), item['user_id']) not in review_ids:
            # populate all items that are to be inserted
            item_dict = dict(item)
            item_dict['date'] = date
            item_dict['status'] = 'OK'
            item_dict['timestamp'] = date
            all_keys = ['status', 'page', 'timestamp', 'product_id', 'username', 'products', 'recommended', 'date', 'text', 'hours', 'found_awarding', 'early_access', 'found_helpful', 'found_funny', 'compensation']
            for key in all_keys:
                if key not in item_dict:
                    item_dict[key] = None


            if (int(item['product_id']), item['page']) not in rscrape_ids:
                query = """
                INSERT INTO rscrape (status, page, url, timestamp, product_id) 
                VALUES (:status, :page, :url, :timestamp, :product_id)
                """
                rscrape_cursor = self.execute(query, item_dict)
                rscrape_id = rscrape_cursor.lastrowid
                rscrape_ids[(int(item['product_id']), item['page'])] = rscrape_id
            else:
                rscrape_id = rscrape_ids[(int(item['product_id']), item['page'])]

            if item['user_id'] not in user_ids:
                try:
                    query = """
                    INSERT INTO user (id, username, products) 
                    VALUES (:user_id, :username, :products)
                    """
                    self.execute(query, item_dict)
                except:
                    logging.log(logging.ERROR, 'Adding user to database failed!')
                user_ids.add(item['user_id'])

            # insert review
            query = """
            INSERT INTO review (recommended, date, text, hours, found_awarding, early_access, found_helpful, found_funny, compensation, product_id, user_id) 
            VALUES (:recommended, :date, :text, :hours, :found_awarding, :early_access, :found_helpful, :found_funny, :compensation, :product_id, :user_id)
            """
            review_cursor = self.execute(query, item_dict)
            review_id = review_cursor.lastrowid
            item_dict['review_id'] = review_id
            item_dict['rscrape_id'] = rscrape_id
            review_ids[(int(item['product_id']), item['user_id'])] = review_id

            query = """
            INSERT INTO rscrape_review (rscrape_id, review_id) 
            VALUES (:rscrape_id, :review_id)
            """
            self.execute(query, item_dict)

    def get_product_ids(self):
        query = """
        SELECT id 
        FROM product
        """
        return {p_id[0] for p_id in self.execute(query, ()).fetchall()}

    def init(self, *args, **kwargs):
        pass

    def commit(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        pass


class News:
    def __init__(self):
        pass

    def get_tags(self):
        query = """
        SELECT * 
        FROM ntag
        """
        return {g: g_id for g_id, g in self.execute(query, ()).fetchall()}

    def create_news_tables(self):
        self.init("""CREATE TABLE news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    author TEXT,
                    contents TEXT,
                    date DATETIME,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    product_id INTEGER,
                    feed_name TEXT,
                    feed_label TEXT,
                    feed_type INTEGER,
                    FOREIGN KEY(product_id) REFERENCES product(id))
                    """)

        self.init("""CREATE TABLE ntag (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL UNIQUE)
                            """)

        self.init("""CREATE TABLE news_ntag (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            news_id INTEGER,
                            ntag_id INTEGER,
                            FOREIGN KEY(news_id) REFERENCES news(id),
                            FOREIGN KEY(ntag_id) REFERENCES ntag(id)
                            )
                            """)

    def get_products_without_news(self):
        """ Return a list of product_ids that have reviews but no news. """
        query = """
        SELECT DISTINCT product.id 
        FROM product LEFT JOIN news ON product.id == news.product_id 
        WHERE reviews_scraped IS NOT NULL AND news.id IS NULL;
        """
        return [p_id[0] for p_id in self.execute(query, ()).fetchall()]

    def add_news(self, item_dict, tags):
        """ Inserts a match to database. """
        item_dict['date'] = dt.datetime.utcfromtimestamp(item_dict['date'])
        item_dict['timestamp'] = str(dt.datetime.today())
        item_dict['product_id'] = item_dict['appid']
        item_dict['feed_name'] = item_dict['feedname']
        item_dict['feed_label'] = item_dict['feedlabel']
        all_keys = ['title', 'author', 'contents', 'date', 'timestamp', 'product_id', 'feed_name', 'feed_label',
                    'feed_type']

        for key in all_keys:
            if key not in item_dict:
                item_dict[key] = None
        query = """
        INSERT INTO news (title, author, contents, date, timestamp, product_id, feed_name, feed_label, feed_type) 
        VALUES (:title, :author, :contents, :date, :timestamp, :product_id, :feed_name, :feed_label, :feed_type)
        """
        news_cursor = self.execute(query, item_dict)
        news_id = news_cursor.lastrowid

        if 'tags' in item_dict and item_dict['tags']:
            for tag in item_dict['tags']:
                # add new tags to database
                if tag not in tags:
                    query = """
                    INSERT INTO ntag (name) 
                    VALUES (?)
                    """
                    ntag_cursor = self.execute(query, (tag,))
                    ntag_id = ntag_cursor.lastrowid
                    tags[tag] = ntag_id
                else:
                    ntag_id = tags[tag]
                # link products with tags
                query = """
                INSERT INTO news_ntag (news_id, ntag_id) 
                VALUES (?, ?)
                """
                self.execute(query, (news_id, ntag_id,))

    def init(self, *args, **kwargs):
        pass

    def commit(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        pass


class Database(Product, Review, News):
    def __init__(self, sqlite_path, overwrite_db, row_factory=False):
        filename = sqlite_path

        if overwrite_db and os.path.exists(filename):
            os.remove(filename)

        self.new = not os.path.exists(filename)
        self.db = sqlite3.connect(filename)
        if row_factory:
            self.db.row_factory = sqlite3.Row

        self.commit()

        super().__init__()
        self.create_product_tables()
        self.create_review_tables()
        self.create_news_tables()

    def execute(self, *args, **kwargs):
        """ Executes database command.  """
        return self.db.execute(*args, **kwargs)

    def init(self, *args, **kwargs):
        """ Same as execute, only skipped if not a new database file. """
        if self.new:
            return self.execute(*args, **kwargs)

    def commit(self):
        """ Commits changes. """
        self.db.commit()

    def close(self):
        """ Close connection. """
        self.db.close()
