import csv
import connect_books as cb


class Ratings:

    # class (static) data
    dt = "DROP TABLE IF EXISTS ratings"
    ct = """
            CREATE TABLE ratings    (
            user_id     integer
            CONSTRAINT fk_ratings_user_id
                REFERENCES users,
            isbn        text
            CONSTRAINT fk_ratings_isbn
                REFERENCES books,
            book_rating integer
        );
    """
    iit = "INSERT INTO ratings VALUES (?, ?, ?)"

    def __init__(self, conn: cb.connect()):
        self.conn = conn

    def create_table(self):
        self.conn.execute(Ratings.ct)
        self.conn.commit()

    def load_ratings(self):
        # context manager --> automatically closes file
        with open('ratings.csv') as f:
            # f = open("parts.csv")
            reader = csv.reader(f, delimiter=';')
            self.conn.executemany(Ratings.iit, list(reader))
            self.conn.commit()

    def drop_table(self):
        self.conn.execute(Ratings.dt)
        self.conn.commit()

    @staticmethod
    def remove_punctuation(input_string):
        import string
        return ''.join(char for char in input_string if char not in string.punctuation)

    # functions/queries specific to the ratings table

    # insert a new review
    @staticmethod
    def insert_review(conn: cb.connect(),
                      user_id: int,
                      isbn: str,
                      rating: int) -> None:

        # VULNERABLE TO INJECTION ATTACK
        cmd = "INSERT INTO ratings VALUES (\'" + str(user_id) + "\', \'" + isbn + "\', \'" + str(rating) + "\');"
        cur = conn.cursor()

        # get a cursor to execute the query
        try:
            cur.execute(cmd)
        except pg.Error as e:
            print(f"Error: {e}")

        # SAFE
        '''
        cmd = """
            INSERT INTO
                ratings
            VALUES
                (%s, %s, %s);
            """

        # get a cursor to execute the query
        cur = conn.cursor()

        # pg.Error check
        try:
            cur.execute(cmd, (user_id, isbn, rating))
        except pg.Error as e:
            print(f"Error: {e}")
            exit()
        '''

        conn.commit()

        cur.close()

    @staticmethod
    def get_avg_rating_by_author(conn: cb.connect(),
                                 name: str) -> list[[float, int]] | None:
        """
        get the average rating of a book by author
        :param conn:
        :param name: author
        :return: avg rating and number of books
        """
        name = Ratings.remove_punctuation(name)

        cmd = """
            WITH num_books as (SELECT count(*) as num_books
                                FROM books 
                                WHERE regexp_replace(author, '[^\w\s]', '', 'g') ILIKE %s)               
            SELECT
                round(AVG(book_rating), 1), num_books
            FROM
                books NATURAL JOIN ratings, num_books
            WHERE
                regexp_replace(author, '[^\w\s]', '', 'g') ILIKE %s
            GROUP BY
                num_books
            """

        # get a cursor to execute the query
        cur = conn.cursor()

        try:
            cur.execute(cmd, (name, name))
        except pg.Error as e:
            print(f"Error: {e}")

        if cur.rowcount == 0:
            return None

        rv = []
        for row in cur:
            rv.append(row)

        cur.close()

        return rv

    @staticmethod
    def get_books_avg_rating(conn: cb.connect(),
                             title: str,
                             author: str) -> list[[float, str]] | None:
        """
        get the average rating of a book
        :param conn:
        :param title:
        :param author:
        :return: average rating of the book, title, author
        """

        author = Ratings.remove_punctuation(author)
        title = Ratings.remove_punctuation(title)

        cmd = """
            SELECT
                round(AVG(book_rating), 1), title, author
            FROM
                books NATURAL JOIN ratings
            WHERE
                regexp_replace(title, '[^\w\s]', '', 'g') ILIKE %s AND regexp_replace(author, '[^\w\s]', '', 'g') ILIKE %s
            GROUP BY
                title, author
            """

        # get a cursor to execute the query
        cur = conn.cursor()

        try:
            cur.execute(cmd, (title, author))
        except pg.Error as e:
            print(f"Error: {e}")

        if cur.rowcount == 0:
            return None

        rv = []
        for row in cur:
            rv.append(row)

        cur.close()

        return rv

    # Find the average rating for the user that has the most book reviews.
    @staticmethod
    def get_avg_rating_from_most_reviews(conn: cb.connect()) -> float | None:
        """
        get the average rating of the user with the most reviews
        :param conn:
        :return: average rating of user with most reviews
        """

        cmd = """
        WITH count_reviews AS (SELECT user_id, count(*) as count
                          FROM ratings
                          GROUP BY user_id)
        SELECT round(avg(book_rating), 1)
        FROM ratings
        where user_id = (SELECT user_id from count_reviews where count = (SELECT MAX(count) from count_reviews));
        """

        # get a cursor to execute the query
        cur = conn.cursor()
        try:
            cur.execute(cmd)
        except pg.Error as e:
            print(f"Error: {e}")

        rv = None
        if cur.rowcount > 0:
            rv = cur.fetchone()[0]
        cur.close()
        return rv


    @staticmethod
    def get_top_n_books(conn: cb.connect(),
                        n: int) -> list[str, int] | None:
        """
        get the top n books by number of reviews
        :param conn: connection to the database
        :param n: user inputted number
        :return: list of books, author and number of reviews
        """

        cmd = """
        WITH count_ratings AS (SELECT count(*) as count, title, author
                            FROM
                            books NATURAL JOIN ratings
                            GROUP BY title, author)
        SELECT title, author, count
        FROM count_ratings
        ORDER BY count DESC 
        LIMIT %s;
        """

        cur = conn.cursor()
        try:
            cur.execute(cmd, (n,))
        except pg.Error as e:
            print(f"Error: {e}")

        if cur.rowcount == 0:
            return None

        rv = []
        for row in cur:
            rv.append(row)

        cur.close()

        return rv
