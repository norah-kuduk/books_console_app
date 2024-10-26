import sqlite3 as sq
import csv
import psycopg as pg
import connect_books as cb


class Books:
    # class (static) data
    dt = "DROP TABLE IF EXISTS books"
    ct = """
            CREATE TABLE IF NOT EXISTS books (
            isbn varchar(10) PRIMARY KEY,
            title text NOT NULL,
            author    text,
            year      numeric(4),
            publisher text,
            small     text,
            medium    text,
            large     text
        );
    """
    iit = "INSERT INTO books VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

    def __init__(self, conn: cb.connect()):
        self.conn = conn

    def create_table(self):
        self.conn.execute(Books.ct)
        self.conn.commit()

    def load_books(self):
        # context manager --> automatically closes file
        with open('books.csv') as f:
            # f = open("parts.csv")
            reader = csv.reader(f, delimiter=';')
            self.conn.executemany(Books.iit, list(reader))
            self.conn.commit()

    def drop_table(self):
        self.conn.execute(Books.dt)
        self.conn.commit()

    # functions/queries specific to the parts table

    @staticmethod
    def remove_punctuation(input_string):
        import string
        return ''.join(char for char in input_string if char not in string.punctuation)

    @staticmethod
    def get_title_by_isbn(conn: cb.connect(),
                          isbn: str) -> str | None:
        """
        Get the title of a book by its isbn
        :param conn:
        :param isbn:
        :return: title
        """

        cmd = "SELECT title FROM books WHERE isbn = %s;"

        # get a cursor to execute the query
        cur = conn.cursor()
        cur.execute(cmd, (isbn.strip(),))

        # a cursor is an object that refers to a db and is used for executing queries
        # a single connection can have many cursors

        # python's conditional expression
        # return None if cur.rowcount == 0 else cur.fetchone()[0]
        # java conditional: return cur.rowcount == 0 ? None : cur.fetchone()

        # get the resultset which is either of size zero or one
        rv = None
        if cur.rowcount > 0:
            rv = cur.fetchone()[0]
        cur.close()
        return rv

    # insert a new book
    @staticmethod
    def insert_book(conn: cb.connect(),
                    isbn: str,
                    title: str,
                    author: str,
                    year: int,
                    publisher: str) -> None:
        cmd = """
            INSERT INTO
                books
            VALUES
                (%s, %s, %s, %s, %s, NULL, NULL, NULL);
            """

        # get a cursor to execute the query
        cur = conn.cursor()

        try:
            cur.execute(cmd, (isbn, title, author, year, publisher))
        except pg.Error as e:
            print(f"Error: {e}")

        conn.commit()

        cur.close()

    @staticmethod
    def get_books_by_author(conn: cb.connect(),
                            author: str) -> list[[str, int, str, str]] | None:
        """
        Get books by author
        :param conn:
        :param author:
        :return: table of books by author with output formatting title, year, publisher, isbn
        """

        author = Books.remove_punctuation(author)

        # remove punctuation from name
        cmd = """
            SELECT 
                title, year, publisher, isbn 
            FROM 
                books 
            WHERE 
                regexp_replace(author, '[^\w\s]', '', 'g') ILIKE %s;
            """

        # get a cursor to execute the query
        cur = conn.cursor()
        cur.execute(cmd, (author,))

        if cur.rowcount == 0:
            return None

        rv = []
        for row in cur:
            rv.append(row)

        cur.close()

        return rv

    @staticmethod
    def get_top_n_authors(conn: cb.connect(),
                          n: int) -> list[str, int] | None:
        """
        Get the top n authors by number of books
        :param conn:
        :param n:
        :return: table of n authors with author, count
        """

        cmd = """
        WITH count_books AS (SELECT author, count(*) as count
                            FROM books
                            GROUP BY author)
        SELECT author, count
        FROM count_books
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
