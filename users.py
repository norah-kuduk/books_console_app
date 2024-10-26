import sqlite3 as sq
import csv
import connect_books as cb


class Users:
    # class (static) data
    dt = "DROP TABLE IF EXISTS users"
    ct = """
            CREATE TABLE IF NOT EXISTS users(
            user_id  integer NOT NULL
                CONSTRAINT users_pk
                PRIMARY KEY,
            location text,
            age      text
        );
    """
    iit = "INSERT INTO users VALUES (?, ?, ?)"

    def __init__(self, conn: cb.connect()):
        self.conn = conn

    def create_table(self):
        self.conn.execute(Books.ct)
        self.conn.commit()

    def load_users(self):
        # context manager --> automatically closes file
        with open('users.csv') as f:
            reader = csv.reader(f, delimiter=';')
            self.conn.executemany(Users.iit, list(reader))
            self.conn.commit()

    def drop_table(self):
        self.conn.execute(Users.dt)
        self.conn.commit()

    # functions/queries specific to the parts table
    @staticmethod
    def remove_punctuation(input_string):
        import string
        return ''.join(char for char in input_string if char not in string.punctuation)

    # insert a user
    @staticmethod
    def insert_user(conn: cb.connect(),
                    user_id: int,
                    location: str,
                    age: str) -> None:

        cmd = """
            INSERT INTO
                users
            VALUES
                (%s, %s, %s);
            """

        # get a cursor to execute the query
        cur = conn.cursor()
        try:
            cur.execute(cmd, (user_id, location, age))
        except pg.Error as e:
            print(f"Error: {e}")

        conn.commit()

        cur.close()
