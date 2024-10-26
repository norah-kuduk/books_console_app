import psycopg as pg

def connect() -> pg.Connection:
    """
    return a connection object to the books database or
    exit if failure
    :return: connection object
    """

    # what can go wrong
    try:
        pwd_file = open("/Users/norahkuduk/.pwd", 'r')
    except OSError as e:
        print(f"Error: file not readable, {e}")
        exit()

    # what can go wrong
    try:
        # connect to an existing database
        conn = pg.connect(
            dbname='james_norah_booksdataset',
            host="ada.hpc.stlawu.edu",
            user="nlkudu21",
            password=pwd_file.readline().strip()
        )

    except pg.Error as e:
        print(f"Error: could not connect to database, {e}")
        exit()
    finally:
        pwd_file.close()  # good security practice

    return conn
