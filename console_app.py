import connect_books as cb
import psycopg as pg
import tabulate
import os
import shutil
from books import Books
from ratings import Ratings
from users import Users


def menu() -> str:
    """
    Present a menu of options to the user and return a valid option
    :return: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, Q, q
    """
    while True:
        print("1) Look up a book by ISBN")
        print("2) Look up book by author")
        print("3) Find an author's average book ratings")
        print("4) Find a book's average rating")
        print("5) Find the average rating for the user that has the most book reviews")
        print("6) Insert a new user")
        print("7) Insert a new book")
        print("8) Insert a new review")
        print("9) Find the top n authors that have the most published books")
        print("10) Find the top n most popular books by rating")
        print("Q) Quit")
        opt = input("> ")
        if opt in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'Q', 'q']:
            return opt


# Function to display content with paging based on terminal size
def display_with_paging(content, t_size):
    lines_per_page = t_size[1] - 1  # One less line for the prompt
    line_count = 0

    for line in content:
        print(line)
        line_count += 1
        if line_count >= lines_per_page:
            line_count = 0
            input("Press Enter to continue...")


# Get terminal size
terminal_size = shutil.get_terminal_size()


if __name__ == "__main__":
    while True:
        opt = menu()
        if opt == '1':
            print("Enter an ISBN: ")
            isbn = input("> ")
            conn = cb.connect()
            title = Books.get_title_by_isbn(conn, isbn)
            if title is None:
                print(f"ISBN {isbn} not found")
            else:
                print(f"Title: {title}")

        elif opt == '2':
            print("Enter an author: ")
            author = input("> ")
            conn = cb.connect()
            books = Books.get_books_by_author(conn, author)
            if books is None:
                print(f"Author {author} not found")
            else:
                # shorten length of title column
                output = tabulate.tabulate(books, headers=["Title", "Year", "Publisher", "ISBN"], tablefmt="grid")
                formatted_output = output.split('\n')
                display_with_paging(formatted_output, terminal_size)

        elif opt == '3':
            print("Enter an author: ")
            author = input("> ")
            conn = cb.connect()
            avg_rating = Ratings.get_avg_rating_by_author(conn, author)

            if avg_rating is None:
                print(f"Author {author} not found")
            else:
                output = tabulate.tabulate(avg_rating, headers=["Avg Rating", "# of Books"], tablefmt="grid")
                formatted_output = output.split('\n')
                display_with_paging(formatted_output, terminal_size)

        elif opt == '4':
            print("Enter a title: ")
            title = input("> ")

            print("Enter an author: ")
            author = input("> ")

            conn = cb.connect()
            avg_rating = Ratings.get_books_avg_rating(conn, title, author)

            if avg_rating is None:
                print(f"Book {title} by {author} not found")
            else:
                output = tabulate.tabulate(avg_rating, headers=["Rating", "Title", "Author"], tablefmt="grid")
                formatted_output = output.split('\n')
                display_with_paging(formatted_output, terminal_size)
                # print(f"Average rating of {title} by {author} is {avg_rating}")

        elif opt == '5':
            conn = cb.connect()
            avg_rating = Ratings.get_avg_rating_from_most_reviews(conn)
            if avg_rating is None:
                print("No users found")
            else:
                print(f"Average rating of user with most reviews is {avg_rating}")

        elif opt == '6':
            print("Enter a user ID: ")
            try:
                user_id = int(input("> "))
            except ValueError as e:
                print("Error: user ID must be an integer")
                continue

            print("Enter a location: ")
            location = input("> ")

            print("Enter an age: ")
            age = input("> ")

            conn = cb.connect()

            Users.insert_user(conn, user_id, location, age)

        elif opt == '7':
            print("Enter an ISBN: ")
            isbn = input("> ")

            print("Enter a title: ")
            title = input("> ")

            print("Enter an author: ")
            author = input("> ")

            print("Enter a year: ")
            try:
                year = int(input("> "))
            except ValueError as e:
                print("Error: year must be an integer")
                continue

            print("Enter a publisher: ")
            publisher = input("> ")

            conn = cb.connect()
            Books.insert_book(conn, isbn, title, author, year, publisher)

        elif opt == '8':
            print("Enter a user ID: ")
            # error check
            try:
                user_id = int(input("> "))
            except ValueError as e:
                print("Error: user ID must be an integer")
                continue

            # user could enter an isbn of 0717284832', '10'); DELETE FROM books WHERE isbn = '0440495717'; --
            print("Enter an ISBN: ")
            isbn = input("> ")

            print("Enter a rating: ")
            try:
                rating = int(input("> "))
            except ValueError as e:
                print("Error: rating must be an integer")
                continue

            conn = cb.connect()
            Ratings.insert_review(conn, user_id, isbn, rating)

        elif opt == '9':
            print("Enter a number of authors: ")
            try:
                n = int(input("> "))
            except ValueError as e:
                print("Error: number of authors must be an integer")
                continue

            conn = cb.connect()
            authors = Books.get_top_n_authors(conn, n)
            if authors is None:
                print("No authors found")
            else:
                output = tabulate.tabulate(authors, headers=["Author", "# of Books"], tablefmt="grid")
                formatted_output = output.split('\n')
                display_with_paging(formatted_output, terminal_size)

        elif opt == '10':
            print("Enter a number of books: ")
            try:
                n = int(input("> "))
            except ValueError as e:
                print("Error: number of books must be an integer")
                continue

            conn = cb.connect()
            books = Ratings.get_top_n_books(conn, n)
            if books is None:
                print("No books found")
            else:
                output = tabulate.tabulate(books, headers=["Title", "Author", "# of Ratings"], tablefmt="grid")
                formatted_output = output.split('\n')
                display_with_paging(formatted_output, terminal_size)

        elif opt in ['Q', 'q']:
            break
