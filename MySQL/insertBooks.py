import mysql.connector
import json


def connect_to_mysql(db_name):
    """
    Verbindungs aufbau mit der jeweiligen MySQL-Datenbank
    :param db_name: String des Namen der Datenbank
    :return: Connection und Cursor
    """
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='bettkante12345!',
        database=db_name
    )

    cursor = mysql_conn.cursor()
    return mysql_conn, cursor


def create_table_if_not_exists(cursor):
    """
    Erstellt die Tabelle in die die Daten integriert werden.
    :param cursor:  Der Cursor der Verbindung zur Datenbank.
    """
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS books(
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                genre VARCHAR(100),
                price_with_tax VARCHAR(50),
                price_without_tax VARCHAR(50),
                availability VARCHAR(50),
                rating VARCHAR(50),
                description TEXT
            )
        """)
    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS books (
    #       book_id INT PRIMARY KEY AUTO_INCREMENT,
    #       title VARCHAR(255),
    #       price_with_tax DECIMAL(10, 2),
    #       price_without_tax DECIMAL(10, 2),
    #       availability INT,
    #       rating DECIMAL(3, 2) DEFAULT NULL,
    #       description TEXT,
    #       genre_id INT,
    #       FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
    #                 )
    # """)


def load_books_from_json(file_path):
    """
    Funktion die die jeweilige JSON lädt.
    :param file_path: String des Pfades welcher zur JSON-Datei führt.
    :return: Die Bücher aus der JSON-Datei
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        books = json.load(file)
    return books


def insert_books_in_batches(cursor, books, total_books, batch_size=1000):
    """
    Funktion zum integrieren der Bücher in die Datenbank.
    :param cursor:  Der Cursor der Verbindung zur Datenbank.
    :param books: Liste mit allen Büchern aus der JSON-Datei
    :param total_books: Anzahl der Bücher die integriert werden sollen.
    :param batch_size: Anzahl der Bücher die in jedem Schritt integriert werden sollen.
    """
    books = books[:total_books]
    insert_query = """
        INSERT INTO books (title, genre, price_with_tax, price_without_tax, availability, rating, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for i in range(0, len(books), batch_size):
        batch = books[i:i + batch_size]
        data = [
            (book["title"],book["genre"] ,book["price_with_tax"], book["price_without_tax"],
             book["availability"], book["rating"], book["description"])
            for book in batch
        ]
        cursor.executemany(insert_query, data)
        print(f"Inserted batch {i // batch_size + 1}")


def main():
    json_file_path = "C:\\Users\\mert-\\PycharmProjects\\bookCrawlerProject\\JSON Formulare\\books_with_strings.json"

    books = load_books_from_json(json_file_path)

    required_books = 4000000
    if len(books) < required_books:
        raise ValueError("The JSON file does not contain enough documents. Ensure it has at least 4 million documents.")

    for i in range(1, 5):
        db_name = f"modellingTestDB_{i}"
        num_books_to_insert = i * 1000000
        print(f"Inserting {num_books_to_insert} books into {db_name}")

        mysql_conn, cursor = connect_to_mysql(db_name)

        create_table_if_not_exists(cursor)

        insert_books_in_batches(cursor, books, num_books_to_insert)

        mysql_conn.commit()
        cursor.close()
        mysql_conn.close()


if __name__ == "__main__":
    main()