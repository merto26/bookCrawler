import pymongo
import json

def connect_to_mongodb(db_name):
    """
    Verbindungsaufbau zu MongoDB-Datenbank
    :param db_name: String des Namens des MongoDB-Datenbank
    :return: Die Collection auf der gearbeitet wird.
    """
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = mongo_client[db_name]
    collection = db["books"]
    return collection

def load_books_from_json(file_path):
    """
    Das Laden der Bücher aus der JSON-Dateien
    :param file_path: String des Pfades zur JSON-Dateien
    :return: Liste mit Büchern aus der JSON-Datei
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        books = json.load(file)
    return books

def insert_books_in_batches(collection, books, total_books, batch_size=1000):
    """
    Funktion zum integrieren der Daten in die MongoDB Datenbank
    :param collection: Collection auf der gearbeitet wird.
    :param books: Liste mit Büchern aus der JSON-Datei
    :param total_books: Anzahl der Bücher die integriert werden sollen.
    :param batch_size: Anzahl der Bücher die in jedem Schritt integriert werden sollen.
    """
    books = books[:total_books]

    for i in range(0, len(books), batch_size):
        batch = books[i:i + batch_size]
        collection.insert_many(batch)
        print(f"Inserted batch {i // batch_size + 1} in {collection.database.name}")

def main():

    json_file_path = "C:\\Users\\mert-\\PycharmProjects\\bookCrawlerProject\\JSON Formulare\\books_with_strings.json"

    books = load_books_from_json(json_file_path)

    required_books = 4000000
    if len(books) < required_books:
        raise ValueError("The JSON file does not contain enough documents. Ensure it has at least 4 million documents.")

    for i in range(1, 5):
        db_name = f"mongoModellingTestDB_{i}"
        num_books_to_insert = i * 1000000
        print(f"Inserting {num_books_to_insert} books into {db_name}")

        collection = connect_to_mongodb(db_name)

        insert_books_in_batches(collection, books, num_books_to_insert)

if __name__ == "__main__":
    main()