import mysql.connector
import pymongo
import time
import graphCreator
import statistics



def run_mysql_query(host, user, password, database, query, num_repeats):
    """
    Programm welches eine Verbindung mit einer MySQL-Datenbank aufbaut und eine Anfrage variabel oft ausführt und die
    Ausführungszeiten misst.

    :param host: String des Hosts der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param user: String des Benutzernamen um sich mit der Datenbank zu verbinden.
    :param password: String des Passworts um sich mit der Datenbank zu verbinden.
    :param database: String des Namen der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param query: Die Jeweilige Anfrage die ausgeführt werden soll.
    :param num_repeats: Integer der angibt wie oft die jeweilige Anfrage ausgeführt werden soll.
    :return: Der Median der Ausführungzeiten der Datenbankanfragen.
    """
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        times = []
        for _ in range(num_repeats):
            start_time = time.time()
            cursor.execute(query)
            cursor.fetchall()
            end_time = time.time()
            elapsed_time = end_time - start_time
            times.append(elapsed_time)
        cursor.close()
        conn.close()
        median = statistics.median(times)

        return median
    except mysql.connector.Error as e:
        print(f"Error with MySQL query: {e}")
        return None

def run_mongo_query(host, port, database, collection, mongo_query, num_repeats):
    """
    Programm welches eine Verbindung mit einer MongoDB-Datenbank aufbaut und eine Anfrage variabel oft ausführt und die
    Ausführungszeiten misst.

    :param host: String des Hosts der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param port: String des Ports der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param database: String des Namen der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param collection: String des Namen Collection der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param mongo_query: String der jeweiligen Anfrage die ausgeführt werden soll.
    :param num_repeats: Integer der angibt wie oft die jeweilige Anfrage ausgeführt werden soll.
    :return: Der Median der Ausführungzeiten der Datenbankanfragen.
    """
    try:
        client = pymongo.MongoClient(host, port)
        db = client[database]
        col = db[collection]
        times = []
        for _ in range(num_repeats):
            start_time = time.time()
            list(col.find(mongo_query).sort(sort_criteria))
            end_time = time.time()
            elapsed_time = end_time - start_time
            times.append(elapsed_time)
        client.close()
        median = statistics.median(times)
        return median
    except Exception as e:
        print(f"Error with MongoDB query: {e}")
        return None




pipeline = {
    'title': { '$regex': '^Book 8', '$options': 'i' },
    'genre': 'History'
}
sort_criteria = [
    ('title', 1),
    ('genre', 1)
]

mySQLQuery = """
                SELECT *
                FROM books
                WHERE title LIKE 'Book 8%'
                  AND genre = 'History'
                ORDER BY title ASC, genre ASC;
"""

mySQLQuery1 = """
                SELECT *
                FROM books FORCE INDEX(idx_title_genre)
                WHERE title LIKE 'Book 8%'
                  AND genre = 'History'
                ORDER BY title ASC, genre ASC;
"""

mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_1', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_2', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_3', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_4', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB', 'query': mySQLQuery}
]

indexed_mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'indexTestDB_1', 'query': mySQLQuery1},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'indexTestDB_2', 'query': mySQLQuery1},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'indexTestDB_3', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'indexTestDB_4', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'indexTestDB', 'query': mySQLQuery}
]

# Database configurations for MongoDB
mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_1', 'collection': 'books', 'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_2', 'collection': 'books', 'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_3', 'collection': 'books', 'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_4', 'collection': 'books', 'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB', 'collection': 'books', 'query': pipeline},
]

indexed_mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoIndexTestDB_1', 'collection': 'books', 'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoIndexTestDB_2', 'collection': 'books', 'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoIndexTestDB_3', 'collection': 'books', 'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoIndexTestDB_4', 'collection': 'books', 'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoIndexTestDB', 'collection': 'books', 'query': pipeline}
]

def testeAnfrageIndex(num_repeats):
    """
    Die Funktion führt innerhalb von vier Schleifen die Funktionen run_mysql_query und run_mongo_query so oft aus wie
    Elemente in den jeweiligen configs sind.
    :param num_repeats: Integer der angibt wie oft die jeweilige Anfrage ausgeführt werden soll.
    """
    db_times = []

    for config in mysql_configs:
        time_taken_median = run_mysql_query(config['host'], config['user'], config['password'], config['database'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'MySQL', 'database': config['database'], 'time': time_taken_median})

    for config in indexed_mysql_configs:
        time_taken_median  = run_mysql_query(config['host'], config['user'], config['password'], config['database'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'IndexedMySQL', 'database': config['database'], 'time': time_taken_median})

    for config in mongo_configs:
        time_taken_median = run_mongo_query(config['host'], config['port'], config['database'], config['collection'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'MongoDB', 'database': config['database'], 'time': time_taken_median})

    for config in indexed_mongo_configs:
        time_taken_median = run_mongo_query(config['host'], config['port'], config['database'], config['collection'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'IndexedMongoDB', 'database': config['database'], 'time': time_taken_median})

    mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MySQL']
    indexed_mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'IndexedMySQL']
    mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MongoDB']
    indexed_mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'IndexedMongoDB']

    graphCreator.createLineGraph(mysql_times, indexed_mysql_times, mongo_times, indexed_mongo_times, "Index")

if __name__ == '__main__':
    testeAnfrageIndex(1)