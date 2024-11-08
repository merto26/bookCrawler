import mysql.connector
import pymongo
import time
import statistics

import graphCreator


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
            list(col.find(mongo_query))
            end_time = time.time()
            elapsed_time = end_time - start_time
            times.append(elapsed_time)
        client.close()
        median = statistics.median(times)
        return median
    except Exception as e:
        print(f"Error with MongoDB query: {e}")
        return None

pipeline1 = {
    'title': {'$regex': '^Book 6', '$options': 'i'},
    'genre': 'Science Fiction',
    'price_with_tax': {'$gte': 10, '$lte': 30},
    'availability': {'$gt': 0}
    }

pipeline2 = {
    'title': {'$regex': '^Book 6', '$options': 'i'},
    'genre': 'Science Fiction',
    '$expr': {
        '$and': [
            {'$gte': [{'$toDouble': '$price_with_tax'}, 10]},
            {'$lte': [{'$toDouble': '$price_with_tax'}, 30]},
            {'$gt': [{'$toInt': '$availability'}, 0]}
        ]
    }
}

mySQLQuery = """
    SELECT *
    FROM books
    WHERE title LIKE 'Book 6%'
      AND genre = 'Science Fiction'
      AND price_with_tax BETWEEN 10 AND 30
      AND availability > 0;
"""

# Database configurations for MySQL
datamodell_mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_1', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_2', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_3', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_4', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB', 'query': mySQLQuery}
]

mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'modellingTestDB_1', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'modellingTestDB_2', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'modellingTestDB_3', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'modellingTestDB_4', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'modellingTestDB', 'query': mySQLQuery}
]

# Database configurations for MongoDB
datamodell_mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_1', 'collection': 'books', 'query': pipeline1},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_2', 'collection': 'books', 'query': pipeline1},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_3', 'collection': 'books', 'query': pipeline1},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_4', 'collection': 'books', 'query': pipeline1},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB', 'collection': 'books', 'query': pipeline1}
]

mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoModellingTestDB_1', 'collection': 'books', 'query': pipeline2},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoModellingTestDB_2', 'collection': 'books', 'query': pipeline2},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoModellingTestDB_3', 'collection': 'books', 'query': pipeline2},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoModellingTestDB_4', 'collection': 'books', 'query': pipeline2},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'badModellingDB', 'collection': 'books', 'query': pipeline2}
]

def testeAnfrageDatamodell(num_repeats):
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

    for config in datamodell_mysql_configs:
        time_taken_median = run_mysql_query(config['host'], config['user'], config['password'], config['database'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'DatamodellMySQL', 'database': config['database'], 'time': time_taken_median})

    for config in mongo_configs:
        time_taken_median = run_mongo_query(config['host'], config['port'], config['database'], config['collection'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'MongoDB', 'database': config['database'], 'time': time_taken_median})

    for config in datamodell_mongo_configs:
        time_taken_median = run_mongo_query(config['host'], config['port'], config['database'], config['collection'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'DatamodellMongoDB', 'database': config['database'], 'time': time_taken_median})

    mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MySQL']
    datamodell_mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'DatamodellMySQL']
    mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MongoDB']
    datamodell_mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'DatamodellMongoDB']


    graphCreator.createLineGraph(mysql_times, datamodell_mysql_times, mongo_times, datamodell_mongo_times, "Datentyp")

if __name__ == '__main__':
    testeAnfrageDatamodell(5)