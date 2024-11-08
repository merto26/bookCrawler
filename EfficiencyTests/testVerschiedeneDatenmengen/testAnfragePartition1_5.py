import statistics

import mysql.connector
import pymongo
import time

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
            list(col.aggregate(mongo_query))
            end_time = time.time()
            elapsed_time = end_time - start_time
            times.append(elapsed_time)
        client.close()
        median = statistics.median(times)
        return median
    except Exception as e:
        print(f"Error with MongoDB query: {e}")
        return None


partitionPipeline = [
    {
        "$lookup": {
            "from": "genre",
            "localField": "genre_id",
            "foreignField": "_id",
            "as": "genre_info"
        }
    },
    {
        "$unwind": "$genre_info"
    },
    {
        "$group": {
            "_id": "$genre_info.genre",
            "book_count": {"$sum": 1}
        }
    }
]
pipeline = [
    {
        "$group": {
            "_id": "$genre",
            "book_count": {"$sum": 1}
        }
    }
]
partitionMySQLQuery = """
                SELECT g.genre, COUNT(*) AS book_count
                FROM books b
                JOIN genres g ON b.genre_id = g.genre_id
                GROUP BY g.genre
                ORDER BY book_count DESC;
"""
mySQLQuery = """
                SELECT genre, COUNT(*) AS book_count
                FROM books
                GROUP BY genre
                ORDER BY book_count DESC
"""

mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_1', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_2', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_3', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_4', 'query': mySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB', 'query': mySQLQuery}
]

partition_mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB_1',
     'query': partitionMySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB_2',
     'query': partitionMySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB_3',
     'query': partitionMySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB_4',
     'query': partitionMySQLQuery},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB',
     'query': partitionMySQLQuery}
]

mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_1', 'collection': 'books',
     'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_2', 'collection': 'books',
     'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_3', 'collection': 'books',
     'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_4', 'collection': 'books',
     'query': pipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB', 'collection': 'books',
     'query': pipeline}
]

partition_mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTestDB_1', 'collection': 'books',
     'query': partitionPipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTestDB_2', 'collection': 'books',
     'query': partitionPipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTestDB_3', 'collection': 'books',
     'query': partitionPipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTestDB_4', 'collection': 'books',
     'query': partitionPipeline},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTest', 'collection': 'books',
     'query': partitionPipeline}
]


def testeAnfragePartition(num_repeats):
    """
    Die Funktion führt innerhalb von vier Schleifen die Funktionen run_mysql_query und run_mongo_query so oft aus wie
    Elemente in den jeweiligen configs sind.
    :param num_repeats: Integer der angibt wie oft die jeweilige Anfrage ausgeführt werden soll.
    """
    db_times = []

    #Einsetzten der Configdaten, in die Funktionen
    for config in mysql_configs:
        time_taken_median = run_mysql_query(config['host'], config['user'], config['password'],
                                                             config['database'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'MySQL', 'database': config['database'], 'time': time_taken_median})

    for config in partition_mysql_configs:
        time_taken_median = run_mysql_query(config['host'], config['user'], config['password'],
                                                             config['database'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'PartitionMySQL', 'database': config['database'], 'time': time_taken_median})

    for config in mongo_configs:
        time_taken_median = run_mongo_query(config['host'], config['port'], config['database'],
                                                             config['collection'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'MongoDB', 'database': config['database'], 'time': time_taken_median})

    for config in partition_mongo_configs:
        time_taken_median = run_mongo_query(config['host'], config['port'], config['database'],
                                                            config['collection'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'PartitionMongoDB', 'database': config['database'], 'time': time_taken_median})

    #Speicherung der Zeiten in entsprechenden Listen.
    mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MySQL']
    partition_mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'PartitionMySQL']
    mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MongoDB']
    partition_mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'PartitionMongoDB']

    graphCreator.createLineGraph(mysql_times, partition_mysql_times, mongo_times, partition_mongo_times, "Partition")