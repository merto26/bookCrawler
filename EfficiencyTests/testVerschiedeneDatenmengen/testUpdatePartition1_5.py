import mysql.connector
import pymongo
import time
import graphCreator
import statistics


def run_mysql_query(host, user, password, database, query, num_repeats):
    """
    Programm welches eine Verbindung mit einer MySQL-Datenbank aufbaut und ein Update variabel oft ausführt und die
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
            conn.start_transaction()
            start_time = time.time()
            cursor.execute(query)
            conn.rollback()
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


def run_mongo_query(host, port, database, collection, pipeline, update, flag, num_repeats):
    """
    Programm welches eine Verbindung mit einer MongoDB-Datenbank aufbaut und ein Update variabel oft ausführt und die
    Ausführungszeiten misst.

    :param host: String des Hosts der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param port: String des Ports der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param database: String des Namen der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param collection: String des Namen Collection der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param pipeline: String der jeweiligen Anfrage die ausgeführt werden soll.
    :param update: String eines Updates das ausgeführt werden soll.
    :param flag: String der entscheidet ob ein Update auf einer paritionierten oder nicht-partitionierten Datenbank
    ausgeführt wird.
    :param num_repeats: Integer der angibt wie oft die jeweilige Anfrage ausgeführt werden soll.
    :return: Der Median der Ausführungzeiten der Datenbankanfragen.
    """

    #Hier wird entschieden ob zuerst aggregate() benutzt werden kann oder direkt update_many().
    if flag == "part":
        try:
            client = pymongo.MongoClient(host, port)
            db = client[database]
            col = db[collection]
            times = []
            for _ in range(num_repeats):
                start_time = time.time()
                # Da in update_many() nur einen Filter und keine Aggregation-Pipline annimmt werden hier die
                # Ergebnisse in einer Liste gespeichert um diese in einem Filter zu benutzen.
                matching_ids = []
                results = col.aggregate(pipeline)
                for result in results:
                    matching_ids.append(result['_id'])
                filter_condition = {'_id': {'$in': matching_ids}}
                col.update_many(filter_condition, update)
                end_time = time.time()
                elapsed_time = end_time - start_time
                times.append(elapsed_time)
            median = statistics.median(times)
            return median
        except Exception as e:
            print(f"Error with MongoDB query: {e}")
            return None
    else:
        try:
            client = pymongo.MongoClient(host, port)
            db = client[database]
            col = db[collection]
            times = []
            for _ in range(num_repeats):
                start_time = time.time()
                col.update_many(pipeline, update)
                end_time = time.time()
                elapsed_time = end_time - start_time
                times.append(elapsed_time)
            client.close()
            median = statistics.median(times)
            return median
        except Exception as e:
            print(f"Error with MongoDB query: {e}")
            return None


pipeline1 = [
    {
        "$lookup": {
            "from": "genre",
            "localField": "genre_id",
            "foreignField": "_id",
            "as": "genre_info"
        }
    },
    {
        '$unwind': '$genre_info'
    },
    {
        '$match': {
            'title': { '$regex': '^Book', '$options': 'i' },
            'genre_info.genre': 'Science Fiction',
            'price_with_tax': { '$gte': 10, '$lte': 30 },
            'availability': { '$gt': 0 }
        }
    },
    {
        '$project': { '_id': 1 }
    }
]

pipeline2 = {
    'title': { '$regex': '^Book', '$options': 'i' },
    'genre': 'Science Fiction',
    'price_with_tax': { '$gte': 10, '$lte': 30 },
    'availability': { '$gt': 0 }
}

update = {
     '$mul': { 'price_with_tax': 1 }
}

mySQLQuery1 = """
                UPDATE books b
                JOIN genres g ON b.genre_id = g.genre_id
                SET b.price_with_tax = b.price_with_tax * 1
                WHERE b.title LIKE 'Book%'
                  AND g.genre = 'Science Fiction'
                  AND b.price_with_tax BETWEEN 10 AND 30
                  AND b.availability > 0;
"""
mySQLQuery2 = """
                UPDATE books
                SET price_with_tax = price_with_tax * 1
                WHERE title LIKE 'Book%'
                  AND genre = 'Science Fiction'
                  AND price_with_tax BETWEEN 10 AND 30
                  AND availability > 0;
"""

mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_1', 'query': mySQLQuery2},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_2', 'query': mySQLQuery2},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_3', 'query': mySQLQuery2},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB_4', 'query': mySQLQuery2},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB', 'query': mySQLQuery2}

]

partition_mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB_1', 'query': mySQLQuery1},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB_2', 'query': mySQLQuery1},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB_3', 'query': mySQLQuery1},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB_4', 'query': mySQLQuery1},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'partitionTestDB', 'query': mySQLQuery1}
]

mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_1', 'collection': 'books', 'query': pipeline2, 'flag': "noPart"},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_2', 'collection': 'books', 'query': pipeline2, 'flag': "noPart"},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_3', 'collection': 'books', 'query': pipeline2, 'flag': "noPart"},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB_4', 'collection': 'books', 'query': pipeline2, 'flag': "noPart"},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoTestDB', 'collection': 'books', 'query': pipeline2, 'flag': "noPart"}
]

partition_mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTestDB_1', 'collection': 'books', 'query': pipeline1, 'flag': "part"},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTestDB_2', 'collection': 'books', 'query': pipeline1, 'flag': "part"},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTestDB_3', 'collection': 'books', 'query': pipeline1, 'flag': "part"},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTestDB_4', 'collection': 'books', 'query': pipeline1, 'flag': "part"},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoPartitionTest', 'collection': 'books', 'query': pipeline1, 'flag': "part"},
]

def testeUpdatePartition(num_repeats):
    """
    Die Funktion führt innerhalb von vier Schleifen die Funktionen run_mysql_query und run_mongo_query so oft aus wie
    Elemente in den jeweiligen configs sind.
    :param num_repeats: Integer der angibt wie oft die jeweilige Anfrage ausgeführt werden soll.
    """
    db_times = []

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
                                                             config['collection'], config['query'], update, config['flag'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'MongoDB', 'database': config['database'], 'time': time_taken_median})

    for config in partition_mongo_configs:
        time_taken_median = run_mongo_query(config['host'], config['port'], config['database'],
                                                             config['collection'], config['query'], update, config['flag'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'PartitionMongoDB', 'database': config['database'], 'time': time_taken_median})

    mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MySQL']
    partition_mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'PartitionMySQL']
    mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MongoDB']
    partition_mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'PartitionMongoDB']

    graphCreator.createLineGraph(mysql_times, partition_mysql_times, mongo_times, partition_mongo_times, "Partition")