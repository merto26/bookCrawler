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


def run_mongo_query(host, port, database, collection, mongo_query, update, num_repeats):
    """
    Programm welches eine Verbindung mit einer MongoDB-Datenbank aufbaut und ein Update variabel oft ausführt und die
    Ausführungszeiten misst.

    :param host: String des Hosts der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param port: String des Ports der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param database: String des Namen der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param collection: String des Namen Collection der Datenbank mit welcher eine Verbindung aufgebaut werden soll.
    :param mongo_query: String der jeweiligen Anfrage die ausgeführt werden soll.
    :param update: String eines Updates das ausgeführt werden soll.
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
            col.update_many(mongo_query, update)
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
    'title': { '$regex': '^Book', '$options': 'i' },
    'genre': 'Science Fiction',
    'price_with_tax': { '$gte': 10, '$lte': 30 },
    'availability': { '$gt': 0 }
}

pipeline2 = {
    'title': {'$regex': '^Book', '$options': 'i'},
    'genre': 'Science Fiction',
    '$expr': {
        '$and': [
            {'$gte': [{'$toDouble': '$price_with_tax'}, 10]},
            {'$lte': [{'$toDouble': '$price_with_tax'}, 30]},
            {'$gt': [{'$toInt': '$availability'}, 0]}
        ]
    }
}

mySQLQuery1 = """
            UPDATE books
            SET price_with_tax = price_with_tax * 1
            WHERE title LIKE 'Book%'
              AND genre = 'Science Fiction'
              AND price_with_tax BETWEEN 10 AND 30
              AND availability > 0;
"""


update = {
     '$mul': { 'price_with_tax': 1 }
}

update1 = [
        { '$set': { 'price_with_tax': { '$concat': ['$price_with_tax', ''] } } }
]

mysql_configs = [
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'testDB', 'query': mySQLQuery1},
    {'host': 'localhost', 'user': 'root', 'password': 'bettkante12345!', 'database': 'modellingTestDB', 'query': mySQLQuery1}
]

mongo_configs = [
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'mongoModellingTestDB', 'collection': 'books', 'query': pipeline1, 'update': update},
    {'host': 'mongodb://localhost:27017/', 'port': 27017, 'database': 'badModellingDB', 'collection': 'books', 'query': pipeline2, 'update': update1},
]

def testeUpdateDatamodell(num_repeats):
    """
    Die Funktion führt innerhalb von zwei Schleifen die Funktionen run_mysql_query und run_mongo_query so oft aus wie
    Elemente in den jeweiligen configs sind.
    :param num_repeats: Integer der angibt wie oft die jeweilige Anfrage ausgeführt werden soll.
    """
    db_times = []

    for config in mysql_configs:
        time_taken_median = run_mysql_query(config['host'], config['user'], config['password'],
                                                             config['database'], config['query'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'MySQL', 'database': config['database'], 'time': time_taken_median})

    for config in mongo_configs:
        time_taken_median = run_mongo_query(config['host'], config['port'], config['database'],
                                                             config['collection'], config['query'], config['update'], num_repeats)
        if time_taken_median:
            db_times.append({'db_type': 'MongoDB', 'database': config['database'], 'time': time_taken_median})

    mysql_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MySQL']
    mongo_times = [entry['time'] for entry in db_times if entry['db_type'] == 'MongoDB']

    graphCreator.createStackedBarGraph(x=["good modelling", "bad modelling"], y1=mysql_times, y2=mongo_times)