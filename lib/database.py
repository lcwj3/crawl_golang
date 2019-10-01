import os
import configparser

runPath = os.path.dirname(os.path.realpath(__file__))


class DatabaseConnection:
    def __init__(self, *args, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.db = kwargs.get('db')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.auth_source = kwargs.get('auth_source')


def get_postgres_connection():
    config = configparser.ConfigParser()
    config.read(os.path.join(runPath.split('/lib')[0], 'Config/configuration.ini'))
    postgres = {}

    try:
        postgres = config['Postgres']
    except:
        print('No Postgres Database config found, will use defaults')

    connection = {
        'host': postgres.get('Host', '192.168.1.16'),
        'port': postgres.get('Port', '5432'),
        'db': postgres.get('DB', 'cvetriage'),
        'user': postgres.get('User', 'cvetriage'),
        'password': postgres.get('Password', 'postgres')
    }

    return DatabaseConnection(**connection)


def get_mongo_connection():
    config = configparser.ConfigParser()
    config.read(os.path.join(runPath.split('/lib')[0], 'Config/configuration.ini'))
    mongo = {}

    try:
        mongo = config['Mongodb']
    except:
        print('No Mongo Database config found, will use defaults')

    # For mongo host and port, check env vars if not in config
    mongo_host, mongo_port = 'localhost', '27019'
    if 'MONGODB_PORT' in os.environ and os.environ['MONGODB_PORT']:
        mongo_port = os.environ['MONGODB_PORT']
    if 'MONGODB_HOST' in os.environ and os.environ['MONGODB_HOST']:
        mongo_host = os.environ['MONGODB_HOST']

    connection = {
        'host': mongo.get('Host', mongo_host),
        'port': mongo.get('Port', mongo_port),
        'db': mongo.get('DB', 'library-crawler'),
        'user': mongo.get('User', ''),
        'password': mongo.get('Password', ''),
        'auth_source': mongo.get('AuthSource', '')
    }
    return DatabaseConnection(**connection)

def get_neo4j_connection():
    config = configparser.ConfigParser()
    print(runPath)
    print(os.path.join(runPath.split('/lib')[0], 'Config/configuration.ini'))
    config.read(os.path.join(runPath.split('/lib')[0], 'Config/configuration.ini'))
    neo4j = {}

    try:
        neo4j = config['Neo4j']
    except:
        print('No Mongo Database config found, will use defaults')
    connection = {
        'host': neo4j.get('Host', 'localhost'),
        'port': neo4j.get('Port', '7687'),
        'db': neo4j.get('DB', ''),
        'user': neo4j.get('User', 'neo4j'),
        'password': neo4j.get('Password', '1234')
    }
    return DatabaseConnection(**connection)