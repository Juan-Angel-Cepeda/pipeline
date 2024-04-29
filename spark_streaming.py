import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS streaming
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """
    )
    print("Keyspace created successfully")
    
def create_table(session):
    session.execute(
        """
            CREATE TABLE IF NOT EXISTS streaming.flights_data(
                id UUID PRIMARY KEY,
                hex TEXT,
                reg_number TEXT,
                flag TEXT,
                flight_number TEXT,
                flight_icao TEXT,
                flight_iata TEXT,
                lat TEXT,
                lng TEXT,
                alt TEXT,
                dir TEXT,
                speed TEXT,
                dep_icao TEXT,
                dep_iata TEXT,
                arr_icao TEXT,
                arr_iata TEXT,
                airline_icao TEXT,
                aircraft_icao TEXT,
                type TEXT,
            );
        """
    )
    print('Table created successfully')

def insert_data(session, **kwargs):
    print('Inserting data')
    fligth_id = kwargs.get('id')
    hex = kwargs.get('hex')
    reg_number = kwargs.get('reg_number')
    flag = kwargs.get('flag')
    flight_number = kwargs.get('flight_number')
    flight_icao = kwargs.get('flight_icao')
    flight_iata = kwargs.get('flight_iata')
    lat = kwargs.get('lat')
    lng = kwargs.get('lng')
    alt = kwargs.get('alt')
    dir = kwargs.get('dir')
    speed = kwargs.get('speed')
    dep_icao = kwargs.get('dep_icao')
    dep_iata = kwargs.get('dep_iata')
    arr_icao = kwargs.get('arr_icao')
    arr_iata = kwargs.get('arr_iata')
    airline_icao = kwargs.get('airline_icao')
    aircraft_icao = kwargs.get('aircraft_icao')
    type = kwargs.get('type')
    
    try:
        session.execute(
            """
            INSERT INTO streaming.flights_data(
                id,hex,reg_number,flag,flight_number,flight_icao,flight_iata,
                lat,lng,alt,dir,speed,dep_icao,dep_iata,arr_icao,arr_iata,
                airline_icao,aircraft_icao,type)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,(fligth_id,hex,reg_number,flag,flight_number,flight_icao,flight_iata,
                lat,lng,alt,dir,speed,dep_icao,dep_iata,arr_icao,arr_iata,
                airline_icao,aircraft_icao,type))
        logging.info('Data inserted successfully')
    
    except Exception as e:
        logging.error(f'Error inserting data, error:{e}')
        
def create_spark_connection():
    spark_conn = None
    try:
        spark_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info('Spark connection created successfully')
    
        
    except Exception as e:
        logging.error(f'No connection with spark, error:{e}')
    
    return spark_conn

def create_cassandra_connection():
    
    try:   
        cassandra_cluster = Cluster['localhost']
        session = cassandra_cluster.connect()
        logging.info('Cassandra connection created successfully')
        return session
    
    except Exception as e:
        logging.error(f'No connection with cassandra, error:{e}')
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(),False),
        StructField("hex", StringType(),True),
        StructField("reg_number", StringType(),True),
        StructField("flag", StringType(),True),
        StructField("flight_number", StringType(),True),
        StructField("flight_icao", StringType(),True),
        StructField("flight_iata", StringType(),True),
        StructField("lat", StringType(),True),
        StructField("lng", StringType(),True),
        StructField("alt", StringType(),True),
        StructField("dir", StringType(),True),
        StructField("speed", StringType(),True),
        StructField("dep_icao", StringType(),True),
        StructField("dep_iata", StringType(),True),
        StructField("arr_icao", StringType(),True),
        StructField("arr_iata", StringType(),True),
        StructField("airline_icao", StringType(),True),
        StructField("aircraft_icao", StringType(),True),
        StructField("type", StringType(),True)
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")).select("data.*")
    
    print(sel)
    return sel

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "flights") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info('Dataframe created successfully')
    
    except Exception as e:
        logging.warning(f'Error creating dataframe, error:{e}')
    
    return spark_df

if __name__ == "__main__":
    
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        
        flights_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(flights_df)
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
            #insert_data(session)
            logging.info('Starting streaming')
            streaming_query = ((selection_df.writeStream)
                            .format("org.apache.spark.sql.cassandra")
                            .option('checkpointLocation', '/tmp/checkpoint')
                            .option('keyspace', 'streaming')
                            .option('table', 'flights_data')
                            .start()
                            )
            