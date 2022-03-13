from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import *
import pandas as pd
from pyspark.sql.functions import split
from pyspark.sql.functions import *
import psycopg2
import psycopg2.extras as extras


#Creating Spark session object
spark = SparkSession.builder \
    .master("local") \
    .appName("Python_insert") \
    .config("spark.some.config.option", "some-value") \
    .config('spark.executor.instances', 4) \
    .config('spark.executor.cores', 4) \
    .config('spark.executor.memory', '10g') \
    .config('spark.driver.memory', '15g') \
    .config('spark.memory.offHeap.enabled', True) \
    .config('spark.memory.offHeap.size', '20g') \
    .config('spark.dirver.maxResultSize', '4096') \
    .getOrCreate()

###########################Reading the File in tp spark sql temp view for transformation####################
#reading the csv file in to a dataframe-spark sql 

df3 = spark.read.option("delimiter", "|").option("header", "true").csv(r'D:\Gayani\PYTHON\PCAP\Interworks-Assessment\DATA\flights.txt')
#df3.show(5)

#Dataset into temp table in saprk sql
df3.createOrReplaceTempView("AirportData")


############################Defining the Postgres connection###############################################

#defining the connection parameters
param_dic = {
    "host"               : "iw-recruiting-test.cygkjm9anrym.us-west-2.rds.amazonaws.com",
    "database"           : "tests_data_engineering",
    "user"               : "candidate8530",
    "password"           : "iGlntK0wn780SBi9",
    "sslmode"            : "disable",
    "keepalives"         : "1",
    "keepalives_idle"    : "30",
    "keepalives_interval": "10",
    "keepalives_count"   : "5"
}

####################Methods Area###########################################################################
#method to create the connection to post
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn

def insert_many_dimensions(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ', '.join(f'"{w}"' for w in df.columns)
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()

def insert_many_stg_fact(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ', '.join(f'"{w}"' for w in df.columns)
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()


def execute_mogrify(conn, df, table):
    """
    Using cursor.mogrify() to build the bulk insert query
    then cursor.execute() to execute the query
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ', '.join(f'"{w}"' for w in df.columns)
    # SQL quert to execute
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
    query  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)
    
    try:
        cursor.execute(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_mogrify() done")
    cursor.close()


#########################Insert AIRPORT Data ##############################################################

#preparing the AIRPORT DIMENSION
AirportData = spark.sql(" SELECT DISTINCT  ORIGINAIRPORTCODE  AS AIRPORTCODE , ORIGAIRPORTNAME  as AIRPORTNAME FROM AirportData ")                    
#AirportData.show()

DestAirportData = spark.sql(" SELECT DISTINCT DESTAIRPORTCODE AS AIRPORTCODE , DESTAIRPORTNAME AS AIRPORTNAME FROM AirportData ")                 
#DestAirportData.show()

AirportData.createOrReplaceTempView("a")
DestAirportData.createOrReplaceTempView("b")

finaliAirport= spark.sql("SELECT AIRPORTCODE, AIRPORTNAME AS UNCLEANEDAIRPORTNAME FROM a UNION  SELECT AIRPORTCODE, AIRPORTNAME  FROM b")

windowSpec  = Window.orderBy("AIRPORTCODE")
#cleaning the AirportName 
finaliAirport = finaliAirport.withColumn('AIRPORTNAME', rtrim(ltrim(split(finaliAirport['UNCLEANEDAIRPORTNAME'], ':').getItem(1))))\
       .withColumn("AIRPORTKEY", row_number().over(windowSpec))\
      .select('AIRPORTKEY','AIRPORTCODE', 'AIRPORTNAME') 

pandasDFAirport = finaliAirport.toPandas()


conn = connect(param_dic)
table ="candidate8530.\"DIM_AIRPORT\""
#insert_many_dimensions(conn, pandasDFAirport,table)


###################Insert AIRLINE Data ##########################################################################

AirlineData = spark.sql(" SELECT DISTINCT  AIRLINECODE  AS AIRLINECODE , AIRLINENAME  as UNCLEANEDAIRLINENAME FROM AirportData ")  
windowSpec  = Window.orderBy("AIRLINECODE")
#cleaning the AirlineName
AirlineData = AirlineData.withColumn('AIRLINENAME', rtrim(ltrim(split(AirlineData['UNCLEANEDAIRLINENAME'], ':').getItem(0))))\
      .withColumn("AIRLINEKEY", row_number().over(windowSpec))\
      .select('AIRLINEKEY','AIRLINECODE', 'AIRLINENAME') 

#AirlineData.printSchema()
#AirlineData.show()

AirlineData.createOrReplaceTempView("Airlines")

#convert pyspark sql dataframe in to pandas dataframe
pandasDFAirLine = AirlineData.toPandas()
#print(pandasDF)

pandasDFAirLine['AIRLINENAME'] = pandasDFAirLine['AIRLINENAME'].str.replace(r"[\"\',]", '')
#print(pandasDFAirLine)

table ="candidate8530.\"DIM_AIRLINE\""
#print("starts the insertion")
#insert_many_dimensions(conn, pandasDFAirLine,table)

###################Insert STATE Data #################################################################################

StateData = spark.sql(" SELECT DISTINCT  DESTSTATENAME AS UNCLEANEDDESTSTATE  , DESTSTATE  AS STATECODE  FROM AirportData WHERE DESTSTATE IS NOT NULL ") 

#Preparing STATE KEY AS Primary Key
windowSpec  = Window.orderBy("STATECODE")
StateData = StateData.withColumn('STATENAME', rtrim(ltrim(StateData['UNCLEANEDDESTSTATE'])))\
            .withColumn("STATEKEY", row_number().over(windowSpec))\
            .select('STATEKEY', 'STATECODE','STATENAME') 
#StateData.show()
StateData.createOrReplaceTempView("DistinctSateData")

#Convert pyspark dataframe in to pandas dataframe
pandasDFState = StateData.toPandas()
pandasDFState['STATENAME'] = pandasDFState['STATENAME'].str.replace(r"[\"\',]", '')
#print(pandasDFAirLine)
table ="candidate8530.\"DIM_STATE\""
#insert_many_dimensions(conn, pandasDFState,table)


###################Insert CITY Data #####################################################################################
CityData = spark.sql(" SELECT DISTINCT  B.STATEKEY,  A.DESTCITYNAME  as UNCLEANEDCITYNAME  FROM AirportData A  INNER JOIN  DistinctSateData  B ON A.DESTSTATE = B.STATECODE WHERE A.DESTSTATE IS NOT NULL") 
windowSpec  = Window.orderBy("CITYNAME")
CityData = CityData.withColumn('CITYNAME', rtrim(ltrim(CityData['UNCLEANEDCITYNAME'])))\
            .withColumn("CITYKEY", row_number().over(windowSpec))\
            .select('CITYKEY', 'STATEKEY' ,'CITYNAME') 

#Convert pyspark dataframe in to pandas dataframe
pandasDFCity = CityData.toPandas()
print(pandasDFCity)

pandasDFCity['CITYNAME'] = pandasDFCity['CITYNAME'].str.replace(r"[\"\',]", '')

table ="candidate8530.\"DIM_CITY\""
#insert_many_dimensions(conn, pandasDFCity,table)

#######################insert Fact Data to Staging table ##################################################################


factData = spark.sql(" SELECT FLIGHTDATE, TRANSACTIONID  ,  FLIGHTNUM, TAILNUM AS UNCLEANEDTAILNUM , ORIGINAIRPORTCODE , ORIGINCITYNAME, AIRLINECODE, ORIGINSTATE, DESTAIRPORTCODE, \
                        DESTCITYNAME, DESTSTATE, CRSDEPTIME,DEPTIME,DEPDELAY,TAXIOUT,WHEELSOFF,WHEELSON,TAXIIN,CRSARRTIME,ARRTIME,ARRDELAY,CRSELAPSEDTIME,ACTUALELAPSEDTIME,\
                        CANCELLED,DIVERTED, DISTANCE  FROM AirportData ") 

windowSpec  = Window.partitionBy("FLIGHTDATE").orderBy("FLIGHTDATE")
FlightFactData = factData.withColumn('TAILNUM', rtrim(ltrim(factData['UNCLEANEDTAILNUM'])))\
            .withColumn("TRANSKEY", row_number().over(windowSpec))\
            .select('TRANSKEY', 'FLIGHTDATE', 'TRANSACTIONID','FLIGHTNUM' ,'TAILNUM' ,'ORIGINAIRPORTCODE','ORIGINCITYNAME' ,'AIRLINECODE' , 'ORIGINSTATE', 'DESTAIRPORTCODE' ,'DESTCITYNAME'\
            ,'DESTSTATE' ,'CRSDEPTIME', 'DEPTIME','DEPDELAY','TAXIOUT' ,'WHEELSOFF','WHEELSON','TAXIIN','CRSARRTIME','ARRTIME','ARRDELAY','CRSELAPSEDTIME','ACTUALELAPSEDTIME',  \
            'CANCELLED','DIVERTED', 'DISTANCE' ) 

FlightFactData.show(5)
pandasDFFactFlight = FlightFactData.toPandas()
pandasDFFactFlight['TAILNUM'] = pandasDFFactFlight['TAILNUM'].str.replace(r"[\"\',]", '')
pandasDFFactFlight['DESTCITYNAME'] = pandasDFFactFlight['DESTCITYNAME'].str.replace(r"[\"\',]", '')
pandasDFFactFlight['ORIGINCITYNAME'] = pandasDFFactFlight['ORIGINCITYNAME'].str.replace(r"[\"\',]", '')


table ="candidate8530.\"STG_FLIGHTDATA\""
print("starts the insertion")
#conn = connect(param_dic)
print("starts the insertion")
#execute_mogrify(conn, pandasDFFactFlight,table)
row_count =  pandasDFFactFlight["TRANSKEY"].max()
print(row_count)
i = 1
while i < row_count: #row_count 849
  #print(i)
  if i != 0:
    pandasDFFact =  pandasDFFactFlight[pandasDFFactFlight['TRANSKEY'] == i]
    #insert_many_dimensions(conn, pandasDFFact,table)
    #execute_mogrify(conn, pandasDFFact, table)
  i += 1