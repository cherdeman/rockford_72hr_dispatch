import urllib.request
import json
import csv
import psycopg2
import os
import sys
#import atexit
import logging
#import db_statements
logger = logging.getLogger('rockford72hr')
sh = logging.StreamHandler(sys.stdout)
logger.addHandler(sh)
logger.setLevel(logging.INFO)


# Pull all records, autolimit = 100
url = 'https://data.illinois.gov/api/3/action/datastore_search?resource_id=5f783951-5f80-4fdd-8b5c-7c434933d7e3&limit=1000'

#IMPLEMENT THESE FUNCTIONS
class client:
    def __init__(self, override=False):
        # you add class variables here like
        # self.myvar="the greatest variable ever. the best"

        self.conn = None
        try:
            with open('secrets.json') as f:
                env = json.load(f)
            self.dbname=env['DBNAME']
            self.dbhost=env['DBHOST']
            self.dbport=env['DBPORT']
            self.dbusername=env['DBUSERNAME']
            self.dbpasswd=env['DBPASSWD']
        except:
            pass


    def openConnection(self):
        """Open a connection to a psql database, using the self.dbXX parameters."""
        self.conn = psycopg2.connect(database=self.dbname,
                                user=self.dbusername,
                                password=self.dbpasswd,
                                host=self.dbhost,
                                port=self.dbport)
        self.conn.set_session(autocommit=True)

        return True

    def closeConnection(self):
        """Close any active connection(should be able to handle closing a closed conn)."""
        #logger.debug("Closing Connection")
        self.conn.close()
        return True

    def write(self, statements, values=None):
        """Execute statements, close the cursor on exit (write-only)."""
        if self.conn is None:
            self.openConnection()

        with self.conn.cursor() as cur:
            for statement in statements:
                    cur.execute(statement)

    def read(self, statement, args):
        """Execute statement, fetchall returned rows."""
        if self.conn is None:
            self.openConnection()

        l = []
        with self.conn.cursor() as cur:
            #logger.debug(statement)
            #logger.debug(args)
            cur.execute(statement, args)
            l = cur.fetchall()
        return l

    def createTables(self):
        """Create any tables needed by this client. Drop if exists first."""
        #logger.debug("Creating Tables")

        DROP_DISPATCH = "DROP TABLE IF EXISTS dispatch;"

        CREATE_TABLE_DISPATCH = """ CREATE TABLE dispatch 
			(_id INT,
			event_number VARCHAR(20) NOT NULL PRIMARY KEY,
			street_num INT,
			street_name VARCHAR(40),
			full_address VARCHAR(60),
			city VARCHAR(10),
			reporting_district VARCHAR(6),
			dispatch_date_time TIMESTAMP,
			dispatch_date DATE,
			dispatch_time TIME,
			incident_type VARCHAR(100),
			lat FLOAT,
			lon FLOAT
			);
			"""

        self.write([
                    DROP_DISPATCH,
                    CREATE_TABLE_DISPATCH
                    ])

        return True

    def addGeoExtensions(self):
    	
        CREATE_EXT_POSTGIS = """CREATE EXTENSION IF NOT EXISTS postgis;"""
        CREATE_EXT_FUZZY = """create extension IF NOT EXISTS fuzzystrmatch;"""
        CREATE_EXT_TIGER = """create extension IF NOT EXISTS postgis_tiger_geocoder;"""
        CREATE_EXT_POSTGIS_TOP = """create extension IF NOT EXISTS postgis_topology;"""

        self.write([
            CREATE_EXT_POSTGIS,
            CREATE_EXT_FUZZY,
            CREATE_EXT_TIGER,
            CREATE_EXT_POSTGIS_TOP
            ])

        return True

    def removeGeoExtensions(self):

        DROP_EXT_POSTGIS = """DROP EXTENSION IF EXISTS postgis CASCADE;"""
        DROP_EXT_FUZZY = """DROP extension IF EXISTS fuzzystrmatch CASCADE;"""
        DROP_EXT_TIGER = """DROP extension IF EXISTS postgis_tiger_geocoder CASCADE;"""
        DROP_EXT_POSTGIS_TOP = """DROP extension IF EXISTS postgis_topology CASCADE;"""

        self.write([
            DROP_EXT_POSTGIS,
            DROP_EXT_FUZZY,
            DROP_EXT_TIGER,
            DROP_EXT_POSTGIS_TOP
            ])   	

        return True

    def addIndexes(self):
        """Add at least two indexes to the tables to improve analytic queries."""
        """
        logger.debug("Adding Indexes")

        idx_member_birth_year = "CREATE INDEX idx_member_birth_year ON trip(member_birth_year);"
        idx_timeframe = "CREATE INDEX idx_timeframe ON trip(start_ts, end_ts);"
        idx_duration = "CREATE INDEX idx_duration ON trip(duration);"

        self.write([
                    idx_member_birth_year,
                    idx_timeframe,
                    idx_duration
                    ])
        """
        pass


    def loadAll(self, url):
        fileobj = urllib.request.urlopen(url)
        data=json.load(fileobj) 

        records = data['result']['records']

        for record in records:
            self.loadRecord(record)

        return True

    def loadMostRecent(self,url):
        sql = 'select max(dispatch_date_time) from dispatch;'
        pass

    def loadRecord(self, entry):
        logger.debug(entry)

        # Process JSON records
        _id = entry['_id']
        event_num = "'" + entry['Event_Number'].strip() + "'"
        street_num = entry['StreetNum'].strip()
        street_name = "'" + entry['StreetName'].strip() + "'"

        if '&' in entry['Full_Address']:
        	lst = entry['Full_Address'].strip().split('&')
        	addy_geocode = ' %26 '.join(lst)
        	full_address = "'" + ' & '.join(lst) + "'"
        else: 
        	addy_geocode = entry['Full_Address'].strip()
        	full_address = "'" + addy_geocode + "'"
        
        city = "'" + entry['City'].strip() + "'"
        
        if entry['Reporting_District'] is None:
        	district = 'NULL'
        else: 
        	district = "'" + entry['Reporting_District'].strip() + "'"
        
        date_time = "'" + entry['Dispatch_Date_Time'].strip() + "'"
        dispatch_date = "'" + entry['Dispatch_Date'].strip() + "'"
        dispatch_time = "'" + entry['Dispatch_Time'].strip() + "'"
        incident_type = "'" + entry['Incident_Type_Desc_Display'].strip() + "'"
        #print(addy_geocode, full_address,city)

        lat, lon = geo_output = self.getLatLong(addy_geocode, entry['City'].strip())
        #print(lat, lon)

        dispatch_insert = """
        INSERT INTO dispatch (_id, event_number, street_num, street_name, full_address, city, reporting_district, dispatch_date_time,
            dispatch_date, dispatch_time, incident_type, lat, lon)
        VALUES ({}, {}, NULLIF('{}','')::integer, {}, {}, {}, {}, {}, {}, TIME WITHOUT TIME ZONE {}, {}, {}, {})
        ON CONFLICT DO NOTHING;
        """.format(_id, event_num, street_num, street_name, full_address, city, district, date_time, dispatch_date, dispatch_time, incident_type, lat, lon)

        self.write([dispatch_insert])

        return True

    def getLatLong(self, address, city):
        urlbase = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address='
        output_format = '&benchmark=9&format=json'
        url_address = address + ', ' + city + ' IL' 
    	
        full_url = urlbase+url_address+output_format
        #print(full_url)
        fileobj = urllib.request.urlopen(full_url)
        data=json.load(fileobj)

        if len(data['result']['addressMatches']) > 0:
            lon = data['result']['addressMatches'][0]['coordinates']['x']
            lat = data['result']['addressMatches'][0]['coordinates']['y']
            return lat, lon
        else:
        	return 'NULL', 'NULL'








