#Server=localhost\SQLEXPRESS;Database=master;Trusted_Connection=True;
#api key redacted 

from __future__ import print_function
import time
import weatherapi
from weatherapi.rest import ApiException
from pprint import pprint
import pyodbc
import pandas as pd
import logging

df = pd.DataFrame()
zips = ["77449","11368","60629","79936","90011","11385","90650","91331","77084","73034"]

logger = logging.getLogger(__name__)
logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)

# Configure API key authorization: ApiKeyAuth
configuration = weatherapi.Configuration()
configuration.api_key['key'] = 'redacted'

# create an instance of the API class
api_instance = weatherapi.APIsApi(weatherapi.ApiClient(configuration))

for z in zips:
    q = z # str | Pass US Zipcode, UK Postcode, Canada Postalcode, IP address, Latitude/Longitude (decimal degree) or city name. Visit [request parameter section](https://www.weatherapi.com/docs/#intro-request) to learn more.
    #dt = '2025-05-03' # date | Date on or after 1st Jan, 2015 in yyyy-MM-dd format

    try:
        #api_response = api_instance.forecast_weather(q, dt)
        api_response = api_instance.realtime_weather(q)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling APIsApi->weather: %s\n" % e)
        logger.debug("Exception when calling APIsApi->weather: %s\n" % e)
    else:
        logger.debug("API called successfully for zip %s" % z)
    
    ''' Example log output for each loop
    DEBUG:__main__:API called successfully for zip 77449
    DEBUG:__main__:API called successfully for zip 11368
    DEBUG:__main__:API called successfully for zip 60629
    DEBUG:__main__:API called successfully for zip 79936
    DEBUG:__main__:API called successfully for zip 90011
    DEBUG:__main__:API called successfully for zip 11385
    DEBUG:__main__:API called successfully for zip 90650
    DEBUG:__main__:API called successfully for zip 91331
    DEBUG:__main__:API called successfully for zip 77084
    DEBUG:__main__:API called successfully for zip 73034
    '''

    ''' Results from realtime_weather API call in JSON format
    {'current': {'cloud': 0,
                'condition': {'code': 1000,
                            'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png',
                            'text': 'Clear'},
                'dewpoint_c': 9.4,
                'dewpoint_f': 48.9,
                'feelslike_c': 12.0,
                'feelslike_f': 53.6,
                'gust_kph': 20.4,
                'gust_mph': 12.7,
                'heatindex_c': 13.2,
                'heatindex_f': 55.8,
                'humidity': 80,
                'is_day': 0,
                'last_updated': '2025-05-04 00:45',
                'last_updated_epoch': 1746337500,
                'precip_in': 0.0,
                'precip_mm': 0.0,
                'pressure_in': 30.02,
                'pressure_mb': 1017.0,
                'temp_c': 12.8,
                'temp_f': 55.0,
                'uv': 0.0,
                'vis_km': 16.0,
                'vis_miles': 9.0,
                'wind_degree': 169,
                'wind_dir': 'S',
                'wind_kph': 9.7,
                'wind_mph': 6.0,
                'windchill_c': 13.1,
                'windchill_f': 55.6},
    'location': {'country': 'USA',
                'lat': 35.6665000915527,
                'localtime': '2025-05-04 00:57',
                'localtime_epoch': 1746338261,
                'lon': -97.4797973632813,
                'name': 'Edmond',
                'region': 'Oklahoma',
                'tz_id': 'America/Chicago'}}
    '''

    #normalize the json data into a flat table
    dftemp = pd.json_normalize(api_response)
    #append each individual zip API call to one dataframe
    df = pd.concat([dftemp, df])

#print(df)
#only selecting four columns of data
df = df[['location.name', 'location.region', 'location.localtime', 'current.temp_f']]

''' reduced data set in flat table format
     location.name location.region location.localtime  current.temp_f
0        Edmond        Oklahoma   2025-05-04 01:04            55.0
'''

#rename columns in dataframe to load into MSSQL database more easily
df.rename(columns={'location.name': 'city', 'location.region': 'region', 'location.localtime': 'localtime', 'current.temp_f': 'temp_f'}, inplace=True)
print(df)

''' renamed flat table output
     city    region         localtime  temp_f
0  Edmond  Oklahoma  2025-05-04 01:35    54.0
'''

#mssql connection credentials, adding trustcertificate = yes in order to be able to connect on my local
SERVER = 'localhost\SQLEXPRESS'
DATABASE = 'WEATHERDB'
USERNAME = 'test_admin'
PASSWORD = 'password'
TRUSTCERT = 'yes'

connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate={TRUSTCERT}'

conn = pyodbc.connect(connectionString) 

#since working with a small data set, doing a flush/fill process.  If working with a larger data set, would change to an incremental load
SQL_STATEMENT = """
DROP TABLE IF EXISTS dbo.CurrentWeather
CREATE TABLE dbo.CurrentWeather (
    city NVARCHAR(100) PRIMARY KEY,
    region NVARCHAR(100),
    localtime DATETIME,
    temp_f FLOAT
    )
"""

cursor = conn.cursor()

cursor.execute(SQL_STATEMENT)
conn.commit()

#inserting each row into the table through iterating each row.  If working with a larger data, would change to a bulk insert process using sqlalchemy and df.to_sql method
for index,row in df.iterrows():
    print(row)
    cursor.execute("INSERT INTO dbo.CurrentWeather (city,region,localtime,temp_f) values(?,?,?,?)", row.city, row.region, row.localtime, row.temp_f)
conn.commit()
cursor.close()
conn.close()

''' Final table output in MSSQL
city	region	localtime	temp_f
Chicago	Illinois	2025-05-04 01:54:00.000	46.9
Corona	New York	2025-05-04 02:54:00.000	64.9
Edmond	Oklahoma	2025-05-04 01:54:00.000	54
El Paso	Texas	2025-05-04 00:54:00.000	70
Houston	Texas	2025-05-04 01:54:00.000	62.1
Katy	Texas	2025-05-04 01:54:00.000	57.7
Los Angeles	California	2025-05-03 23:54:00.000	59.4
Norwalk	California	2025-05-03 23:54:00.000	59.2
Pacoima	California	2025-05-03 23:54:00.000	57
Ridgewood	New York	2025-05-04 02:54:00.000	63
'''