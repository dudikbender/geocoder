# Import standard Python packages
import pandas as pd
import numpy as np
import requests

# Geographic packages
import geopy
import shapely
from shapely.geometry import Point, Polygon
from shapely.ops import transform
#from pyproj import Proj, transform
#import pyproj
from functools import partial

# Import utilities
from os import environ
from dotenv import find_dotenv, load_dotenv

# SQLite database import
from utils import connection

env_loc = find_dotenv('.env')
load_dotenv(env_loc)

google_api_key = environ.get('GOOGLE_API_KEY')

# Filepath references for shapefiles
wards = 'data/shapefiles/wards/Wards__December_2017__Boundaries_in_GB.shp'
local_authorities = 'data/shapefiles/local_authorities/Local_Authority_Districts__May_2020__Boundaries_UK_BFC.shp'
constituencies = 'data/shapefiles/constituencies/Westminster_Parliamentary_Constituencies__December_2019__Boundaries_UK_BFC.shp'

class GeocoderUtilities():
    def __init__(self):
        pass

    def google_geocoding(self, address: str, api_key: str, region:str = 'uk'):
        # Google Geocoding API request
        url = f'https://maps.googleapis.com/maps/api/geocode/json'

        # Set parameters, including Google API key
        parameters = {'key':api_key,
                      'address':address}
        # Make request
        response = requests.get(url, params=parameters).json()['results'][0]

        formatted_response = {'formatted_address':response['formatted_address'],
                              'postcode':response['address_components'][-1]['long_name'],
                              'latitude':response['geometry']['location']['lat'],
                              'longitude':response['geometry']['location']['lng']}

        return formatted_response

    def point_buffer_kms(self, latitude: float, longitude: float, kilometres: float):
        # Azimuthal equidistant projection
        aeqd_proj = '+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'
        project = partial(
            pyproj.transform,
            pyproj.Proj(aeqd_proj.format(lat=latitude, lon=longitude)),
            self.proj_wgs84)
        buf = Point(0, 0).buffer(kilometres * 1000)  # distance in metres
        poly = Polygon(transform(project, buf).exterior.coords[:])
        # Returns a shapely Polygon object
        return poly

class GeoCoderXL():
    def __init__(self, google_api_key):
        self.google_api_key = google_api_key
        self.utilities = GeocoderUtilities()
        return

    def postcode_query(self, postcode):
        cur = connection.cursor()
        query_string = '''SELECT postcode, latitude, longitude, easting, northing, ward_name, lad_name
                        FROM Postcode_coordinates as postcode
                        INNER JOIN UK_Wards as wards  ON wards.code = postcode.ward
                        INNER JOIN UK_Local_Authorities as lads ON lads.code = postcode.local_authority 
                        WHERE postcode.postcode == ? '''
        
        cur.execute(query_string, [postcode])
        result = cur.fetchone()
        
        return result

    def google_geocoding(self, address: str, api_key: str, region:str = 'uk'):
        # Make a request to the Google Geocoding API
        google = self.utilities.google_geocoding(address, api_key, region)

        # Query the database for details about the postcode from the address
        cur = connection.cursor()
        query_string = '''SELECT postcode, ward_name, lad_name, Total as ward_total_pop_2018, Median as ward_median_pop_2018,
                        SUM("2030") as local_authority_proj_pop_2030
                        FROM Postcode_coordinates as postcode
                        INNER JOIN UK_Wards as wards ON wards.code = postcode.ward
                        INNER JOIN UK_Local_Authorities as lads ON lads.code = postcode.local_authority 
                        INNER JOIN uk_ward_population_2018 as ward_pop ON wards.code = ward_pop.code
                        INNER JOIN local_authority_population_projections as lad_pop ON lads.code = lad_pop.code
                        WHERE postcode.postcode == ? '''
        
        cur.execute(query_string, [google['postcode']])
        result = cur.fetchone()
        result['ward'] = result.pop('ward_name')
        result['local_authority'] = result.pop('lad_name')

        # Add the Google formatted address, latitude, and longitude
        result['formatted_address'] = google['formatted_address']
        result['latitude'] = google['latitude']
        result['longitude'] = google['longitude']
        
        return result

    def radius_query(self, postcode: str, radius: int = 35):
        # Submit postcode to Google Maps Geocoding API
        pass