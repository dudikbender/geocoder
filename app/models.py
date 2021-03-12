# Import standard Python packages
import pandas as pd
import numpy as np
import requests
from pandas import json_normalize

# Geographic packages
import geopy
import shapely
from shapely.geometry import Point, Polygon
from shapely.ops import transform as shapely_transform
import pyproj
from functools import partial
import geopandas as gpd

# Import utilities
from os import environ
from dotenv import find_dotenv, load_dotenv

# SQLite database import
from .utils import connection

env_loc = find_dotenv('.env')
load_dotenv(env_loc)

google_api_key = environ.get('GOOGLE_API_KEY')

# Filepath references for shapefiles
wards = 'data/shapefiles/wards/Wards__December_2017__Boundaries_in_GB.shp'
local_authorities = 'data/shapefiles/local_authorities/Local_Authority_Districts__May_2020__Boundaries_UK_BFC.shp'
constituencies = 'data/shapefiles/constituencies/Westminster_Parliamentary_Constituencies__December_2019__Boundaries_UK_BFC.shp'

# Estable database connection
cursor = connection.cursor()
def sql_to_df(table_name: str, connection: object):
        request = connection.execute(f'SELECT * FROM {table_name}')
        rows = request.fetchall()
        df = json_normalize(rows)
        return df

# Ward shapefile converted into GeoDataFrames
ward_df = geo.sql_to_df('UK_Wards_Population', cur)
ward_shp = gpd.read_file(wards).to_crs(epsg=4326)
wards_gdf = ward_df.merge(ward_shp, left_on='Code', right_on='wd17cd', how='left')

# Prices Paid dataset, converted into GeoDataFrame
sales_df = sql_to_df('property_prices_2019', cur)
sales_gdf = gpd.GeoDataFrame(sales_df, geometry=gpd.points_from_xy(sales_df.longitude,
                                                                   sales_df.latitude)).set_crs(epsg=4326)

class GeocoderUtilities():
    def __init__(self):
        pass

    def address_geocoded(self, address: str, api_key: str, region:str = 'uk'):
        # Google Geocoding API request
        url = f'https://maps.googleapis.com/maps/api/geocode/json'

        # Set parameters, including Google API key
        parameters = {'key':api_key,
                      'address':address,
                      'region':region}
        # Make request
        response = requests.get(url, params=parameters).json()['results'][0]

        formatted_response = {'formatted_address':response['formatted_address'],
                              'postcode':response['address_components'][-1]['long_name'],
                              'latitude':response['geometry']['location']['lat'],
                              'longitude':response['geometry']['location']['lng']}

        return formatted_response

    def point_buffer(self, lat: float, lng: float, kilometres: float, initial_epsg: int = 4326):
        aeqd_projection = f'+proj=aeqd +lat_0={lat} +lon_0={lng} +x_0=0 +y_0=0'
        beginning_projection = pyproj.CRS.from_epsg(initial_epsg)
        
        transformed_projection = partial(pyproj.transform, 
                                pyproj.Proj(aeqd_projection),
                                beginning_projection)
        
        buffer_point = Point(0, 0).buffer(kilometres * 1000) # converts distance to metres
        buffer_polygon = Polygon(shapely_transform(transformed_projection, buffer_point).exterior.coords[:])
        return buffer_polygon

    def address_to_buffer(self, address, buffer_kilometres, api_key, region: str = 'uk',
                            initial_epsg: int = 4326):
        geocoded_response = self.address_geocoded(address=address,
                                                  api_key=api_key, 
                                                  region=region)

        lat, lng = geocoded_response['latitude'], geocoded_response['longitude']
        buffer_polygon = self.point_buffer(lat=lat, lng=lng, kilometres=buffer_kilometres,
                                           initial_epsg=initial_epsg)
        return buffer_polygon

    def convert_df_to_geodf(self, dataframe: object, address_column: str, api_key: str,
                            crs: int = 4326):
        coordinates = []
        lats = []
        lngs = []
        for index, row in dataframe.iterrows():
            address = row[address_column]
            try:
                location = self.address_geocoded(address, api_key)
                latitude = location['latitude']
                longitude = location['longitude']
            except:
                latitude = 0
                longitude = 0

            lats.append(latitude)
            lngs.append(longitude)
            coordinates.append((latitude, longitude))

        dataframe['coordinates'] = coordinates
        dataframe['latitude'] = lats
        dataframe['longitude'] = lngs

        geodf = gpd.GeoDataFrame(dataframe, geometry=gpd.points_from_xy(dataframe.latitude,
                                                                        dataframe.longitude))
        return geodf.set_crs(epsg=crs)

    def create_buffer(self, coordinates, distance_kms):
        '''Takes a list of coordinate tuples (lat, lng) and returns list of Polygon 
        geometries'''
        buffer_geometry = [ self.point_buffer(x[0], x[1], distance_kms) for x in coordinates ]
        return buffer_geometry

    def df_to_geodf_with_buffer(self, dataframe: object, address_column: str, api_key: str,
                                buffer_kilometres: float, crs: int = 4326):

        gdf = self.convert_df_to_geodf(dataframe=dataframe,address_column=address_column,
                                       api_key=api_key, crs=crs)

        gdf['geometry'] = self.create_buffer(coordinates=gdf['coordinates'], 
                                               distance_kms=buffer_kilometres)
        return gdf

    def df_merge_with_sales(self, dataframe: object, address_column: str, api_key: str, 
                            buffer_kilometres: float, region: str = 'uk',
                            agg_function_list: str = 'sum,mean,median,count'):
    
        buffer_df = self.df_to_geodf_with_buffer(dataframe, address_column, api_key, buffer_kilometres, region)
        sales_in_buffer = gpd.sjoin(buffer_df, sales_gdf, how='left', op='intersects')
        full_gdf = sales_in_buffer\
                .groupby('location')\
                .agg({'AMOUNT':agg_function_list.split(',')})\
                .reset_index()\
                .dropna()
        
        return full_gdf

    def df_merge_with_wards(self, dataframe: object, address_column: str, api_key: str, 
                            buffer_kilometres: float, region: str = 'uk'):
    
        buffer_df = self.df_to_geodf_with_buffer(dataframe, address_column, api_key, buffer_kilometres, region)
        wards_in_buffer = gpd.sjoin(buffer_df, wards_gdf, how='left', op='intersects')
        full_gdf = sales_in_buffer\
                .groupby('location')\
                .agg({'Total population':['sum'], 'Median age':['median']}\
                .reset_index()\
                .dropna()
        
        return full_gdf
        
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