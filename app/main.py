from fastapi import FastAPI
from models import GeoCoderXL
from typing import Optional
from utils import PostcodeResult, GoogleResult
from os import environ
from dotenv import find_dotenv, load_dotenv

env_loc = find_dotenv('.env')
load_dotenv(env_loc)

geocoder = GeoCoderXL(google_api_key=environ.get('GOOGLE_API_KEY'))
google_api_key = geocoder.google_api_key

google_api_key = environ.get('GOOGLE_API_KEY')

app = FastAPI(title='Geocoder2',
              description='Expanded geocoding cabilities, including demographic, economic, and political data',
              version=0.1)

@app.get('/q', response_model=GoogleResult)
async def google_geocode(address, api_key: str = google_api_key, region: Optional[str] = 'uk'):
    response = geocoder.google_geocoding(address=address, api_key=api_key, region=region)
    return GoogleResult(**response)