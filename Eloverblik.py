from sqlite3.dbapi2 import Connection
from numpy import empty, errstate
from pandas.core.arrays import boolean
from pandas.core.frame import DataFrame
import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import sys

## Token til post metoden
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlblR5cGUiOiJDdXN0b21lckFQSV9SZWZyZXNoIiwidG9rZW5pZCI6IjNhYWY3NGFjLWU4ZDMtNDBhYi1hOTljLTFjNjI5ODc4MzI1MSIsImp0aSI6IjNhYWY3NGFjLWU4ZDMtNDBhYi1hOTljLTFjNjI5ODc4MzI1MSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL25hbWVpZGVudGlmaWVyIjoiUElEOjkyMDgtMjAwMi0yLTU3MTI3MTY1MTgwOCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6Ik5pYXQgVG9sb3UgQW1hbiIsImxvZ2luVHlwZSI6IktleUNhcmQiLCJwaWQiOiI5MjA4LTIwMDItMi01NzEyNzE2NTE4MDgiLCJ0eXAiOiJQT0NFUyIsImV4cCI6MTY0MzQ0MjgxNiwiaXNzIjoiRW5lcmdpbmV0IiwidG9rZW5OYW1lIjoiYXBpX2hqZW0iLCJhdWQiOiJFbmVyZ2luZXQifQ.8rNeV9Mevg8embRNYxrL20TcS-8mCFUWhsa5yRqjliw" # your Spotify API token

if __name__ == "__main__":

    # Extract part of the ETL process
    # Headers
    headers_datatoken = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    # Requesting short-lived token in json-format
    get_url = "https://api.eloverblik.dk/CustomerApi/api/token"
    r = requests.get(get_url, headers=headers_datatoken).json()
    
    # Vi returnerer "Value" delen af json-formatet eks. {"result": "LANG TOKEN STRENG"}
    datatoken = r["result"]
    ## Done.

## Nu skal vi bruge short-lived token til at lave en POST-method og returnerer vores værdier  
    # Header
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer "+ datatoken
    }

    # Post metoden kræver målepunktsnummeret
    data = """{"meteringPoints": {"meteringPoint": ["571313174113669471"]}}"""
    post_url = "https://api.eloverblik.dk/CustomerApi/api/MeterData/GetTimeSeries/2016-01-01/2021-10-16/Hour"
    req = requests.post(post_url, headers=headers, data=data).json()
    
    print(req)