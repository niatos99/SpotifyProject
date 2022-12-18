from ast import List
from cgi import test
from sqlite3.dbapi2 import Connection
import string
from tokenize import String
from tracemalloc import start
from typing import Dict
from xmlrpc.client import DateTime
from numpy import empty, errstate, subtract
from pandas.core.arrays import boolean
from pandas.core.frame import DataFrame
import pandas as pd
from sqlalchemy import column 
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime
import sys
import pyodbc
import sqlalchemy
import sqlite3
import maskpass

## Token til post metoden
TOKEN = "INSERT TOKEN"
## FOR XAMPLE #"eyJhiOiJIUzI1DNiIsInR5cCI6IkpXVCJ9.eyJ0b2tlblR5cGUiOiJDdXN0b21lckFQSV9SZWZyZXNoIiwidG9rZW5pZCI6Ijc1NDQ1ZTlkLTM0N2UtNGIzMy1hZDY0LTlkNDQzMDNlOTYxNyIsIndlYkFwcCI6WyJDdXN0b21lckFwaSIsIkN1c3RvbWVyQXBwQXBpIl0sImp0aSI6Ijc1NDQ1ZTlkLTM0N2UtNGIzMy1hZDY0LTlkNDQzMDNlOTYxNyIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL25hbWVpZGVudGlmaWVyIjoiUElEOjkyMDgtMjAwMi0yLTU3MTI3MTY1MTgwOCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6Ik5pYXQgVG9sb3UgQW1hbiIsImxvZ2luVHlwZSI6IktleUNhcmQiLCJwaWQiOiI5MjA4LTIwMDItMi01NzEyNzE2NTE4MDgiLCJ0eXAiOiJQT0NFUyIsInVzZXJJZCI6IjgyNDI2IiwiZXhwIjoxNjgxODkyODA0LCJpc3MiOiJFbmVyZ2luZXQiLCJ0b2tlbk5hbWUiOiJIZXJsZXYiLCJhdWQiOiJFbmVyZ2luZXQifQ.4EhX_jAEplfONLgISWOVx1s9BYbp8VQ3UBFM7LUoqIk" #
yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
TwoYearsAgo = datetime.datetime.today() - datetime.timedelta(days=730)

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
    
    # Vi returnerer "Value"/token delen af json-formatet eks. {"result": "LANG TOKEN STRENG"}
    datatoken = r["result"]
    # ## Nu skal vi bruge short-lived token til at lave en POST-method og returnerer vores værdier  
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer "+ datatoken
    }

    # Post metoden kræver målepunktsnummeret
    data = """{"meteringPoints": {"meteringPoint": ["571313179100713311"]}}"""
    post_url = "https://api.eloverblik.dk/CustomerApi/api/MeterData/GetTimeSeries/ "+TwoYearsAgo.strftime("%Y-%m-%d")+" / "+yesterday.strftime("%Y-%m-%d")+"/Hour"
    req = requests.post(post_url, headers=headers, data=data).json()
    json_str = json.dumps(req)
    resp = json.loads(json_str)
    # vi får det ud i en dataframe og extracter json værdier, som vi skal bruge ud
    df = pd.json_normalize(resp,
    record_path=['result', 'MyEnergyData_MarketDocument', 'TimeSeries','Period', 'Point'], 
    meta=[
        ['result', 'MyEnergyData_MarketDocument', 'TimeSeries','Period', 'timeInterval', 'start'], 
        ['result', 'MyEnergyData_MarketDocument', 'TimeSeries','Period', 'timeInterval', 'end']],
        errors='ignore')

    #TRANSFORM

    #1 Rename Columns(OldColumnName: NewColumnName)
    df_rename = df.rename(columns={
    "result.MyEnergyData_MarketDocument.TimeSeries.Period.timeInterval.start": "StartTime", 
    "result.MyEnergyData_MarketDocument.TimeSeries.Period.timeInterval.end": "EndTime",
    "out_Quantity.quantity": "kWh",
    "out_Quantity.quality":"Quality",
    "position": "Position"})

    #2 Change datatype and formats
    df_rename['Position'] = df_rename['Position'].astype('int')
    df_rename['StartTime'] = pd.to_datetime(df_rename['StartTime'], format='%Y-%m-%dT%H:%M:%SZ')
    df_rename['EndTime'] = pd.to_datetime(df_rename['EndTime'], format='%Y-%m-%dT%H:%M:%SZ')
    df_rename['Quality'] = df_rename['Quality'].astype('str')
    df_rename['kWh'] = df_rename['kWh'].astype('float')

    # 3 Data manipulation
    #1... simpel if statement virkede ikke ordentligt til dataframe - derfor brugen af df.LOC
    #2 GroupBy er brugt, da der til vintertids-skift kommer en ekstra række nr.25, som vi så samler/adderer til eksisterende række kl. 03:00, altså Position 3. række nr 25 er egentlig en dublet af position 3. derfor laver vi 25 om til 3, så vores groupby virker bedre
    df = pd.DataFrame(df_rename)
    #laver den ekstra række Position 25, som kreeres ved vintertid-skift om til Position 3, således vi kan addere denne på den eksisterende 3
    df.loc[df['Position'] == 25, 'Position'] = 3
    #vi summerer de to rækker (Position 3), da vi ellers vil have 25 rækker/målinger/timer på en enkelt dag.
    df['kWh'] = df.groupby(['Position','Quality', 'StartTime', 'EndTime'])['kWh'].transform('sum')
    # Efter vi har summeret de to rækker, vil vi gerne fjerne den ene af dem, så der ikke er dubletter.
    df = df.drop_duplicates()
    df.loc[df['StartTime'].dt.hour == 22, 'StartTime'] = df['StartTime'] + pd.to_timedelta(2, unit='h') + pd.to_timedelta(df['Position'], unit='h') - pd.to_timedelta(1, unit='h')
    df.loc[df['StartTime'].dt.hour == 23, 'StartTime'] = df['StartTime'] + pd.to_timedelta(1, unit='h') + pd.to_timedelta(df['Position'], unit='h') - pd.to_timedelta(1, unit='h')

    ##df.loc[df['Position'] == 25, 'Position'] = 3
    ##df.loc[df['Position'] == 3 and df['StartTime'].dst() , 'Position'] = 3
    df.to_excel(r'C:\Users\w31610\Desktop\ProjectTest\exceltest\excelftw.xlsx', sheet_name='Sheet', index=False)
    print(df)
    
serv = ""
usr = ""
passwd = maskpass.advpass()
db = ""
prt = ""
drver="Adaptive Server Enterprise"

conn = pyodbc.connect(driver=drver, server=serv, database=db,port = prt, uid=usr, pwd=passwd, autocommit=True)
sql = "INSERT INTO Fact_el (Position, StartTime, EndTime, Quality, kWh) VALUES (?,?,?,?,?)"
cursor = conn.cursor()
for index, row in df.iterrows():
    cursor.execute("INSERT INTO Fact_el (Position, StartTime, EndTime, Quality, kWh) VALUES (?,?,?,?,?)", row.Position, row.StartTime, row.EndTime, row.Quality, row.kWh )
    conn.commit()

        ##### Insert Dataframe into SQL Server:
# for index, row in df.iterrows():
#      cursor.execute("INSERT INTO ZZZ (DepartmentID,Name,GroupName) values(?,?,?)", row.DepartmentID, row.Name, row.GroupName)