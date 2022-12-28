

#                ETL MED ENERGINETS API OG MIT ELFORBRUG HJEMME            #


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
from dateutil.rrule import rrule, MONTHLY, SU

# Token til post metoden
TOKEN = ""
#"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlblR5cGUiOiJDdXN0b21lckFQSV9SZWZyZXNoIiwidG9rZW5pZCI6Ijc1NDQ1ZTlkLTM0N2UtNGIzMy1hZDY0LTlkNDQzMDNlOTYxNyIsIndlYkFwcCI6WyJDdXN0b21lckFwaSIsIkN1c3RvbWVyQXBwQXBpIl0sImp0aSI6Ijc1NDQ1ZTlkLTM0N2UtNGIzMy1hZDY0LTlkNDQzMDNlOTYxNyIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL25hbWVpZGVudGlmaWVyIjoiUElEOjkyMDgtMjAwMi0yLTU3MTI3MTY1MTgwOCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6Ik5pYXQgVG9sb3UgQW1hbiIsImxvZ2luVHlwZSI6IktleUNhcmQiLCJwaWQiOiI5MjA4LTIwMDItMi01NzEyNzE2NTE4MDgiLCJ0eXAiOiJQT0NFUyIsInVzZXJJZCI6IjgyNDI2IiwiZXhwIjoxNjgxODkyODA0LCJpc3MiOiJFbmVyZ2luZXQiLCJ0b2tlbk5hbWUiOiJIZXJsZXYiLCJhdWQiOiJFbmVyZ2luZXQifQ.4EhX_jAEplfONLgISWOVx1s9BYbp8VQ3UBFM7LUoqIk" #
MeteringPoint = ''

# Sybase ASE
serv = ""
usr = ""
db = ""
prt = ""
drver="Adaptive Server Enterprise"

#Energinet giver kun data for de sidste 730 dage.
yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
TwoYearsAgo = datetime.datetime.today() - datetime.timedelta(days=730)

if __name__ == "__main__":
    #           EXTRACT          EXTRACT          EXTRACT          EXTRACT          EXTRACT
    # Headers
    headers_datatoken = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    # Requester short-lived token i json-format.
    get_url = "https://api.eloverblik.dk/CustomerApi/api/token"
    r = requests.get(get_url, headers=headers_datatoken).json()
    
    # Vi returnerer en short-lived LANG TOKEN STRENG fra json eks. {"result": "LANG TOKEN STRENG"}
    datatoken = r["result"]
    # Nu skal vi bruge short-lived token til at lave en POST-method og returnerer vores værdier  
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer "+ datatoken
    }
    # Post metoden kræver målepunktsnummeret
    # Var nødt til at double up på curly bracket før den forstod det var en string og kunne læse variablen
    data = f'{{"meteringPoints": {{"meteringPoint": ["{MeteringPoint}"]}}}}'

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

    #       TRANSFORM      TRANSFORM      TRANSFORM      TRANSFORM      TRANSFORM      TRANSFORM      TRANSFORM

    #1 Rename Columns(OldColumnName: NewColumnName)
    df = df.rename(columns={
    "result.MyEnergyData_MarketDocument.TimeSeries.Period.timeInterval.start": "StartTime", 
    "result.MyEnergyData_MarketDocument.TimeSeries.Period.timeInterval.end": "EndTime",
    "out_Quantity.quantity": "kWh",
    "out_Quantity.quality":"Quality",
    "position": "Position"})

    #2 Change datatype and formats
    df['Position'] = df['Position'].astype('int')
    df['StartTime'] = pd.to_datetime(df['StartTime'], format='%Y-%m-%dT%H:%M:%SZ')
    df['EndTime'] = pd.to_datetime(df['EndTime'], format='%Y-%m-%dT%H:%M:%SZ')
    df['Quality'] = df['Quality'].astype('str')
    df['kWh'] = df['kWh'].astype('float')

    # 3 Data manipulation
    df = pd.DataFrame(df)
    #       VINTERTID
    # Energinet laver en ekstra række når tiden bliver sat en time tilbage ved vintertidsdatoen: dvs 25 timer på et døgn
    # Ifølge Energinet skal den ekstra række (Position 25) kl. 25:00:00 adderes på tidspunkt 03:00:00.
    # derfor ændrer vi (Position 25) ---> Position 3 så vi nemmere kan lave en GROUP BY og summere de to rækker
    df.loc[df['Position'] == 25, 'Position'] = 3
    # Vi laver group by så vi kan summe de to værdier med hinanden med hinanden
    df['kWh'] = df.groupby(['Position','Quality', 'StartTime', 'EndTime'])['kWh'].transform('sum')
    # Efter vi har summeret de to rækker, vil vi gerne fjerne den ene af dem, så der ikke er dubletter.
    df = df.drop_duplicates()

    #Sommertid
    # Finder sommertidsskift-datoen i marts (sidste søndag i Marts)
    last_sunday_march = list(rrule(MONTHLY, bymonth=3, byweekday=SU, dtstart=df['EndTime'].min(), until=df['EndTime'].max()))[-1]
    # Ved sommertidsskift mister vi en hel time af døgnet på den specifikke dag og derfor springer vi kl 02:00:00 over ved at plusse med 1
    df.loc[(df['EndTime'] == last_sunday_march) & (df['Position'] > 2), 'Position'] = df['Position'] + 1
    
    # minusser med -1 således at udgangspunktet Position 1 bliver til 0, som er kl. 00:00:00, så vi letter kan konvertere 0 --> 00:00:00 og 1 -->01:00:00
    df['DateTime'] = pd.to_datetime(df['Position']-1, unit='h').dt.time
    df['EndTime'] = pd.to_datetime(df['EndTime'], format='%Y-%m-%dT%H:%M:%SZ').dt.date
    # laver en ny frisk datetime kolonne
    df['DateTime'] = pd.to_datetime(df['EndTime'].astype('str') + ' ' + df['DateTime'].astype('str'))


    # LOAD               LOAD              LOAD              LOAD              LOAD              LOAD

# Vælg hvor du vil loade data (Excel ELLER Database)
    if 1 == 1:
        df.to_excel(r'C:\Users\w31610\Desktop\ProjectTest\exceltest\excelftw.xlsx', sheet_name='Sheet', index=False)
        print(df)
    else:
        passwd = maskpass.advpass()
        # Denne metode er valgt fordi det er en SYBASE ASE database, men kan nok også bruges til andre database
        conn = pyodbc.connect(driver=drver, server=serv, database=db,port = prt, uid=usr, pwd=passwd, autocommit=True)
        sql = "INSERT INTO Fact_el (Position, StartTime, EndTime, Quality, kWh) VALUES (?,?,?,?,?)"
        cursor = conn.cursor()
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO Fact_el (Position, StartTime, EndTime, Quality, kWh) VALUES (?,?,?,?,?)", row.Position, row.StartTime, row.EndTime, row.Quality, row.kWh )
        conn.commit()
        print(df)
