
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
from notebookutils import mssparkutils
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, to_utc_timestamp, to_timestamp, date_format

tenantID = '6ee0d2f5-899d-48c8-b2a3-33061f5665bc' # Organisationens TenantId
workspaceID = '5423056f-773f-45b0-831c-8f7828c7ba39' # Dit LogAnalytics Workspace
clientID = '' # serviceprinicpal
clientsecret = '' # serviceprinicpal

# destination for parquetfiler
Destination_Folder      = "abfss://CONTAINERNAME@STORAGEACCOUNT.dfs.core.windows.net/FOLDERNAME/SUBFOLDERNAME/SUBSUBFOLDERNAME"
# csv fil som logger sidste kørte tidspunkt, så vi kan opsamle dette ved hver kørsel
LastModifiedDate_Folder = "abfss://CONTAINERNAME@STORAGEACCOUNT.dfs.core.windows.net/FOLDERNAME/SUBFOLDERNAME/SUBSUBFOLDERNAME"


#### Henter refresh token, så vi kan få adgang til API'et i loganalytics

#vi er nødt til at få en refreshable access token før vi kan tilgå power admin api'erne. for hvis vi hardcoder token, udløber den efter 1-2 timer 
def get_access_token():
    tenantId = tenantID
    clientId = clientID
    clientSecret = clientsecret
    resource = "https://api.loganalytics.io"
    authorityUrl = "https://login.microsoftonline.com/" + tenantId
    tokenUrl = authorityUrl + "/oauth2/token"
    body = {
        "client_id": clientId,
        "client_secret": clientSecret,
        "resource": resource,
        "grant_type": "client_credentials"
    }

    response = requests.post(tokenUrl, data=body)
    jsonResponse = response.json()
    accessToken = jsonResponse.get("access_token")

    return accessToken

access_token = get_access_token()
print(access_token)


# ### Definerer API

spark = SparkSession.builder.appName("App").getOrCreate()

api_endpoint = f"https://api.loganalytics.azure.com/v1/workspaces/{workspaceID}/query"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}


### Henter sidste tidspunkt/dato for indlæst data, så vi kan hente data incrementelt

# Vi logger altid, hvornår sidste kørsel har været, så vi kan opsamle Incrementelt. Derfor 
LastModifiedDate_str = spark.read.load(LastModifiedDate_Folder + '/*', format='csv', header='true').first()[0]
LastModifiedDate = datetime.strptime(LastModifiedDate_str, "%Y-%m-%d %H:%M:%S")
yesterday = datetime.now() - timedelta(days=1)
yesterday_235959 = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)

# ### Looper gennem API. Time for time 

# Vi laver en tom liste, så vi kan indsætte alle vores rows ind i denne liste for hvert request vi laver (Vi laver 1 request, for at få én times data fra en specifik dag)
data_list = []

while LastModifiedDate <= yesterday_235959:
    # Denne er fra vores logget tidspunkt som vi læser fra folderen med csv fil, som har seneste indlæste data tidspunkt logget.
    filter_LastModifiedDate = LastModifiedDate.strftime('%Y-%m-%dT%H:%M:%SZ')
    # filter_EndDate = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(filter_LastModifiedDate)
    # På grund af begrænsinger af API'et, så henter vi dataen time for time altså mellem tidspunket filter_LastModifiedDate og filter_LastModifiedDate + 1h... altså dynamiske filtre
    kql_query = f"""
    PowerBIDatasetsWorkspace
    | where todatetime(TimeGenerated) >= datetime({filter_LastModifiedDate}) and todatetime(TimeGenerated) < datetime({filter_LastModifiedDate}) + 1h
    | where isnotnull(CpuTimeMs) and CpuTimeMs != 0
    | extend IdentityData = parse_json(Identity), ApplicationContextData = parse_json(ApplicationContext), FormattedTimeGenerated = todatetime(strcat(format_datetime(TimeGenerated, 'yyyy-MM-dd HH:mm:'), '00'))
    | project
    FormattedTimeGenerated,
    OperationName,
    OperationDetailName,
    LogAnalyticsCategory,
    PowerBIWorkspaceId,
    ArtifactId,
    DatasetMode,
    XmlaRequestId,
    Status,
    User,
    ExecutingUser,
    Roles = strcat_array(IdentityData.effectiveClaims.roles, " | "),
    DatasetId = tostring(ApplicationContextData.DatasetId),
    ReportId = tostring(ApplicationContextData.Sources[0].ReportId),
    VisualId = tostring(ApplicationContextData.Sources[0].VisualId),
    ConsumptionMethod = tostring(ApplicationContextData.Sources[0].HostProperties.ConsumptionMethod),
    UserSession = tostring(ApplicationContextData.Sources[0].HostProperties.UserSession),
    CpuTimeMs,
    DurationMs
    | summarize
    CpuTimeMs = sum(CpuTimeMs),
    DurationMs = sum(DurationMs)
    by
    FormattedTimeGenerated,
    OperationName,
    OperationDetailName,
    LogAnalyticsCategory,
    PowerBIWorkspaceId,
    ArtifactId,
    DatasetMode,
    XmlaRequestId,
    Status,
    User,
    ExecutingUser,
    Roles,
    DatasetId,
    ReportId,
    VisualId,
    ConsumptionMethod,
    UserSession
    """

    request_body = {
    "query": kql_query
    }
    
    response = requests.post(api_endpoint, headers=headers, data=json.dumps(request_body))
    print('2')
    print(response.status_code)
    # Vi ser om API-service hos Microsoft er oppe, da den kan være under vedlighehold el. andet nogle gange. Bare god at gøre detpraksis.
    if response.status_code == 200:
        print(response.status_code)
        # Vi parser responset til et json objekt, så man kan fortolke den som json
        query_results = response.json()
        # Vi laver en if, for at sikre os at der er noget i responset som lever op til det som vi forventer. Her forventer vi at der i api'et er angivet "Tables"
        ## Under tables i responset er der tre dele: Columns, Names and rows. Vi er kun interesseret i Columns og rows.
        if "tables" in query_results and len(query_results["tables"]) > 0:
            print('4')
            rows = query_results["tables"][0]["rows"]
            columns = query_results["tables"][0]["columns"]
            data_list.extend(rows)
            
            # Incrementer med 1 time for hvert loop i while loopet condition er opfyldt
            LastModifiedDate += timedelta(hours=1)
    else: 
        break


# ### **Skriver ned som Parquetfiler **

# Hvis der ikke er data, så bare skriv 'No Date'. Man kan også bare sige den skal stoppe ved ovenstående "break". Choices in life are many
if not data_list:
    print("No data")
# Hvis der er data, så skriv dataen til parquet filer med tilhørende schema (Husk at udvide schema, hvis der kommer krav om andre kolonner)
else: 
    df = spark.createDataFrame(data_list, schema=['FormattedTimeGenerated', 'OperationName', 'OperationDetailName', 'LogAnalyticsCategory', 'PowerBIWorkspaceId', 'ArtifactId', 'DatasetMode', 'XmlaRequestId', 'Status', 'User', 'ExecutingUser', 'Roles', 'DatasetId', 'ReportId', 'VisualId', 'ConsumptionMethod', 'UserSession', 'CpuTimeMs', 'DurationMs'])
    df = df.withColumn("y", date_format(df["FormattedTimeGenerated"], "yyyy"))
    df = df.withColumn("m", date_format(df["FormattedTimeGenerated"], "MM"))
    df = df.withColumn("d", date_format(df["FormattedTimeGenerated"], "dd"))
    df = df.withColumn("FormattedTimeGenerated", to_timestamp(from_utc_timestamp(to_utc_timestamp("FormattedTimeGenerated", 'UTC'), 'UTC')))
    df.show(5)
    # folderstrukturen bliver vores Destination_Folder + year, month, day. Det er dette PartitionBy dækker over. nothing more, nothing less
    df.write.partitionBy("y","m","d").mode("overwrite").parquet(Destination_Folder)


# logger nu kørslen, så vi kan genoptage herfra ved næste kørsel
date_string = end_date.strftime('%Y-%m-%d')
df_datetime = spark.createDataFrame([(date_string + ' ' + '00:00:00',)], ["datetime"])
df_datetime.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(LastModifiedDate_Folder)