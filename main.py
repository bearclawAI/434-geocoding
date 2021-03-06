import pandas as pd
import requests
import os

def getData(fileName):
#    df = pd.read_csv(fileName).head(25)
    df = pd.read_csv(fileName)
    df['ADDRESS'] = df['VENDOR_STREET'] +', '+ df['VENDOR_CITY'] +', '+ df['VENDOR_COUNTRY_NAME']
    df['response'], df['lat'], df['lng'], df['place_id']=0, 0, 0, 0
    df['city_country'] =df['VENDOR_CITY'] + df['VENDOR_COUNTRY_NAME']
    return(df)

def keyAPIKey(fileName):
    f = open(fileName, "r")
    GOOGLE_API_KEY = f.read()
    return(GOOGLE_API_KEY)

def getLatLon(row, r, place=0):
    if place==1:
        results=r.json()['predictions'][0]['place_id']
    else:
        results=r.json()['results'][0]
        
    row['lat'] = results['geometry']['location']['lat']
    row['lng'] = results['geometry']['location']['lng']
    row['response']=1
    return(row)

def extract_lat_long(row, location, GOOGLE_API_KEY):
    if row['response']==1:
        return(row)
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    endpoint = f"{base_url}?address={row[location]}&key={GOOGLE_API_KEY}"
    r = requests.get(endpoint)
    try:
        row=getLatLon(row, r)
    except:
        pass
    return(row)
    
def extract_place_id(row,  GOOGLE_API_KEY):
    if row['response']==1:
        return(row)
    base_url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
    endpoint = f"{base_url}?input={row['ADDRESS']}&key={GOOGLE_API_KEY}"
    r = requests.get(endpoint)
    try:
        row['place_id']=getLatLon(row, r, 1)
    except:
        pass
    return(row)
    
def enrich_with_place_api(row,   GOOGLE_API_KEY):
    row = extract_place_id(row, GOOGLE_API_KEY)
    row= extract_lat_long(row, 'place_id', GOOGLE_API_KEY)
    return(row)

def extract_lat_long_via_place_id(row, place_id, GOOGLE_API_KEY):
    if row['response']==1:
        return(row)
    base_url = "https://maps.googleapis.com/maps/api/place/details/json"
    endpoint = f"{base_url}?place_id={place_id}&key={GOOGLE_API_KEY}"
    r = requests.get(endpoint)
    try:
        results = r.json()['result']
        row=getLatLon(row, results)
    except:
        pass
    return(row)
    
def enrich_with_geocoding_api(row, GOOGLE_API_KEY, column_name):
    row=extract_lat_long(row, 'city_country', GOOGLE_API_KEY)
    return(row)
    
def allWaysOfGettingLatLon(df, GOOGLE_API_KEY):
    dfFromGeocodingAPI=pd.DataFrame([enrich_with_geocoding_api(df.iloc[i], GOOGLE_API_KEY, 'ADDRESS') for i in range(0, len(df))])
    dfFromEnrichWithPlace=pd.DataFrame([enrich_with_place_api(dfFromGeocodingAPI.iloc[i], GOOGLE_API_KEY) for i in range(0, len(dfFromGeocodingAPI))])
    dfFromCity=pd.DataFrame([enrich_with_geocoding_api(dfFromEnrichWithPlace.iloc[i], GOOGLE_API_KEY, 'city_country') for i in range(0, len(dfFromEnrichWithPlace))])
    return(dfFromCity)
    
def main():
    os.chdir(os.getcwd().replace("code", "data"))
    fileName='contour-export-03-03-2021.csv'    
    GOOGLE_API_KEY=keyAPIKey("googleAPI.txt")
    originalData= getData(fileName)
    finalData=allWaysOfGettingLatLon(originalData, GOOGLE_API_KEY)
    return(finalData)

finalData=main()