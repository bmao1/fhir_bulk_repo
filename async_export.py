#!/usr/bin/env python3
# -*- coding: utf-8 -*-


#async api call 

#curl -v -H 'accept: application/fhir+json' -H 'prefer: respond-async' 'https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1IjozfQ/fhir/Group/449f3677-6053-4e00-a4aa-881721f07332/$export'


import requests
import time
#import re

base_url = 'https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1IjozfQ/fhir/'
query= 'Group/e0896a87-0547-4a29-9b2f-eb2cfa708806/$export'
HEADERS = {
    "accept": "application/fhir+json",
    "prefer": "respond-async"
}

resp = requests.get(base_url+query, headers=HEADERS).json()
print(resp)

print("request status: {0}".format(resp["issue"][0]["code"]))
print("datapull status url: {0}".format(resp["issue"][0]["diagnostics"].split("\"")[-2]))


time.sleep(20)

url = resp["issue"][0]["diagnostics"].split("\"")[-2]
resp2 = requests.get(url).json()
print(resp2)


for file in resp2["output"]:
    print(file["url"])
    file_url = file["url"]
    filetype= file["type"]
    r = requests.get(file_url, stream = True) 
  
    with open("/Users/binmao/Documents/bulkdata_analytics/data/{0}.ndjson".format(filetype),"wb") as ndjsonfile: 
        for chunk in r.iter_content(chunk_size=512*1024): 
            if chunk: 
                ndjsonfile.write(chunk)