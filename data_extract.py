import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import json 
import re 
# get dataset list via api and convert to json
response = requests.get('https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items')
data=response.json()
#keep hospital data only
hospitals = [d for d in data if 'Hospitals' in d.get('theme') ]
# directory for output data and metadata 
os.makedirs('hospital_output', exist_ok=True)
os.makedirs('runs', exist_ok = True)
metadata_path = 'runs/metadata.json'
if os.path.exists(metadata_path):
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    if "datasets" not in metadata:
        metadata["datasets"] = {}
else:
    metadata = {"datasets": {}}


# fx for csv download
def download_csv(url, filename, identifier, modified):
    response=requests.get(url)
    if response.status_code == 200: 
        with open(filename, 'wb') as file: 
            file.write(response.content)
        # snake case implementation 
        df = pd.read_csv(filename, dtype=str)
        df.columns = [re.sub(r'[^a-z0-9]+', '_', str(col).strip('"\'').lower()).strip('_') for col in df.columns]
        df.to_csv(filename, index=False)
        return(identifier, modified)
    else: 
        print("Failed to download file")
    return None

#compile csvs with metadata info 
def process_one(item):
    identifier, modified, url, filename = item
    return download_csv(url, filename, identifier, modified)

# add files that need to be updated to to_download 
to_download = []
for dataset in hospitals:
    if not dataset.get('distribution'): #skip datasets without distribution info
        continue
    if dataset['identifier'] in metadata["datasets"]:
        if metadata["datasets"][dataset['identifier']] == dataset['modified']: #skip datasets that haven't been updated since last processing
            continue   
    download_url = dataset['distribution'][0]['downloadURL']
    filename = os.path.join('hospital_output', dataset['identifier'] + '_' + dataset['modified'] + '.csv')
    to_download.append((dataset['identifier'], dataset['modified'], download_url, filename))

#read csvs in parallel from the to_download list and update their metadata 
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_one, item) for item in to_download]
    for future in as_completed(futures):
        result = future.result()
        if result:
            identifier, modified = result
            metadata["datasets"][identifier] = modified

# update metadata json 
with open(metadata_path, 'w') as f:
    json.dump(metadata, f, indent=2)
       
    


