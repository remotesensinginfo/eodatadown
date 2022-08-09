	
# =============================================================================
#  By : Carole Planque from Aberystwyth University
#  Last modified: 14 Jul 2021
#  Description: Download Landsat Collection 2 Level-2 from EarthExplorer platform
#                using M2M
#  Usage: download_landsat_c2l2(filetype, datasetName, period_start, period_end, path)
#  where filetype refers to filetype ('bundle' or 'band');
#        datasetName refers to the name of satellite dataset ('landsat_ot_c2_l2', 
#                           'landsat_etm_c2_l2', or 'landsat_tm_c2_l2');
#        period_start str ('%Y-%m-%d') indicating the starting date of downloading period ;
#        period_end str ('%Y-%m-%d') indicating the ending date of downloading period ;
#        path str indicating the path to the output directory for downloaded data.
# =============================================================================

import json
import requests
import sys
import time
import argparse
import re
import threading
import datetime
import os
import getpass



def sendRequest(url, data, apiKey = None, exitIfNoResponse = True): 
    """
    Function to send http request to m2m
    input:
    - url: url address to m2m for http request
    - data: parameters for http request
    - apiKey: apiKey obtained from m2m username and password
    """
    json_data = json.dumps(data)
    
    if apiKey == None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}              
        response = requests.post(url, json_data, headers = headers)  
    
    try:
        httpStatusCode = response.status_code 
        if response == None:
            print("No output from service")
            if exitIfNoResponse: sys.exit()
            else: return False
        output = json.loads(response.text)
        if output['errorCode'] != None:
            print(output['errorCode'], "- ", output['errorMessage'])
            if exitIfNoResponse: sys.exit()
            else: return False
        if  httpStatusCode == 404:
            print("404 Not Found")
            if exitIfNoResponse: sys.exit()
            else: return False
        elif httpStatusCode == 401: 
            print("401 Unauthorized")
            if exitIfNoResponse: sys.exit()
            else: return False
        elif httpStatusCode == 400:
            print("Error Code", httpStatusCode)
            if exitIfNoResponse: sys.exit()
            else: return False
    except Exception as e: 
        response.close()
        print(e)
        if exitIfNoResponse: sys.exit()
        else: return False
    response.close()
    
    return output['data']


def downloadFile(url, path):
    """
    Function to download a scene
    input:
    - url: url of the scene to download
    - path: path of the download directory
    """    
    try:        
        response = requests.get(url, stream=True)
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        print(f"Downloading {filename} ...\n")
        if (path != "" and path[-1] != "/"):
            filename = "/" + filename
        
        if (os.path.isfile(path+filename)!=True):
            open(path+filename, 'wb').write(response.content)
            print(f"Downloaded {filename}\n")
        else:
            print(f"Skipped {filename}: already exists\n")
    except Exception as e:
        print(f"Failed to download from {url}. Will try to re-download.")
        runDownload(url, path)


def runDownload(url, path):
    """
    Function to run download 
    input:
    - url: url of the scene to download
    - path: path of the download directory
    """    
    downloadFile(url,path)


def ee_query(dataset_name, period_start, period_end, cloud_max, wrs2, satellite=None):
    """
    Creates a dictionary of parameters and metadata filters required for searching secenes
    input:
    - dataset_name: alias of the dataset name (i.e., "landsat_tm_c2_l2", "landsat_etm_c2_l2", "landsat_ot_c2_l2")
    - period_start: date to start scene search from (format: "YYYY-MM-DD")
    - period_end: date to end of scene search (format: "YYYY-MM-DD")
    - cloud_max: integer specifying the max cloud cover allowed per scene
    - wrs2: dictionary with path (int) and row (int) of the scene (WRS2 format)
    - satellite: str indicating the number of the satellite 
    """    
    path = wrs2['path']
    row = wrs2['row']
    # Search scenes
    ## metadata filter ids can be retrieved by calling dataset-filters (https://m2m.cr.usgs.gov/api/test/json/)
    childFilters = [   {"filterType": "between",
                        "filterId": "5e83d14fb9436d88",
                        "firstValue": path,
                        "secondValue": path},

                        {"filterType": "between",
                        "filterId": "5e83d14ff1eda1b8",
                        "firstValue": row,
                        "secondValue": row}
                   ]
    if satellite is not None:
        satelliteFilter = [{"filterType": "value",
                            "filterId": "61b0ca3aec6387e5",
                            "value": satellite,
                            "operand": "="}
                          ]
        childFilters = childFilters + satelliteFilter
    
    payload = {   "datasetName": dataset_name,
                    "sceneFilter": {
                        "metadataFilter":   {"filterType": "and",
                                            "childFilters": childFilters
                                            },
                        "acquisitionFilter":{"start": period_start,
                                            "end": period_end
                                            },
                        "cloudCoverFilter":{"max": cloud_max,
                                           "includeUnknown": True}
                        
                    }
    }
    
    return payload


def get_download_details(listId, datasetName, scene_id, filetype, serviceUrl, apiKey):
    """
    Creates a dictionary with the url and file size info of a scene of interest (required to fill the EODD local database)
    input:
    - listId: a temporary name
    - datasetName: alias of the dataset name (i.e., "landsat_tm_c2_l2", "landsat_etm_c2_l2", "landsat_ot_c2_l2")
    - scene_id: ID of the scene of interest
    - filetype: str specifying the type of downloaded files (i.e., "bundle" or "band") 
    """   
    ## Add scene to list 
    payload = {
        "listId": listId,
        "datasetName": datasetName,
        "entityIds": [scene_id]
    }
    
    print("Adding scene to list...\n")
    count = sendRequest(serviceUrl + "scene-list-add", payload, apiKey)    
    print("Added", scene_id, " scene\n")    
    
    ## Get download options
    payload = {
    "listId": listId,
    "datasetName": datasetName
    }
    
    print("Getting product download options...\n")
    products = sendRequest(serviceUrl + "download-options", payload, apiKey)
    print("Got product download options\n")
    
    ## Select products
    downloads = []
    if filetype == 'bundle':
        # select bundle files
        for product in products:        
            if product["bulkAvailable"]:               
                downloads.append({"entityId":product["entityId"], "productId":product["id"]})
                filesize = product["filesize"]
    elif filetype == 'band':
        # select band files
        for product in products:  
            if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                for secondaryDownload in product["secondaryDownloads"]:
                    if secondaryDownload["bulkAvailable"]:
                        downloads.append({"entityId":secondaryDownload["entityId"], "productId":secondaryDownload["id"]})
    else:
        # select all available files
        for product in products:        
            if product["bulkAvailable"]:               
                downloads.append({"entityId":product["entityId"], "productId":product["id"]})
                filesize = product["filesize"]
                if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                    for secondaryDownload in product["secondaryDownloads"]:
                        if secondaryDownload["bulkAvailable"]:
                            downloads.append({"entityId":secondaryDownload["entityId"], "productId":secondaryDownload["id"]})
    
    # Remove the list
    payload = {
        "listId": listId
    }
    sendRequest(serviceUrl + "scene-list-remove", payload, apiKey)                
    
    # Send download-request
    label = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    payLoad = {
        "downloads": downloads,
        "label": label,
        'returnAvailable': True
    }
    
    
    print(f"Sending download request ...\n")
    results = sendRequest(serviceUrl + "download-request", payLoad, apiKey)
    
    for result in results['availableDownloads']:
        download_details = {'url': result['url'],
                            'filesize' : filesize}
        print(download_details)
    
    # Retrieving urls for preparing downloads
    preparingDownloadCount = len(results['preparingDownloads'])
    preparingDownloadIds = []
    if preparingDownloadCount > 0:
        print("There is :", preparingDownloadCount, "preparing download")       
        for result in results['preparingDownloads']:  
            preparingDownloadIds.append(result['downloadId'])
        
        payload = {"label" : label}    
        # Retrieve download urls
        print("Retrieving download urls...\n")
        pending = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
        print("waiting for : ", pending)
        
        if pending != False:
            print("pending : True")
            for scene in pending['available']:
                print("Now available")
                print(pending['available'])
                if scene['downloadId'] in preparingDownloadIds:
                    print("removing from pending list")
                    preparingDownloadIds.remove(scene['downloadId'])
                    print(f"Get download url: {scene['url']}\n" )
                    download_details = {'url': scene['url'],
                                        'filesize' : filesize}
                    print(download_details)
                    print(len(preparingDownloadIds), " scenes remaining in download list...")
            
            for scene in pending['requested']:
                print("In requested section")
                if scene['downloadId'] in preparingDownloadIds:
                    print("removing from pending list")
                    preparingDownloadIds.remove(scene['downloadId'])
                    print(f"Get download url: {scene['url']}\n" )
                    download_details = {'url': scene['url'],
                                        'filesize' : filesize}
                    print(download_details)
            
            # Don't get all download urls, retrieve again after 30 seconds
            while pending['queueSize'] > 0: 
                print(f"{pending['queueSize']} downloads are not available yet. Waiting for 30s to retrieve again\n")
                time.sleep(30)
                pending = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
                if pending != False:
                    for scene in pending['available']: 
                        print("Now available")
                        print(pending['available'])
                        if scene['downloadId'] in preparingDownloadIds:
                            preparingDownloadIds.remove(scene['downloadId'])
                            print(f"Get download url: {scene['url']}" )
                            download_details = {'url': scene['url'],
                                                'filesize' : filesize}
                            print(download_details)
                            print(pending['queueSize'], " scenes remaining in the queue...\n")
    
    print(f"Done sending download request\n")
    return download_details


def get_metadata(ScnMetadata):
    """
    Creates a dictionary with the metadata required by the EODD local database
    input:
    - ScnMetadata: a dictionary of all the metadata of the scene of interest (i.e., out put of m2m scene-metadata request)
    """   
    product_id = [meta['value'] for 
                  meta in ScnMetadata['metadata'] if 
                  meta['fieldName']=='Landsat Product Identifier L2'][0]
    
    cloud_cover = float(ScnMetadata['cloudCover'])
    spacecraft_id =  'LANDSAT_'+ str([meta['value'] for 
                                     meta in ScnMetadata['metadata'] if 
                                     meta['fieldName']=='Satellite'][0])
    sensor_id = [meta['value'] for 
                  meta in ScnMetadata['metadata'] if 
                  meta['fieldName']=='Sensor Identifier'][0]
    date_acquired = [meta['value'] for 
                     meta in ScnMetadata['metadata'] if 
                     meta['fieldName']=='Date Acquired'][0]
    date_acquired = datetime.datetime.strptime(date_acquired, '%Y/%m/%d')
    collection_number = [meta['value'] for 
                         meta in ScnMetadata['metadata'] if 
                         meta['fieldName']=='Collection Number'][0]
    collection_category = [meta['value'] for 
                           meta in ScnMetadata['metadata'] if 
                           meta['fieldName']=='Collection Category'][0]
    sensing_time = [meta['value'] for 
                    meta in ScnMetadata['metadata'] if 
                    meta['fieldName']=='Start Time'][0]
    data_type = [meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Data Type L2'][0]
    wrs_path = [meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='WRS Path'][0]
    wrs_row = [meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='WRS Row'][0]
    west_lon = min([
                float([meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Corner Upper Left Longitude'][0]), 
                float([meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Corner Lower Left Longitude'][0])
              ])
    east_lon = max([
                float([meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Corner Upper Right Longitude'][0]), 
                float([meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Corner Lower Right Longitude'][0])
              ])
    south_lat = min([
                float([meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Corner Lower Left Latitude'][0]), 
                float([meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Corner Lower Right Latitude'][0])
              ])
    north_lat = max([
                float([meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Corner Upper Left Latitude'][0]), 
                float([meta['value'] for 
                meta in ScnMetadata['metadata'] if 
                meta['fieldName']=='Corner Upper Right Latitude'][0])
              ]) 
    
    return {'product_id': product_id,
           'cloud_cover': cloud_cover,
           'spacecraft_id': spacecraft_id,
           'sensor_id': sensor_id,
           'date_acquired': date_acquired, 
           'collection_number': collection_number,
           'collection_category': collection_category,
           'sensing_time':sensing_time,
           'data_type': data_type,
           'wrs_path': wrs_path,
           'wrs_row': wrs_row,
           'west_lon': west_lon,
           'east_lon': east_lon,
           'south_lat': south_lat,
           'north_lat': north_lat}

