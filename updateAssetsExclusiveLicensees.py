'''
Created on Oct 13, 2018

@author: v.b.pandey
'''
import json
import requests
import pandas as pd
import time
nuxeo_api = 'http://nuxeoalb-stage14-1263263825.us-west-2.elb.amazonaws.com:8080/nuxeo/api/v1/id/'
nuxeo_authentication = {"content-type": "application/json", "X-NXproperties": "*", "Nuxeo-Transaction-Timeout": "300", "Authorization": "Basic QWRtaW5pc3RyYXRvcjpOdXgzMCEyMDE4"}
get_assets_licenee_group_file = 'https://raw.githubusercontent.com/nikss790/Python-projects/master/Jim%20Lee-assets_licensee_groups.csv'


def get_exact_json_data(licensee_group_from_Nuxeo, licensee_group_from_librarian):
    """
    Preparing final JSON in order to
    update licensee group for respective assets
    """
    if licensee_group_from_Nuxeo == "null":
        combined_json_body = list(set(licensee_group_from_librarian))
        #print combined_json_body
        final_json_body = {"entity-type": "document",
                           "properties":
                           {
                               "admin:licenseeCodeGroup": combined_json_body
                           }
                           }
        print "json body for  upudate is ", json.dumps(final_json_body)
    else:
        combined_json_body = list(set(licensee_group_from_Nuxeo + licensee_group_from_librarian.split()))
        print ("Licensee group after removing duplicates --->  %s " %combined_json_body)
        final_json_body = {"entity-type": "document",
                           "properties":
                           {
                               "admin:licenseeCodeGroup": combined_json_body
                           }
                           }
        print "json body for  upudate is ", json.dumps(final_json_body)
    return final_json_body


def get_asset_uid_with_licenee_groups():
    """
     this function will get all asset UID and licensee group
     from spreadsheet
     """
    df = pd.read_csv(get_assets_licenee_group_file)
    asset_uids = df['ASSET_UID']
    print("type for asset -uids  %s : " % asset_uids)
    print asset_uids
    licensee_groups = df['LICENSEE_GROUPS']
    print("type for Licensee group %s : " % licensee_groups)
    print licensee_groups
    counter = 0

    for each_asset_uid in asset_uids:
        counter = counter
        each_job = str(each_asset_uid).strip()
        print("job status to be updated for %s" % (each_asset_uid, ))
        print("final url for to get job id is %s" %
              str(nuxeo_api+each_asset_uid))
        licensee_group_from_librarian = licensee_groups[counter]
        counter = counter + 1
        print("licensee group  [%s] for this asset is :: %s " %
              (licensee_group_from_librarian, each_job))
        try:
            get_assets_details = requests.get(nuxeo_api+each_job,
                                              headers=nuxeo_authentication)
            if get_assets_details.status_code == 200:
                print("Response body is %s " % get_assets_details.json(),)
                asset_uid = get_assets_details.json()['uid']
                asset_name = get_assets_details.json()['title']
                with open('C:\\Users\\v.b.pandey\\Desktop\\Assets_updates\\'+asset_name+'.json', 'w') as outfile:
                    json.dump(get_assets_details.json(), outfile)
                print("job id to be updated is %s " % (asset_uid, ))
                licensee_group_fromDam = get_assets_details.json(
                )['properties']['admin:licenseeCodeGroup']
                print ("exiting licensee group from DAM %s " %licensee_group_fromDam)
                final_json_data = get_exact_json_data(
                    licensee_group_fromDam, licensee_group_from_librarian)
                if(counter%10==0):
                    print "Total assets updated till now ::", counter-1
                    time.sleep(30)
                update_licensee_group(asset_uid, final_json_data)
            else:
                print("Failed to get json body for uid :: %s as it does not exists having client status code as :: %s " % (
                    each_job, get_assets_details.status_code))
        except requests.exceptions.RequestException as err:
            print("Exception occured:: %s while getting job details %s " %
                  err, each_job)


def update_licensee_group(asset_guid, final_json_value):
    """
    This functions call NUXEO update API to partial update 
    licensee group
    """
    print("adding licensee group %s " % nuxeo_api+asset_guid)
    try:
        update_group_response = requests.put(nuxeo_api+asset_guid,
                                             headers=nuxeo_authentication,
                                             data=json.dumps(final_json_value))
        print "status while update is ::  %s " % update_group_response.status_code
        if update_group_response.status_code == 200:
            print("asset is updated successfully for %s " % asset_guid)
        else:
            print("Failed to update the uid :: %s  as Response from the server is %s" % (
                asset_guid, update_group_response.status_code))
    except requests.exceptions.RequestException as err:
        print("Exception occurred::  %s while updating licensee group with payload ::::  %s " % (
            err, final_json_value))


"""
below line of code to execute this utility
"""
if __name__ == "__main__":
    print("Start of utility data pull...")
    get_asset_uid_with_licenee_groups()
    print("End of  Utility...exiting")
