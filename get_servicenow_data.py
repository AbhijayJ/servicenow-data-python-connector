
import requests
import json
from requests.auth import HTTPBasicAuth
import time
import pandas as pd

def get_surf_data(instance, table, query, fields, user, password, data_type):
    """
    method to get data from servicenow instance using REST calls (Pagination included)
    inputs: 
        instance name (str) -  eg - xxxx.service-now.com
        table (str) - table name eg- incidents
        query (str) - filter query eg - u_job_id%5Eu_topic%3D7196835647d6211013c396f4126d4314%5Eu_verified%3Dtrue  
        fileds (list) - table fields you want in response eg - ["number", "short_description"]
        user (str) - username for auth
        password (str) - password od username
        data_type (str) - values accepted - "json" or "dataframe"
    Output:
        returns json or pandas dataframe
    """
    auth = HTTPBasicAuth(user, password)
    headers = {'Content-Type': 'application/json'}
    sysparm_offset = 0
    data_dict = {}
    sysparam_fields = ""
    
    for field in fields:
        data_dict[field] = []
        sysparam_fields += field+"%2C"

    while(True):
        url = "https://{0}/api/now/table/{1}?sysparm_query={2}&sysparm_display_value=true&sysparm_fields={3}&sysparm_offset={4}".format(instance, table, query, sysparam_fields, sysparm_offset)
        response = requests.request("GET", url, headers=headers, auth=auth)
        data = json.loads(response.text)

        if not response.ok:
            raise Exception(data)
               
        for record in data['result']:
            for key in data_dict:
                if isinstance(record[key], dict):
                    data_dict[key].append(record[key]['display_value'])
                else:
                    data_dict[key].append(record[key])

        if len(data['result']) == 0:
            print("break")
            break

        print(sysparm_offset)
        time.sleep(5)
        sysparm_offset += 1000
    response.close()

    if data_type == "dataframe":
        df = pd.DataFrame(data_dict)
        return df
    elif data_type == "json":
        return json.dumps(data_dict)
    else:
        raise ValueError('data_type can only have values "json" and "dataframe". recieved {}'.format(data_type)) 
