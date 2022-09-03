import os
import requests
import json
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from extract import json_extract
import pytz
import datetime
import sys
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from dotenv import load_dotenv

f = open("/var/opt/cloudbolt/proserv/customer_settings.py","r")
for line in f.readlines():
  line=line.rstrip()
  if 'TIME_ZONE' in line:
    TIME_ZONE=line.split('=')[1]
    TIME_ZONE=TIME_ZONE.replace(' ','')
    TIME_ZONE=TIME_ZONE.replace('\'','')
    break

load_dotenv()

# #GET orders---captures the data using API according to page number in reverse order that will later be sorted and captures the last(last__)page number
# #Connection for token
def connex_post(url,dat):

  uname=os.environ.get("username")
  pswd=os.environ.get("password")
  response = requests.post( url, data=dat, auth=(uname, pswd),verify=False)  
  return response.json()

# connection to get orders
def connex_get(url_,tok):
  uname=os.environ.get("username")
  pswd=os.environ.get("password")
  payload={"username": uname,"password": pswd}
  headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer {ftoken}'.format(ftoken=str(tok))
  }
  
  response = requests.get(url_, data=payload, headers=headers,verify=False)  

  return response.json()

cb_server=os.environ.get("cb_server")
cb_instance=os.environ.get("instance")
fileName="../output/{}.json".format(cb_instance)
__url="https://{}/api/v2".format(cb_server)

#POST method to get the Token for the session
uname=os.environ.get("username")
pswd=os.environ.get("password")
url_token = "{}/api-token-auth/".format(__url)
data_token={"username": uname,"password": pswd}
res=connex_post(url_token,data_token)
token_=res["token"]

#using the token to get the orders
url_orders="{}/orders".format(__url)
res_orders=connex_get(url_orders,token_)

#last page in orders
last__=res_orders["_links"]["self"]["title"].split()[-1]
last__=int(last__)+1

#using the token to get the blueprints
url_bps="{}/blueprints".format(__url)
res_bps=connex_get(url_bps,token_)
blueprints={}
bp_collect=1
while bp_collect:
  for bp in res_bps['_embedded']:
    bp_url=bp['_links']['self']['href']
    bp_name=bp['_links']['self']['title']
    blueprints[bp_url]={'name':bp_name}

  if 'next' in res_bps['_links']:
    next_url="https://{}{}".format(cb_server,res_bps['_links']['next']['href'])
    res_bps=connex_get(next_url,token_)
  else:
    bp_collect=0


#List to append data to
data_from_orders={}

#function to get data of each order
def order_data(or_id):
  url_orderdata="{}/orders/{}".format(__url,or_id)
  res_orderdata=connex_get(url_orderdata,token_)
  return res_orderdata

#function to get the error of the failed orders
def fail_ure(o_id):
  res_failure=order_data(o_id)
  url_err_append=res_failure["_links"]["jobs"][0]["href"]
  url_error="https://{}/{}".format(cb_server,url_err_append)
  res_error=connex_get(url_error,token_)
  ret_val={'errors':None}
  try:
    if 'start-date' in res_error:
      st_date = converttoawareTz(res_error['start-date'])
      ret_val['start_date']= st_date.strftime("%Y-%m-%dT%H:%M:%S%z")
    if 'end-date' in res_error:
      ed_date = converttoawareTz(res_error['end-date'])
      ret_val['end_date']= ed_date.strftime("%Y-%m-%dT%H:%M:%S%z")
    if st_date is not None and ed_date is not None:
      ret_val['duration']=(ed_date - st_date).total_seconds()
    if 'errors' in res_error:
      ret_val['errors']=res_error["errors"]
    else:
      ret_val['errors']=None
    return ret_val
  except:
    return ret_val

def converttoawareTz(dt_str):
  datetime_obj = datetime.datetime.strptime(dt_str,'%Y-%m-%dT%H:%M:%S.%f' )
  timezone = pytz.timezone(TIME_ZONE)
  d_aware = timezone.localize(datetime_obj)
  return d_aware

#function to get the blueprint & other job details
def order_details(o_id):
  res_order=order_data(o_id)
  url_job_append=res_order["_links"]["jobs"][0]["href"]
  order_blueprint=None
  if 'items' in res_order:
    if 'deploy-items' in res_order['items']:
      if 'blueprint' in res_order['items']['deploy-items'][0]:
        order_blueprint_url=res_order['items']['deploy-items'][0]['blueprint']
        order_blueprint=blueprints[order_blueprint_url]['name']
  url_job="https://{}/{}".format(cb_server,url_job_append)
  res_job=connex_get(url_job,token_)
  st_date=None
  ed_date=None
  ret_val={"errors":None}
  try:
    if 'start-date' in res_job:
      st_date = converttoawareTz(res_job['start-date'])
      ret_val['start_date']= st_date.strftime("%Y-%m-%dT%H:%M:%S%z")
    if 'end-date' in res_job:
      ed_date = converttoawareTz(res_job['end-date'])
      ret_val['end_date']= ed_date.strftime("%Y-%m-%dT%H:%M:%S%z")
    if st_date is not None and ed_date is not None:
      ret_val['duration']=(ed_date - st_date).total_seconds()
    if 'errors' in res_job:
      ret_val['errors']=res_job["errors"]
    else:
      ret_val['errors']=None
    ret_val['blueprint']=order_blueprint
    return ret_val
  except Exception as e:
    print("Error in order details fetch:{}".format(e))
    return ret_val

#function to load the parameters into the dictionary
def load_data(dic,dat):
  temp = json_extract(dat, 'customer-id')
  fla_g=0
  if temp:
    fla_g=1
    dic["customer_id"]=temp[0]
  temp = json_extract(dat, 'resource-group')
  if temp:
    fla_g=1
    dic["resource_group"]=temp[0]
  temp = json_extract(dat, 'subnet')
  if temp:
    fla_g=1
    dic["subnet"]=temp[0]
  temp = json_extract(dat, "virtual-network")
  if temp:
    fla_g=1
    dic["virtual_network"]=temp[0]
  temp = json_extract(dat, "contract-id")
  if temp:
    fla_g=1
    dic["contract_id"]=temp[0]
  temp = json_extract(dat, 'farm-name')
  if temp:
    fla_g=1
    dic["farm_name"]=temp[0]
  temp = json_extract(dat, "subscription-name")
  if temp:
    fla_g=1
    dic["subscription_name"]=temp[0]
  temp = json_extract(dat, "farm-project")
  if temp:
    fla_g=1
    dic["farm_project"]=temp[0]
  if fla_g==0:
    return None
  return dic
  

# function to send GET request by page----------gets the data 
def page_data(last_page,odata):
    global token_
    stop=0
    timeZoneString="America/Los_Angeles"
    for pg_no in reversed(range(1,last_page)):
        url_orders_page="{}/orders?attributes=name,id,status,create-date&page={}".format(__url,int(pg_no))
        #print("URL Orders Page:{}".format(url_orders_page))
        res_eachorder=connex_get(url_orders_page,token_)
        try:
            count=int(res_eachorder["count"])
    
            for i in reversed(range(0,count)):
              temp_dict={}
              temp_dict["order_id"],temp_dict["request_name"],temp_dict["status"],temp_dict["create_date"]=int(res_eachorder["_embedded"][i]["id"]),res_eachorder["_embedded"][i]["name"],res_eachorder["_embedded"][i]["status"],res_eachorder["_embedded"][i]["create-date"]
              temp_dict["instance"]=cb_instance
              datetime_obj = datetime.datetime.strptime(temp_dict["create_date"],'%Y-%m-%d %H:%M:%S.%f' )
              timezone = pytz.timezone(timeZoneString)
              d_aware = timezone.localize(datetime_obj)
              create_date_tz = d_aware.strftime("%Y-%m-%dT%H:%M:%S%z")
              temp_dict["created_date"]=create_date_tz
              id_string="{}_{}".format(temp_dict["order_id"],cb_instance)
              if id_string in odata:
                  print("Breaking as previous data already present in output file")
                  stop=1
                  break
              #fetching data from the orders
              if temp_dict["status"]=="SUCCESS" or temp_dict["status"]=="FAILURE":
                param=order_data(temp_dict["order_id"])
                #appending the data from the order including the parameters: customer-id, resource-group,subnet,vn,contract-id, farm name,    subscription
                check_=load_data(temp_dict,param)
                if check_ is None:
                  print("load_data is None for order:{}".format(temp_dict["order_id"]))
                  continue

                print("I:{} ID:{}".format(i,temp_dict["order_id"]))
                odetails=order_details(temp_dict["order_id"])
                temp_dict.update(odetails)
                if temp_dict['request_name'] is None or temp_dict['request_name'] == "":
                  if odetails['blueprint'] is not None:
                    temp_dict['request_name']=odetails['blueprint']
                data_from_orders[id_string]=temp_dict
                '''
                if temp_dict["status"]=="FAILURE":
                    error_=fail_ure(temp_dict["order_id"])
                    # if error_ is not None:
                    temp_dict["error"]=str(error_)
                    data_from_orders[id_string]=temp_dict
                else:
                    temp_dict["error"]=None
                    data_from_orders[id_string]=temp_dict
                '''
        except Exception as e:
                print("Error encountered:{}".format(e))
        finally:
            if stop:
                break



#initial method
#choose the number of pages to send request to---sends request in reversed order of pages 
flag="0"
sort_lis=[]

with open(fileName,"r+") as fil:
    try:
        old_data=json.load(fil)
    except json.decoder.JSONDecodeError as e:
        print("new")
        old_data={}

page_data(last__,old_data)

old_data.update(data_from_orders)
with open(fileName,"r+") as fil:
    json.dump(old_data,fil)
fil.close()

##Upload file to Blob
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

try:
    connect_str=os.environ.get("blob_conn_string")
    container_name=os.environ.get("blob_container_name")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    local_path="./"
    local_file_name = fileName
    fName=os.path.basename(local_file_name)
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=fName)

    upload_file_path = os.path.join(local_path, local_file_name)
    print("\nUploading local file:{} to Azure Storage as blob:{}\t".format(local_file_name,fName))

    # Upload the created file
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data,overwrite=True)

except Exception as ex:
    print('Exception:')
    print(ex)
