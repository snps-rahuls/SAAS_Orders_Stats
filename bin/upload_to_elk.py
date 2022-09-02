import json
import requests
from elasticsearch import Elasticsearch
import elasticsearch.helpers as help
import sys
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from dotenv import load_dotenv

load_dotenv()

ES_server=os.environ.get("server")
es = Elasticsearch([ES_server])
es_index_pattern=os.environ.get("index")


try:
    connect_str=os.environ.get("blob_conn_string")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name=os.environ.get("blob_container_name")
    local_path="../output/"

except Exception as ex:
    print('Exception:')
    print(ex)



environments=["qa","dev","pv","preprod","prod","eval"]
for env in environments:

    print("Processing for env:{}".format(env.upper()))
    ##Download file from Blob
    fileName="{}.json".format(env.upper())
    download_file_path = os.path.join(local_path, fileName)
    blob_client = blob_service_client.get_container_client(container= container_name)

    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob(fileName).readall())


    query_count='{ "query": { "query_string": { "query": "instance:'+str(env.upper())+'" } } }'
    es_index_search="{}*".format(es_index_pattern)
    res = es.count(index=es_index_search,body=query_count)
    if res['count'] > 0:
        query_body='{ "size":1,"sort": [ { "order_id": { "order": "desc" } } ], "query": { "query_string": { "query": "instance:'+str(env.upper())+'" } } }'

        res_ = es.search(index=es_index_search,scroll="2m",body=query_body)
        last_record=res_['hits']['hits'][0]['_source']['order_id']
    else:
        last_record=0


    # #capturing the last id from ES
    sort_lis=[]
    with open(fileName,"r+") as fil:
        data_main=json.load(fil)

    import datetime
    x = datetime.datetime.now()
    index="{}{}".format(es_index_pattern,x.year)
    final_data=[]
    for o in sorted(data_main):
        (oid,inst_name)=o.split('_')
        if int(oid) > last_record:
            temp_data={"_index":index,"_type":"doc","_source": data_main[o]}
            final_data.append(temp_data)
    if len(final_data) > 0:
        print(help.bulk(es, final_data, refresh=True))
