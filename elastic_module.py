
import os
import csv
from elasticsearch import helpers, Elasticsearch, exceptions
from ssl import create_default_context
import pandas as pd
import sys

def elk_session():

    try:
        context = create_default_context(cafile='certs\\ca.crt')
        elkserver1 = "server1"
        elkserver2 = "server2"
        elkserver3 = "server3"
        list_of_hosts_FQDN = [elkserver1,elkserver2,elkserver3]
        client =Elasticsearch(list_of_hosts_FQDN, ssl_context=context, http_auth=(user,password), timeout=20)
        return client

    except exceptions.ConnectionError as e:
		logger.critical("Connectivity to ELK cluster failed")
		
def insert_data_to_es(infile, index, client):
    with open (infile+".csv", 'r') as f:
        reader = csv.DictReader(f)
        helpers.bulk(client, reader, index = index)
        

def check_create_index(indexname, client, functionname):
    if not client.indices.exists(index=indexname):
        functionname(indexname, client)

def check_delete_index(indexname,client):
    if client.indices.exists(index=indexname):
        client.indices.delete(index=indexname)


def create_dir_if_not_available(directory_name):
    if not os.path.exists(os.path.dirname(os.path.abspath(__file__))+'\\'+directory_name):
        os.makedirs(os.path.dirname(os.path.abspath(__file__))+'\\'+directory_name)

def create_file_if_not_available(directory, filename, fieldnames):
    if not os.path.exists(os.path.dirname(os.path.abspath(__file__))+'\\'+directory+'\\'+filename+'.csv'):
        file = os.path.dirname(os.path.abspath(__file__))+'\\'+directory+'\\'+filename+'.csv'
        with open(file, 'w') as file1:
            fields = fieldnames()
            writer = csv.DictWriter(file1, fieldnames = fields)
            writer.writeheader()


def microsec_to_millisec(input):
    """
    This will convert micro second to milli second and return the value with two decimal places
    """
    return round(float(input/1000),2)

def bytes_to_megabytes(input):
    """
    This will convert bytes to megabytes and return the value with two decimal places
    """

    return round(float(input/1024/1024),2)

def bytes_to_gigabytes(input):
    """
    This will convert bytes to gigabytes and return the value with two decimal places
    """

    return round(float(input/1024/1024/1024),2)

def bytes_to_terabytes(input):
    """
    This will convert bytes to gigabytes and return the value with two decimal places
    """

    return round(float(input/1024/1024/1024/1024),2)

def kilobytes_to_terabytes(input):
    """
    This will convert kilobytes to terabytes and return the value with two decimal places
    """

    return round(float(input/1024/1024/1024),2)










