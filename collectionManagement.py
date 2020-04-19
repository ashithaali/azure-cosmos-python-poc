import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import config as cfg
from flask import Flask, jsonify, request
import json

HOST = cfg.envVariables['host']
MASTER_KEY = cfg.envVariables['master_key']

app = Flask(__name__)

class ContextManagement:
    def __init__(self, obj):
        self.obj = obj
    def __enter__(self):
        return self.obj
    def __exit__(self, exception_type, exception_val, trace):
        self.obj = None

class CollectionManagement:
    #Find Container
    @staticmethod
    def find_Container(client, id,database_link):
        collections = list(client.QueryContainers(
            database_link,
            {
                "query": "SELECT * FROM r WHERE r.id=@id",
                "parameters": [
                    { "name":"@id", "value": id }
                ]
            }
        ))

        if len(collections) > 0:
            dbResponse='Collection with id \'{0}\' was found'.format(id)
        else:
            dbResponse='No collection with id \'{0}\' was found'.format(id)
        return dbResponse
    
    #Create Container
    @staticmethod
    def create_Container(client, id,database_link,subOperation,key):
        if subOperation == "BASIC":
            try:
                client.CreateContainer(database_link, {"id": id})
                dbResponse='Collection with id \'{0}\' created'.format(id)
            except errors.HTTPFailure as e:
                if e.status_code == 409:
                    dbResponse='A collection with id \'{0}\' already exists'.format(id)
                else: 
                    raise

        if subOperation == "DEFAULT_INDEX":
            try:
                coll = {
                    "id": id,
                    "indexingPolicy": {
                    "indexingMode": "lazy",
                    "automatic": False
                    }
                }
                collection = client.CreateContainer(database_link, coll)
                dbResponse1='Collection with id \'{0}\' created'.format(collection['id'])
                dbResponse2='IndexPolicy Mode - \'{0}\''.format(collection['indexingPolicy']['indexingMode'])
                dbResponse3='IndexPolicy Automatic - \'{0}\''.format(collection['indexingPolicy']['automatic'])
                
            
            except errors.CosmosError as e:
                if e.status_code == 409:
                    dbResponse2='A collection with id \'{0}\' already exists'.format(collection['id'])
                else: 
                    raise
            dbResponse=dbResponse1+" "+dbResponse2+" "+dbResponse3

        if subOperation == "CUSTOM_THROUGHPUT":
            try:
                coll = {"id": id}
                collection_options = { 'offerThroughput': 400 }
                collection = client.CreateContainer(database_link, coll, collection_options )
                dbResponse='Collection with id \'{0}\' created'.format(collection['id'])
            
            except errors.HTTPFailure as e:
                if e.status_code == 409:
                    dbResponse='A collection with id \'{0}\' already exists'.format(collection['id'])
                else: 
                    raise
        if subOperation == "UNIQUE_KEYS":
        
            try:
                coll = {"id": id, 'uniqueKeyPolicy': {'uniqueKeys': [{'paths': ['/field1/field2', '/field3']}]}}
                collection_options = { 'offerThroughput': 400 }
                collection = client.CreateContainer(database_link, coll, collection_options )
                unique_key_paths = collection['uniqueKeyPolicy']['uniqueKeys'][0]['paths']
                dbResponse1='Collection with id \'{0}\' created'.format(collection['id'])
                dbResponse2='Unique Key Paths - \'{0}\', \'{1}\''.format(unique_key_paths[0], unique_key_paths[1])
            
            except errors.HTTPFailure as e:
                if e.status_code == 409:
                    dbResponse1='A collection with id \'{0}\' already exists'.format(collection['id'])
                else: 
                    raise
            dbResponse=dbResponse1+" "+dbResponse2

        if subOperation == "PARTITION_KEY":
            try:
                coll = {
                    "id": id,
                    "partitionKey": {
                    "paths": [
                       key
                    ],
                    "kind": "Hash"
                    }
                }

                collection = client.CreateContainer(database_link, coll)
                dbResponse1='Collection with id \'{0}\' created'.format(collection['id'])
                dbResponse2='Partition Key - \'{0}\''.format(collection['partitionKey'])
            
            except errors.CosmosError as e:
                if e.status_code == 409:
                    dbResponse1='A collection with id \'{0}\' already exists'.format(collection['id'])
                else: 
                    raise
            dbResponse=dbResponse1+" "+dbResponse2

        return dbResponse

    #Manage offer throughput
    @staticmethod
    def manage_offer_throughput(client, id,database_link):
        try:
            # read the collection, so we can get its _self
            collection_link = database_link + '/colls/{0}'.format(id)
            collection = client.ReadContainer(collection_link)

            # now use its _self to query for Offers
            offer = list(client.QueryOffers('SELECT * FROM c WHERE c.resource = \'{0}\''.format(collection['_self'])))[0]
            
            dbResponse1='Found Offer \'{0}\' for Collection \'{1}\' and its throughput is \'{2}\''.format(offer['id'], collection['_self'], offer['content']['offerThroughput'])

        except errors.HTTPFailure as e:
            if e.status_code == 404:
                dbResponse='A collection with id \'{0}\' does not exist'.format(id)
            else: 
                raise

        offer['content']['offerThroughput'] += 100
        offer = client.ReplaceOffer(offer['_self'], offer)

        dbResponse2='Replaced Offer. Offer Throughput is now \'{0}\''.format(offer['content']['offerThroughput'])

        dbResponse=dbResponse1+"\n"+dbResponse2
        return dbResponse

    #Read Container       
    @staticmethod
    def read_Container(client, id,database_link)    :
        try:
            collection_link = database_link + '/colls/{0}'.format(id)
            collection = client.ReadContainer(collection_link)
            dbResponse='Collection with id \'{0}\' was found, it\'s _self is {1}'.format(collection['id'], collection['_self'])

        except errors.HTTPFailure as e:
            if e.status_code == 404:
               dbResponse='A collection with id \'{0}\' does not exist'.format(id)
            else: 
                raise
        return dbResponse
    
    #List Container
    @staticmethod
    def list_Containers(client,database_link):
        dbResponse=""
        collections = list(client.ReadContainers(database_link))
        
        if not collections:
            return

        for collection in collections:
            dbResponse=dbResponse+"\n"+collection['id']
        
        return dbResponse         

    #Delete Container    
    @staticmethod
    def delete_Container(client, id,database_link):
        try:
           collection_link = database_link + '/colls/{0}'.format(id)
           client.DeleteContainer(collection_link)

           dbResponse='Collection with id \'{0}\' was deleted'.format(id)

        except errors.HTTPFailure as e:
            if e.status_code == 404:
               dbResponse='A collection with id \'{0}\' does not exist'.format(id)
            else: 
                raise
        return dbResponse
    

    @app.route('/collectionOperations',methods=['POST'])
    def collectionOperations():
        databaseJson= request.json
        operation=databaseJson["operation"]
        client=databaseJson["client"]
        databaseId=databaseJson["databaseId"]
        database_link="dbs/"+databaseId
        if operation == "CREATE_CONTAINER":
            subOperation=databaseJson["subOperation"]
            if subOperation == "PARTITION_KEY":
                key=databaseJson["partitionKey"]
        if operation != "LIST_CONTAINER":
            collectionId=databaseJson["collectionId"]
            
        
        with ContextManagement(cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY} )) as client:
            try:
                if operation == "FIND_CONTAINER":
                    response = CollectionManagement.find_Container(client, collectionId,database_link) 
                elif operation == "CREATE_CONTAINER":
                    response =  CollectionManagement.create_Container(client, collectionId,database_link,subOperation,key) 
                elif operation == "OFFER_THROUGHPUT":
                    response = CollectionManagement.manage_offer_throughput(client, collectionId,database_link)
                elif operation == "READ_CONTAINER":
                    response = CollectionManagement.read_Container(client, collectionId,database_link)
                elif operation == "LIST_CONTAINER":
                    response = CollectionManagement.list_Containers(client,database_link)
                elif operation == "DELETE_CONTAINER":
                    response = CollectionManagement.delete_Container(client, collectionId,database_link)
                else:
                    return("Invalid Operation") 
            except errors.HTTPFailure as e:
                    response = 'Collection management run has caught an error. {0}'.format(e)
            finally:
                print("\n run collection operation done")
        return response

@app.route('/collectionHealth',methods=['GET'])
def health():
    return "Collection management Application is up"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')


