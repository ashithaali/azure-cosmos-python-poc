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

class DatabaseManagement:
    #Find Database
    @staticmethod
    def find_database(client, id):
        databases = list(client.QueryDatabases({
            "query": "SELECT * FROM r WHERE r.id=@id",
            "parameters": [
                { "name":"@id", "value": id }
            ]
        }))
        
        if len(databases) > 0:
            dbResponse = 'Database with id \'{0}\' was found'.format(id)
        else:
            dbResponse = 'No database with id \'{0}\' was found'. format(id)
        
        return dbResponse 

    #Create Database
    @staticmethod
    def create_database(client, id):
        try:
            client.CreateDatabase({"id": id})
            dbResponse='Database with id \'{0}\' created'.format(id)

        except errors.HTTPFailure as e:
            if e.status_code == 409:
               dbResponse='A database with id \'{0}\' already exists'.format(id)
            else: 
                raise  
        return dbResponse

    #List Databases
    @staticmethod
    def list_databases(client):
        databases = list(client.ReadDatabases())
        print(databases)
        if not databases:
            dbResponse ="No Database Found"
        for database in databases:
            dbResponse = database['id']
        return dbResponse  

    #Delete Database
    @staticmethod
    def delete_database(client, id):
        try:
           database_link = 'dbs/' + id
           client.DeleteDatabase(database_link)
           dbResponse='Database with id \'{0}\' was deleted'.format(id)
            
        except errors.HTTPFailure as e:
            if e.status_code == 404:
               dbResponse='A database with id \'{0}\' does not exist'.format(id)
            else: 
                raise 
        return dbResponse

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
                
            
            except errors.HTTPFailure as e:
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
            
            except errors.HTTPFailure as e:
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

class DocumentManagement:
    @staticmethod
    def create_document(client,collection_link,items):
        for item in items:
            client.CreateItem(collection_link, item)
        return "Item Created"

    @staticmethod
    def read_document(collectionId,docId,collection_link,client):
        items=[]
        query = "SELECT * FROM "+collectionId+ " c WHERE  c.id = '"+ docId+"'"
        
        for item in client.QueryItems(collection_link,
                              query,
                              {'enableCrossPartitionQuery': True}
                              ):
            items.append(item)
        
        if len(items) == 0:
            return "Product Not Found"
        else:
            return str(json.dumps(items, indent=True))
       

    @staticmethod
    def read_documents(client, collection_link):
        documentlist = list(client.ReadItems(collection_link, {'maxItemCount':10}))
        return json.dumps(documentlist, indent=True)

    @staticmethod
    def delete_document(client,doc_link,key):
        options = {}
        options['enableCrossPartitionQuery'] = True
        options['maxItemCount'] = 5
        options['partitionKey'] = key
        client.DeleteItem(doc_link,options)
        return "Delete Success"

    @staticmethod
    def update_document(client,collection_link,item):
        client.UpsertItem(collection_link,item)
        return "Success"
        

@app.route('/dbOperations',methods=['POST'])
def dbOperations():
    databaseJson= request.json
    operation=databaseJson["operation"]
    client=databaseJson["client"]
    if operation != "LIST_DATABASE":
        DATABASE_ID=databaseJson["databaseId"]
    
    with ContextManagement(cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY} )) as client:
        try:
            if operation == "FIND_DATABASE":
                response = DatabaseManagement.find_database(client, DATABASE_ID) 
            elif operation == "CREATE_DATABASE":
                response =  DatabaseManagement.create_database(client, DATABASE_ID)
            elif operation == "LIST_DATABASE":
                response = DatabaseManagement.list_databases(client)
            elif operation == "DELETE_DATABASE":
                response = DatabaseManagement.delete_database(client, DATABASE_ID)
            else:
                return("Invalid Operation") 
        except errors.HTTPFailure as e:
                response = 'Database management run has caught an error. {0}'.format(e)
        finally:
             print("\n run db operation done")
    return response

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

@app.route('/documentOperations',methods=['POST'])
def documentOperations():
    databaseJson= request.json
    client=databaseJson["client"]
    collectionId=databaseJson["collectionId"]
    databaseId= databaseJson["databaseId"]
    operation=databaseJson["operation"]
    database_link="dbs/"+databaseId
    collection_link=database_link + '/colls/' + collectionId
    
    if operation == "CREATE_DOCUMENT" or operation == "UPDATE_DOCUMENT":
        items=databaseJson["item"]
    
    if operation == "READ_DOCUMENT" or operation == "DELETE_DOCUMENT" or operation == "UPDATE_DOCUMENT":
        doc_id=databaseJson["docId"]
        doc_link = collection_link + '/docs/' + doc_id
    
    if operation == "DELETE_DOCUMENT" or operation == "UPDATE_DOCUMENT":
        key=databaseJson["partitionValue"]
    
    with ContextManagement(cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY} )) as client:
        try:
            if operation == "CREATE_DOCUMENT":
                response = DocumentManagement.create_document(client, collection_link,items) 
            elif operation == "READ_DOCUMENT":
                response = DocumentManagement.read_document(collectionId,doc_id,collection_link,client)
            elif operation == "READ_DOCUMENTS":
                response = DocumentManagement.read_documents(client,collection_link)
            elif operation == "DELETE_DOCUMENT":
                response = DocumentManagement.delete_document(client,doc_link,key)
            elif operation == "UPDATE_DOCUMENT":
                response = DocumentManagement.update_document(client,collection_link,items)
            else:
                return("Invalid Operation") 

        except errors.HTTPFailure as e:
                response = 'Document management run has caught an error. {0}'.format(e)
        finally:
             print("\n run document operation done")
    return response


@app.route('/health',methods=['GET'])
def health():
    return "Database management Application is up"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')



    
            


