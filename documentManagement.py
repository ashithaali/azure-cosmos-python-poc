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


@app.route('/documentHealth',methods=['GET'])
def health():
    return "Document management Application is up"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

    

        