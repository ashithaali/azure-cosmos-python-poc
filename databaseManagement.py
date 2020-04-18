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

@app.route('/health',methods=['GET'])
def health():
    return "Database management Application is up"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')



    
            


