from pymongo import MongoClient
client = MongoClient("mongodb://ranjith:Ranjith@34.42.109.189:27017/admin")
print(client.list_database_names())
