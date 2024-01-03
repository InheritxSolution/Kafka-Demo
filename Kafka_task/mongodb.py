import pymongo
from bson.objectid import ObjectId


class MongoDBManager:
    def __init__(
        self,
        db_name,
        collection_name,
        mongo_url="mongodb://localhost:27017/",
    ):
        self.client = pymongo.MongoClient(mongo_url)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def create_document(self, data):
        inserted_document = self.collection.insert_one(data)
        return inserted_document.inserted_id

    def close_connection(self):
        self.client.close()
