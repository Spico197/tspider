from typing import Optional

import pymongo


class MongoDBCollection:
    def __init__(
        self, host: str, port: int,
        db: str, collection: str,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        self.host = host
        self.port = port
        self.db_name = db
        self.collection_name = collection
        self.username = username
        self.password = password

        self.client = pymongo.MongoClient(
            host, port,
            username=username,
            password=password
        )
        self.db = self.client[db]
        self.collection = self.db[collection]

    def insert_one(self, data: dict):
        try:
            self.collection.insert_one(data)
        except pymongo.errors.DuplicateKeyError:
            pass

    def update_one(self, filter_condition: dict, new_data: dict):
        self.collection.update_one(filter_condition, new_data)

    def delete_one(self, filter_condition: dict, data: dict):
        self.collection.delete_one(filter_condition, data)

    def __str__(self) -> str:
        return f"<MongoDB: {self.db_name}/{self.collection_name}>"

    def __repr__(self) -> str:
        return self.__str__()
