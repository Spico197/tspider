from typing import Optional

import pymongo


class MongoDBCollection:
    def __init__(
        self, host: str, port: str,
        db: str, collection: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_source: Optional[str] = None,
        auth_mechanism: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.db_name = db
        self.collection_name = collection
        self.username = username
        self.password = password
        self.auth_source = auth_source
        self.auth_mechanism = auth_mechanism

        self.client = pymongo.MongoClient(
            host, port,
            db, collection,
            username=username,
            password=password,
            auth_source=auth_source,
            auth_mechanism=auth_mechanism
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
