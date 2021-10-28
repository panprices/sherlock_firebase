from typing import Any
from firebase_admin import firestore
from datetime import datetime

# Copied from stackoverflow
# https://stackoverflow.com/a/62983902
class FirestoreTriggerConverter(object):
    def __init__(self, client: firestore._FirestoreClient) -> None:
        self.client = client
        self._action_dict = {
            "geoPointValue": (lambda x: dict(x)),
            "stringValue": (lambda x: str(x)),
            "arrayValue": (
                lambda x: [self._parse_value(value_dict) for value_dict in x["values"]]
            ),
            "booleanValue": (lambda x: bool(x)),
            "nullValue": (lambda x: None),
            "timestampValue": (lambda x: self._parse_timestamp(x)),
            "referenceValue": (lambda x: self._parse_doc_ref(x)),
            "mapValue": (
                lambda x: {
                    key: self._parse_value(value) for key, value in x["fields"].items()
                }
            ),
            "integerValue": (lambda x: int(x)),
            "doubleValue": (lambda x: float(x)),
        }

    def convert(self, data_dict: dict) -> dict:
        result_dict = {}
        for key, value_dict in data_dict.items():
            result_dict[key] = self._parse_value(value_dict)
        return result_dict

    def _parse_value(self, value_dict: dict) -> Any:
        data_type, value = value_dict.popitem()

        return self._action_dict[data_type](value)

    def _parse_timestamp(self, timestamp: str):
        try:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError as e:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")

    def _parse_doc_ref(self, doc_ref: str) -> Any:
        path_parts = doc_ref.split("/documents/")[1].split("/")
        collection_path = path_parts[0]
        document_path = "/".join(path_parts[1:])

        doc_ref = self.client.collection(collection_path).document(document_path)
        return doc_ref
