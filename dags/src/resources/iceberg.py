import os

from dagster import ConfigurableResource
from pyiceberg.catalog import load_catalog

class IcebergResource(ConfigurableResource):
    def init_catalog(self):
        catalog = load_catalog('rest', **{
            "auth.type": "NONE", 
            "uri": os.getenv("S3_URI"),
            "s3.endpoint": os.getenv("S3_ENDPOINT"),
            "s3.access-key-id": os.getenv("S3_ACCESS_KEY"),
            "s3.secret-access-key": os.getenv("S3_SECRET_KEY"),
            "path-style-access": True
        })
        return catalog
