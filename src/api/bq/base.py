import os
from google.cloud import bigquery
from api.settings import settings


def get_bq_client():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        settings.google_application_credentials
    )
    return bigquery.Client()
