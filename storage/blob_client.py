
import os

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()


def create_bronze_container():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_STORAGE_BRONZE_CONTAINER")

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )

    container_client = blob_service_client.get_container_client(
        container_name
    )

    if not container_client.exists():
        container_client.create_container()
        print(f"[BLOB] Container creado: {container_name}")
    else:
        print(f"[BLOB] Container ya existe: {container_name}")


if __name__ == "__main__":
    create_bronze_container()