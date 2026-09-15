import os
from pathlib import Path

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient


load_dotenv()

def create_container(container_name: str):
    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(
        container_name
    )

    if not container_client.exists():
        container_client.create_container()
        print(f"[BLOB] Container creado: {container_name}")
    else:
        print(f"[BLOB] Container ya existe: {container_name}")


def upload_to_container(
    container_name: str,
    local_file_path: str,
    blob_name: str
):
    blob_service_client = get_blob_service_client()

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    file_path = Path(local_file_path)

    with file_path.open("rb") as data:
        blob_client.upload_blob(
            data,
            overwrite=True
        )

    print(
        f"[BLOB] {file_path} -> "
        f"{container_name}/{blob_name}"
    )

def list_blobs():
    container_name = os.getenv("AZURE_STORAGE_BRONZE_CONTAINER")

    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(
        container_name
    )

    print(f"[BLOB] Archivos en container '{container_name}':")

    for blob in container_client.list_blobs():
        print(f" - {blob.name}")


def get_blob_service_client() -> BlobServiceClient:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    return BlobServiceClient.from_connection_string(
        connection_string
    )


def create_bronze_container():
    container_name = os.getenv("AZURE_STORAGE_BRONZE_CONTAINER")

    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(
        container_name
    )

    if not container_client.exists():
        container_client.create_container()
        print(f"[BLOB] Container creado: {container_name}")
    else:
        print(f"[BLOB] Container ya existe: {container_name}")


def upload_file(
    local_file_path: str,
    blob_name: str
):
    container_name = os.getenv("AZURE_STORAGE_BRONZE_CONTAINER")

    blob_service_client = get_blob_service_client()

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    file_path = Path(local_file_path)

    with file_path.open("rb") as data:
        blob_client.upload_blob(
            data,
            overwrite=True
        )

    print(f"[BLOB] Archivo subido: {blob_name}")

def list_container_blobs(container_name: str):
    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(
        container_name
    )

    print(f"[BLOB] Contenido de '{container_name}':")

    for blob in container_client.list_blobs():
        print(f" - {blob.name}")


if __name__ == "__main__":
    create_bronze_container()

    upload_file(
        local_file_path="output/sales.parquet",
        blob_name="sales/sales.parquet"
    )

    list_blobs()