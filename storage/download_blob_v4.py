from pathlib import Path
from storage.blob_client_general import get_blob_service_client


def download_blob(
    container_name: str,
    blob_name: str,
    destination: str
):
    blob_service_client = get_blob_service_client()

    container_client = blob_service_client.get_container_client(
        container_name
    )

    blob_client = container_client.get_blob_client(blob_name)

    destination_path = Path(destination)
    destination_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    print(
        f"[STAGING] Descargando: "
        f"{container_name}/{blob_name}"
    )

    with destination_path.open("wb") as file:
        file.write(
            blob_client.download_blob().readall()
        )

    print(
        f"[STAGING] Archivo disponible en: "
        f"{destination_path}"
    )