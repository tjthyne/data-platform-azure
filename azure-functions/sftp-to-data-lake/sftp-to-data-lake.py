import logging
import os
from azure.functions import Function
from azure.storage.blob import BlobServiceClient, BlobClient
import yaml
import re


def get_container_config(config, vendor_name, container_name):
    """Retrieves configuration for a specific container from the YAML, handling placeholders."""
    vendor_config = config.get("vendors", {}).get(vendor_name)
    if not vendor_config:
        return None
    container_config = vendor_config.get(container_name)
    if not container_config:
        return None

    # Extract details and handle folder mnemonic placeholder
    destination_container = container_config["destination-container"]
    folder_mnemonic = container_config["folder-mnemonic"]
    source_container = container_config["source-container"]
    file_configs = container_config.get("files", {})  # Dictionary of file configurations

    return {
        "source_container": source_container,
        "destination_container": destination_container,
        "folder_mnemonic": folder_mnemonic,
        "file_configs": file_configs,
    }


def create_virtual_folder(blob_service_client, container_name, virtual_folder_path):
    """Creates a virtual folder within the specified container if it doesn't exist."""
    # Check if container exists
    if not blob_service_client.get_container_client(container_name).exists():
        blob_service_client.create_container(container_name)

    # Construct virtual blob client with empty name for folder creation
    virtual_blob_client = blob_service_client.get_blob_client(container_name, virtual_folder_path)
    virtual_blob_client.create_blob(content="", metadata={})  # Create empty blob for virtual folder


def move_blob_with_virtual_folder(source_blob_service_client, source_container, source_blob_name, destination_blob_service_client, destination_container, folder_mnemonic):
    """Moves a blob from source to destination, creating virtual folders if needed."""
    # Extract date components (assuming consistent format)
    match = re.match(r"\d{4}/\d{2}/\d{2}$", source_blob_name)
    if not match:
        # Handle invalid blob name pattern
        return

    date_str = match.group(0)

    # Construct virtual folder path using folder mnemonic and date
    virtual_folder_path = f"{folder_mnemonic}/{date_str}"

    # Create virtual folder structure if necessary
    create_virtual_folder(destination_blob_service_client, destination_container, virtual_folder_path)

    # Construct source and destination blob paths
    source_blob_path = os.path.join(source_container, source_blob_name)
    destination_blob_client = destination_blob_service_client.get_blob_client(destination_container, virtual_folder_path + "/" + source_blob_name)

    # Copy blob from source to destination (including virtual folder path)
    source_blob_client = source_blob_service_client.get_blob_client(source_blob_path)
    source_blob_client.copy_blob(destination_blob_client)

def main(msg: func.HttpRequest) -> func.HttpResponse:
    """Azure Function triggered by HTTP request.

    Args:
        msg (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response object.
    """

    logging.info('Python function processing a request.')

    try:
        # Load configuration from YAML file
        with open('config.yaml') as config_file:
            config = yaml.safe_load(config_file)

        # Extract vendor and container names from request (replace with your logic)
        vendor_name = msg.params.get('vendor')  # Replace with actual parameter retrieval method
        container_name = msg.params.get('container')  # Replace with actual parameter retrieval method

        # Retrieve container configuration from YAML
        container_config = get_container_config(config, vendor_name, container_name)
        if not container_config:
            return func.HttpResponse(
                "Error: Configuration not found for vendor or container.",
                status_code=400
            )

        # Initialize BlobServiceClient objects for source and destination
        source_blob_service_client = BlobServiceClient.from_connection_string(
            config["source-storage-connection-string"]
        )
        destination_blob_service_client = BlobServiceClient.from_connection_string(
            config["destination-storage-connection-string"]
        )

        # Process each file configuration within the container
        for file_name, file_config in container_config["file_configs"].items():
            # Extract file mnemonic and pattern from single-line dictionary
            file_mnemonic = file_config["file-mnemonic"]
            file_pattern = file_config.get("file-pattern")  # Optional pattern

            # Construct source blob name using pattern (if provided)
            source_blob_name = f"{file_mnemonic}*" if file_pattern else f"{file_mnemonic}.csv"

            # Move blob with virtual folder creation
            move_blob_with_virtual_folder(
                source_blob_service_client,
                container_config["source-container"],
                source_blob_name,
                destination_blob_service_client,
                container_config["destination-container"],
                container_config["folder-mnemonic"],
            )

        return func.HttpResponse(
            "Blobs moved successfully with virtual folder structure.", status_code=200
        )

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return func.HttpResponse(
            f"Error processing request: {str(e)}", status_code=500
        )