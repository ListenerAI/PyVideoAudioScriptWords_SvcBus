import os
import re
import json
import logging
import asyncio
from io import BytesIO
from dotenv import load_dotenv
from Authentication_BlobStorageClient import BlobStorageClient

# Load environment
load_dotenv("app.env")
INPUT_CONTAINER = os.getenv("INPUT_CONTAINER")
OUTPUT_CONTAINER = os.getenv("OUTPUT_CONTAINER")

# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def combine_transcript_jsons(id_video):
    output_filename = f"{id_video}-audioTextResult.json"
    combined_words = []

    logging.info(f"🔍 Fetching transcription clips for: {id_video}")
    
    blob_client = BlobStorageClient()
    service_client = await blob_client.get_blob_service_client()
    input_container = service_client.get_container_client(INPUT_CONTAINER)
    output_container = service_client.get_container_client(OUTPUT_CONTAINER)

    # Collect matching blob names
    blobs_to_combine = []
    async for blob in input_container.list_blobs():
        if re.search(rf"{re.escape(id_video)}-clip-\d{{3}}_transcription\.json$", blob.name):

            blobs_to_combine.append(blob.name)

    if not blobs_to_combine:
        logging.warning("⚠️ No transcription clips found.")
        return

    # Sort blob names by clip number
    blobs_to_combine.sort(key=lambda name: int(re.search(r"clip-(\d{3})", name).group(1)))

    # Download and combine contents
    for blob_name in blobs_to_combine:
        try:
            logging.info(f"⬇️ Downloading {blob_name}")
            blob_data = await input_container.get_blob_client(blob_name).download_blob()
            stream = BytesIO()
            await blob_data.readinto(stream)
            words = json.loads(stream.getvalue().decode('utf-8'))
            combined_words.extend(words)
        except Exception as e:
            logging.error(f"❌ Error downloading/parsing {blob_name}: {e}")

    # Upload combined result
    output_blob_name = f"{id_video}-audioTextResult.json"
    logging.info(f"☁️ Uploading combined file to {OUTPUT_CONTAINER}/{output_blob_name}")
    await output_container.get_blob_client(output_blob_name).upload_blob(
        json.dumps(combined_words, indent=2), overwrite=True
    )

    # Delete original transcription files
    for blob_name in blobs_to_combine:
        try:
            await input_container.delete_blob(blob_name)
            logging.info(f"🗑️ Deleted: {blob_name}")
        except Exception as e:
            logging.warning(f"⚠️ Failed to delete {blob_name}: {e}")

    return output_blob_name



