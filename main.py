import asyncio
import os
import json
import time
import logging
from datetime import datetime

from azure.servicebus.aio import ServiceBusClient
from Authentication_BlobStorageClient import BlobStorageClient
from script import process_all_video_chunks
from combine_script import combine_transcript_jsons  # ✅ Added
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="app.env")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Service Bus settings
SERVICE_BUS_CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
SERVICE_BUS_TOPIC_NAME = os.getenv("SERVICE_BUS_TOPIC_NAME")
SERVICE_BUS_SUBSCRIPTION_NAME = os.getenv("SERVICE_BUS_SUBSCRIPTION_NAME")

# Constants
DATA_DIR = "data"
INPUT_CONTAINER = os.getenv("INPUT_CONTAINER")
OUTPUT_CONTAINER = os.getenv("OUTPUT_CONTAINER")
os.makedirs(DATA_DIR, exist_ok=True)

async def process_message(message):
    start_time = time.time()
    try:
        payload = json.loads(str(message))
        id_user = payload["id_user"]
        id_video = payload["id_video"]
        clip_filename = payload.get("clip_filename", id_video)

        blob_filename = clip_filename if clip_filename.endswith(".mp4") else f"{clip_filename}.mp4"
        download_path = os.path.join(DATA_DIR, blob_filename)

        storage = BlobStorageClient()
        if not await storage.download_blob(blob_filename, download_path):
            logger.error(f"Blob not found in {INPUT_CONTAINER}: {blob_filename}")
            return

        await process_all_video_chunks(download_path)

        # ✅ Combine all transcription files after chunk processing
        await combine_transcript_jsons(f"{id_user}-{id_video}")

        logger.info(f"✅ Finished processing: {blob_filename} in {(time.time() - start_time):.2f}s")


    except Exception as e:
        logger.exception(f"Error processing message: {e}")

async def main():
    async with ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR) as client:
        receiver = client.get_subscription_receiver(
            topic_name=SERVICE_BUS_TOPIC_NAME,
            subscription_name=SERVICE_BUS_SUBSCRIPTION_NAME
        )
        async with receiver:
            while True:
                messages = await receiver.receive_messages(max_message_count=1, max_wait_time=30)
                if not messages:
                    await asyncio.sleep(5)
                    continue
                for msg in messages:
                    await process_message(msg)
                    await receiver.complete_message(msg)

if __name__ == "__main__":
    logger.info("Starting AI Video Transcription Service...")
    asyncio.run(main())
