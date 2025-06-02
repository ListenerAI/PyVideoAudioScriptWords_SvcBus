import asyncio
import os
import json
import time
import logging
from datetime import datetime

from azure.servicebus.aio import ServiceBusClient
from ai_speech_transcription_processor_stablev1 import (
    extract_audio,
    split_audio,
    process_audio_chunks,
    clean_related_files,
    AUDIO_TRANSCRIPTIONS_PATH,
    AUDIO_OUTPUT_PATH
)
from Authentication_BlobStorageClient import BlobStorageClient
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
OUTPUT_CONTAINER = "mediatestingdata"
INPUT_CONTAINER = "computervision"
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

        clean_related_files(AUDIO_TRANSCRIPTIONS_PATH, download_path)

        if not extract_audio(download_path, AUDIO_OUTPUT_PATH):
            logger.error("Audio extraction failed.")
            return

        chunks = split_audio(AUDIO_OUTPUT_PATH, AUDIO_TRANSCRIPTIONS_PATH)
        if not chunks:
            logger.error("Audio split failed.")
            return

        process_audio_chunks(chunks, base_name=os.path.splitext(blob_filename)[0])

        # Upload results
        result_blob_name = f"{os.path.splitext(blob_filename)[0]}_transcription.json"
        result_path = os.path.join(AUDIO_TRANSCRIPTIONS_PATH, f"{os.path.splitext(blob_filename)[0]}_combined_transcription.json")
        log_blob_name = f"{os.path.splitext(blob_filename)[0]}_log.txt"

        with open(result_path, "r", encoding="utf-8") as f:
            transcription_data = json.load(f)

        result_payload = {
            "formatted_utc_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "id_usuario": id_user,
            "id_video": id_video,
            "id_blob": result_blob_name,
            "id_container": OUTPUT_CONTAINER,
            "words_count": len(transcription_data),
            "transcriptions": transcription_data,
            "end_data": True
        }

        client = await storage.get_blob_service_client()
        container = client.get_container_client(OUTPUT_CONTAINER)

        await container.get_blob_client(result_blob_name).upload_blob(json.dumps(result_payload), overwrite=True)

        log_content = f"Finished in {(time.time() - start_time):.2f}s"
        await container.get_blob_client(log_blob_name).upload_blob(log_content, overwrite=True)

        logger.info(f"Upload complete for {result_blob_name}")

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
    logger.info("Starting AI Speech Transcription Service...")
    asyncio.run(main())
