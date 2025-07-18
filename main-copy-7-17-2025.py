import asyncio
import os
import json
import time
import logging
from datetime import datetime
from azure.servicebus.aio import AutoLockRenewer
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
        is_wav = clip_filename.endswith(".wav")
        input_container = os.getenv("INPUT_CONTAINER_VOCALS") if is_wav else os.getenv("INPUT_CONTAINER")



        #blob_filename = clip_filename if clip_filename.endswith(".mp4") else f"{clip_filename}.mp4"
        if clip_filename.endswith(".wav") or clip_filename.endswith(".mp4"):
            blob_filename = clip_filename
        else:
            blob_filename = f"{clip_filename}.mp4"

        download_path = os.path.join(DATA_DIR, blob_filename)

        storage = BlobStorageClient()
        storage.container_name = input_container  # Set correct container before download

        if not await storage.download_blob(blob_filename, download_path):
            logger.error(f"Blob not found in {input_container}: {blob_filename}")
            return


        await process_all_video_chunks(download_path)

        # ✅ Combine all transcription files after chunk processing
        base_video_id = os.path.splitext(os.path.basename(clip_filename))[0].split("-clip")[0]
        await combine_transcript_jsons(base_video_id)

        logger.info(f"✅ Finished processing: {blob_filename} in {(time.time() - start_time):.2f}s")

    except Exception as e:
        logger.exception(f"Error processing message: {e}")

async def main():
    async with ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR) as client:
        receivers = [
            client.get_subscription_receiver(
                topic_name=os.getenv("SERVICE_BUS_TOPIC_NAME"),
                subscription_name=os.getenv("SERVICE_BUS_SUBSCRIPTION_NAME")
            ),
            client.get_subscription_receiver(
                topic_name=os.getenv("INBOUND_VOCALS_SERVICE_BUS_TOPIC"),
                subscription_name=os.getenv("INBOUND_VOCALS_SERVICE_BUS_SUBSCRIPTION")
            )
        ]

        async with receivers[0], receivers[1]:
            while True:
                for receiver in receivers:
                    try:
                        messages = await receiver.receive_messages(max_message_count=1, max_wait_time=5)
                        if messages:
                            for msg in messages:
                                logger.info(f"📥 Received message ID: {msg.message_id}")

                                # ✅ Use lock renewal in context
                                async with AutoLockRenewer() as auto_lock_renewer:
                                    auto_lock_renewer.register(receiver, msg, max_lock_renewal_duration=900)

                                    await process_message(msg)

                                    try:
                                        logger.info(f"📝 Attempting to complete message ID: {msg.message_id}")
                                        await receiver.complete_message(msg)
                                        logger.info(f"✅ Message completed and removed from queue: {msg.message_id}")
                                    except Exception as e:
                                        logger.error(f"❌ Failed to complete message ID: {msg.message_id} — {e}")
                        else:
                            logger.info("😴 No message received. Waiting...")
                    except Exception as e:
                        logger.exception(f"🔁 Receiver loop error: {e}")
                await asyncio.sleep(1)




if __name__ == "__main__":
    logger.info("Starting AI Video Transcription Service...")
    asyncio.run(main())
