import os
import json
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from dotenv import load_dotenv

load_dotenv(dotenv_path="app.env")

service_bus_connection_str = os.environ.get("SERVICE_BUS_CONNECTION_STR")
topic_name = os.environ.get("SERVICE_BUS_TOPIC_NAME")

async def send_test_message():
    async with ServiceBusClient.from_connection_string(service_bus_connection_str) as client:
        sender = client.get_topic_sender(topic_name=topic_name)
        async with sender:
            # Manually defined message payload
            message_payload = {
                "id_user": "00",
                # "id_video": "LV_Women_15sec",
                # "id_video": "Marvel_F4_1_min",
                # "id_video": "Equalizer_90s",
                # "id_video": "Equalizer_120s",
                # "id_video": "Equalizer_150",
                # "id_video": "Equalizer_180s",
                "id_video": "Equalizer_60s"
            }

            # Use a matching session_id
            message = ServiceBusMessage(
                json.dumps(message_payload),
                session_id="17_Equalizer_60s"  # Required for session-enabled topics
            )

            await sender.send_messages(message)
            print("Test message sent with session_id")

# Run it as a standalone script
if __name__ == "__main__":
    asyncio.run(send_test_message())