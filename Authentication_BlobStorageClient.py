from azure.storage.blob.aio import BlobServiceClient
import os
import aiohttp
import asyncio
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class BlobStorageClient:
    def __init__(self):
        self.account_key = os.environ.get('ACCOUNTKEY')
        self.account_name = os.environ.get('ACCOUNTNAME')
        self.container_name = os.environ.get('CONTAINERNAME')

        if not all([self.account_key, self.account_name, self.container_name]):
            raise EnvironmentError("Missing one or more required environment variables: ACCOUNTKEY, ACCOUNTNAME, CONTAINERNAME")

        self.blob_service_client = None

    async def _initialize_client(self):
        if self.blob_service_client is None:
            self.blob_service_client = BlobServiceClient(
                account_url=self.account_name,
                credential=self.account_key
            )

    async def get_blob_service_client(self):
        await self._initialize_client()
        return self.blob_service_client

    async def close(self):
        if self.blob_service_client:
            await self.blob_service_client.close()

    async def upload_blob(self, container_name, blob_name, file_path_or_bytes, overwrite=True):
        client = await self.get_blob_service_client()
        container_client = client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        if isinstance(file_path_or_bytes, (bytes, bytearray)):
            data = file_path_or_bytes
        elif isinstance(file_path_or_bytes, str) and os.path.exists(file_path_or_bytes):
            with open(file_path_or_bytes, "rb") as f:
                data = f.read()
        else:
            raise ValueError("❌ Invalid input to upload_blob: must be file path or bytes")

        await blob_client.upload_blob(data, overwrite=overwrite)


    async def download_blob(self, search_pattern, download_file_path):
        """
        Downloads a blob matching exact filename (excluding extension).
        - If blob name includes "-clip-N": require exact match (excluding extension).
        - Else: allow prefix match (excluding extension).
        """
        found = False
        normalized_pattern = os.path.splitext(search_pattern)[0]

        try:
            await self._initialize_client()
            container_client = self.blob_service_client.get_container_client(self.container_name)

            async for blob in container_client.list_blobs():
                blob_basename = os.path.splitext(blob.name)[0]
                is_clip_file = "-clip-" in blob_basename

                match = (
                    blob_basename == normalized_pattern if is_clip_file
                    else blob_basename.startswith(normalized_pattern)
                )

                if match:
                    logger.info(f"Found matching blob: {blob.name}")

                    dir_path = os.path.dirname(download_file_path)
                    if dir_path:
                        os.makedirs(dir_path, exist_ok=True)

                    blob_client = self.blob_service_client.get_blob_client(
                        container=self.container_name,
                        blob=blob.name
                    )

                    start_time = asyncio.get_event_loop().time()
                    stream = await blob_client.download_blob(max_concurrency=4)

                    with open(download_file_path, "wb") as f:
                        await stream.readinto(f)

                    duration = asyncio.get_event_loop().time() - start_time
                    logger.info(
                        f"Downloaded file '{blob.name}' from container '{self.container_name}' "
                        f"to '{download_file_path}'. Total time: {duration:.2f}s"
                    )

                    found = True
                    break

            if not found:
                logger.warning(f"No blob matched the search pattern: {normalized_pattern}")
            return found

        except aiohttp.ClientError as e:
            logger.error(f"Network error while downloading blob: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while downloading blob: {type(e).__name__}: {e}")
            return False
