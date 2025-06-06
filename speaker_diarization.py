import os
import json
import logging
import time
import uuid
import requests
import subprocess
import wave
from collections import defaultdict

COGNITIVE_API_KEY = "DlDkrEMCBEW3VtxuT8EtcHNMFvhlbwgngwneAEXcvu29g3mGdhuvJQQJ99BAACHYHv6XJ3w3AAAYACOGs899"
COGNITIVE_REGION = "eastus2"
LANGUAGE = "en-US"

INPUT_CONTAINER = "computervision"
OUTPUT_CONTAINER = "mediatestingdata"

def check_audio_format_compatibility(audio_path):
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=codec_name,channels,sample_rate,bit_rate",
            "-of", "json", audio_path
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            logging.warning(f"⚠️ Failed to read audio format: {result.stderr}")
            return

        info = json.loads(result.stdout)
        stream = info["streams"][0]

        codec = stream.get("codec_name")
        channels = int(stream.get("channels", 0))
        sample_rate = int(stream.get("sample_rate", 0))
        bit_rate = int(stream.get("bit_rate", 0))

        logging.info(f"🎧 Audio Format Check: codec={codec}, channels={channels}, sample_rate={sample_rate}, bit_rate={bit_rate}")

        if codec != "pcm_s16le":
            logging.error("❌ Audio format is NOT PCM 16-bit.")
        elif not (16000 <= sample_rate <= 48000):
            logging.error("❌ Sample rate is out of acceptable range (16kHz–48kHz).")
        elif channels not in [1, 2]:
            logging.error("❌ Audio must be mono or stereo.")
        else:
            logging.info("✅ Audio format is compatible with Azure Speech Services.")
    except Exception as e:
        logging.warning(f"⚠️ Error checking audio format: {e}")

def get_audio_duration(audio_path):
    try:
        with wave.open(audio_path, 'rb') as wf:
            frames = wf.getnframes()
            rate = wf.getframerate()
            duration = frames / float(rate)
            logging.info(f"⏱️ Audio duration: {duration:.2f} seconds")
            return duration
    except Exception as e:
        logging.warning(f"⚠️ Could not calculate audio duration: {e}")
        return 0

def reencode_wav(audio_path):
    reencoded_path = audio_path.replace(".wav", "_fixed.wav")
    cmd = [
        "ffmpeg", "-y", "-i", audio_path,
        "-ac", "1", "-ar", "16000", "-sample_fmt", "s16",
        reencoded_path
    ]
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return reencoded_path

async def run_speaker_diarization_from_audio(audio_path: str, video_filename: str, blob_client):
    try:
        logging.info(f"🔍 Running batch speaker diarization on: {audio_path}")
        check_audio_format_compatibility(audio_path)
        get_audio_duration(audio_path)

        fixed_audio_path = reencode_wav(audio_path)

        client = await blob_client.get_blob_service_client()
        input_container = client.get_container_client(INPUT_CONTAINER)

        blob_name = os.path.basename(fixed_audio_path)
        with open(fixed_audio_path, "rb") as f:
            await input_container.get_blob_client(blob_name).upload_blob(f.read(), overwrite=True)
        logging.info(f"✅ Uploaded audio blob: {blob_name}")

        # ✅ Fix for malformed blob_url
        accountname = os.getenv("ACCOUNTNAME", "").replace("https://", "").replace(".blob.core.windows.net", "")
        blob_url = f"https://{accountname}.blob.core.windows.net/{INPUT_CONTAINER}/{blob_name}"
        logging.info(f"🔗 Audio blob URL: {blob_url}")

        transcription_url = f"https://{COGNITIVE_REGION}.api.cognitive.microsoft.com/speechtotext/v3.1/transcriptions"
        transcription_id = str(uuid.uuid4())

        definition = {
            "displayName": f"Transcription-{transcription_id}",
            "locale": LANGUAGE,
            "contentUrls": [blob_url],
            "properties": {
                "diarizationEnabled": True,
                "wordLevelTimestampsEnabled": True,
                "punctuationMode": "DictatedAndAutomatic",
                "profanityFilterMode": "Masked"
            }
        }

        headers = {
            "Ocp-Apim-Subscription-Key": COGNITIVE_API_KEY,
            "Content-Type": "application/json"
        }

        logging.info(f"📤 Sending transcription job: {json.dumps(definition, indent=2)}")
        response = requests.post(transcription_url, headers=headers, json=definition)
        logging.info(f"📡 Response status: {response.status_code}")

        if response.status_code not in [201, 202]:
            logging.error(f"❌ Transcription creation failed: {response.status_code} - {response.text}")
            return None

        transcription_location = response.headers["Location"]
        logging.info(f"📍 Polling transcription job at: {transcription_location}")

        while True:
            status_res = requests.get(transcription_location, headers=headers)
            status_json = status_res.json()
            status = status_json.get("status")
            logging.info(f"🔁 Transcription status: {status}")
            if status in ["Succeeded", "Failed"]:
                break
            time.sleep(10)

        if status != "Succeeded":
            logging.error("❌ Transcription job failed.")
            return None

        result_url = status_json["resultsUrls"]["transcription"]
        result_json = requests.get(result_url).json()

        base_filename = os.path.splitext(os.path.basename(video_filename))[0] + f"_{LANGUAGE}"
        diarization_filename = f"{base_filename}_speaker_diarization.json"
        with open(diarization_filename, "w", encoding="utf-8") as f:
            json.dump(result_json, f, indent=2)

        speaker_blocks = defaultdict(list)
        for phrase in result_json.get("recognizedPhrases", []):
            speaker = f"Speaker {phrase.get('speaker', '?')}"
            speaker_blocks[speaker].append({
                "start": phrase.get("offset"),
                "end": phrase.get("duration"),
                "text": phrase.get("display", "")
            })

        speaker_block_output = [{"speaker": s, "segments": segs} for s, segs in speaker_blocks.items()]
        speaker_block_filename = f"{base_filename}_speaker_blocks.json"
        with open(speaker_block_filename, "w", encoding="utf-8") as f:
            json.dump(speaker_block_output, f, indent=2)

        summary_data = {s: f"Spoke {len(segs)} time(s)." for s, segs in speaker_blocks.items()}
        summary_filename = f"{base_filename}_speaker_summary.json"
        with open(summary_filename, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2)

        output_container = client.get_container_client(OUTPUT_CONTAINER)
        for file in [diarization_filename, speaker_block_filename, summary_filename]:
            with open(file, "r", encoding="utf-8") as f:
                await output_container.get_blob_client(file).upload_blob(f.read().encode("utf-8"), overwrite=True)
            logging.info(f"✅ Uploaded: {file}")

        logging.info("✅ Batch diarization complete.")
        return diarization_filename

    except Exception as e:
        logging.error(f"❌ Error in batch diarization: {e}", exc_info=True)
        return None
