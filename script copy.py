import os
import logging
import json
import re
import subprocess
from glob import glob

# === CONFIGURATION ===
FFMPEG_PATH = r"C:\ffmpeg\bin\ffmpeg.exe"
VIDEO_SOURCE_PATH = r"C:\github\s2t\2-34edbujq3omo7mu7zc-00_Equalizer_180s.mp4"
CHUNK_OUTPUT_DIR = r"C:\github\s2t"
CHUNK_DURATION = 30  # seconds

# Azure Speech
COGNITIVE_API_KEY = "149a326359244a2faa89acf4ad0d4396"
COGNITIVE_ENDPOINT = "https://eastus.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=en-US&format=detailed"

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def format_ms(ms):
    seconds = ms // 1000
    minutes = seconds // 60
    sec = seconds % 60
    millis = ms % 1000
    return f"{minutes:02}:{sec:02}:{millis:03}"


def extract_audio(video_path, audio_path):
    try:
        command = [
            FFMPEG_PATH, "-y", "-i", video_path,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", audio_path
        ]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except Exception as e:
        logging.error(f"❌ Audio extraction failed for {video_path}: {e}")
        return False


def audio_to_text_cognitive_services(audio_file_path):
    import base64
    import requests

    headers = {
        'Ocp-Apim-Subscription-Key': COGNITIVE_API_KEY,
        'Content-type': 'audio/wav; codecs=audio/pcm; samplerate=16000'
    }

    try:
        with open(audio_file_path, 'rb') as audio_file:
            audio_data = audio_file.read()

        # Initial STT call
        response = requests.post(COGNITIVE_ENDPOINT, headers=headers, data=audio_data)
        if response.status_code != 200:
            logging.error(f"❌ Speech-to-text error: {response.status_code} {response.text}")
            return None

        response_data = response.json()
        display_text = response_data.get("DisplayText", "")
        pron_assessment_params = {
            "ReferenceText": display_text,
            "GradingSystem": "HundredMark",
            "Granularity": "Word",
            "Dimension": "Comprehensive"
        }

        headers['Pronunciation-Assessment'] = base64.b64encode(json.dumps(pron_assessment_params).encode()).decode('utf-8')
        response = requests.post(COGNITIVE_ENDPOINT, headers=headers, data=audio_data)
        return response.json() if response.status_code == 200 else None

    except Exception as e:
        logging.error(f"❌ Exception during speech-to-text: {e}")
        return None


def convert_word_timings_to_seconds(response_data, chunk_name):
    def format_hh_mm_ss_ms(ms_total):
        hours = ms_total // (60 * 60 * 1000)
        minutes = (ms_total % (60 * 60 * 1000)) // (60 * 1000)
        seconds = (ms_total % (60 * 1000)) // 1000
        milliseconds = ms_total % 1000
        return f"{hours:02}:{minutes:02}:{seconds:02}:{milliseconds:03}"

    # ✅ Static dictionary mapping clip IDs to offsets in seconds
    clip_offset_seconds = {
        "clip-001": 30, "clip-002": 60, "clip-003": 90, "clip-004": 120, "clip-005": 150,
        "clip-006": 180, "clip-007": 210, "clip-008": 240, "clip-009": 270, "clip-010": 300,
        "clip-011": 330, "clip-012": 360, "clip-013": 390, "clip-014": 420, "clip-015": 450,
        "clip-016": 480, "clip-017": 510, "clip-018": 540, "clip-019": 570, "clip-020": 600,
        "clip-021": 630, "clip-022": 660, "clip-023": 690, "clip-024": 720, "clip-025": 750,
        "clip-026": 780, "clip-027": 810, "clip-028": 840, "clip-029": 870, "clip-030": 900,
        "clip-031": 930, "clip-032": 960, "clip-033": 990, "clip-034": 1020, "clip-035": 1050
    }

    # ✅ Extract "clip-XXX" identifier from chunk name
    match = re.search(r"(clip-\d{3})", chunk_name)
    clip_id = match.group(1) if match else None
    offset_ms = clip_offset_seconds.get(clip_id, 0) * 1000  # seconds → ms

    words = []
    try:
        word_entries = response_data.get("NBest", [{}])[0].get("Words", [])
        for word in word_entries:
            original_offset = word["Offset"]
            original_duration = word["Duration"]
            original_end = original_offset + original_duration

            # Shifted time based on dictionary offset
            start_ms = (original_offset // 10_000) + offset_ms
            end_ms = (original_end // 10_000) + offset_ms

            words.append({
                **word,
                "start_original": original_offset,
                "end_original": original_end,
                "word start time": format_hh_mm_ss_ms(start_ms),
                "word end time": format_hh_mm_ss_ms(end_ms)
            })
    except Exception as e:
        logging.warning(f"⚠️ Failed to convert word timings for {chunk_name}: {e}")
    return words






def split_video_into_chunks():
    logging.info("🎬 Splitting video into 30s chunks")
    base_name = os.path.splitext(os.path.basename(VIDEO_SOURCE_PATH))[0]
    output_template = os.path.join(CHUNK_OUTPUT_DIR, f"{base_name}-clip-%03d.mp4")

    command = [
        FFMPEG_PATH, "-i", VIDEO_SOURCE_PATH,
        "-c", "copy", "-map", "0",
        "-f", "segment", "-segment_time", str(CHUNK_DURATION),
        output_template
    ]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    chunk_files = sorted(glob(os.path.join(CHUNK_OUTPUT_DIR, f"{base_name}-clip-*.mp4")))
    logging.info(f"✅ Created {len(chunk_files)} video chunks.")
    return chunk_files


def process_all_video_chunks():
    video_chunks = split_video_into_chunks()

    for chunk_path in video_chunks:
        base_name = os.path.splitext(os.path.basename(chunk_path))[0]
        chunk_number_match = re.search(r"clip-(\d+)", base_name)
        chunk_index = int(chunk_number_match.group(1)) if chunk_number_match else 1

        audio_output = os.path.join(CHUNK_OUTPUT_DIR, f"{base_name}.wav")
        json_output = os.path.join(CHUNK_OUTPUT_DIR, f"{base_name}_transcription.json")

        logging.info(f"🔍 Processing Chunk #{chunk_index}: {chunk_path}")

        if not extract_audio(chunk_path, audio_output):
            continue

        response_data = audio_to_text_cognitive_services(audio_output)
        if not response_data:
            continue

        word_timings = convert_word_timings_to_seconds(response_data, base_name)
        with open(json_output, "w", encoding="utf-8") as f:
            json.dump(word_timings, f, indent=2)

        logging.info(f"📝 Transcription saved: {json_output}")


# Entry
if __name__ == "__main__":
    process_all_video_chunks()
