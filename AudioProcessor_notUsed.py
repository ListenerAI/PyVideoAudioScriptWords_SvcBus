import subprocess
import requests
import logging
import json
import re
import base64

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path configuration
FFMPEG_PATH = r"C:\ffmpeg\bin\ffmpeg.exe"
VIDEO_INPUT_PATH = r"C:\Github\AudioToTextTiming\36-00_Equalizer_60s.mp4"
AUDIO_OUTPUT_PATH = r"C:\Github\AudioToTextTiming\36-00_Equalizer_60s_audio.wav"

# Azure Cognitive Services configuration
COGNITIVE_API_KEY = "149a326359244a2faa89acf4ad0d4396"  # Replace with your key
COGNITIVE_ENDPOINT = "https://eastus.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=en-US&format=detailed"

def extract_audio(video_path, audio_path):
    """
    Extracts audio from a video using FFmpeg and saves it to a file.

    Args:
        video_path (str): Path to the input video file.
        audio_path (str): Path to save the extracted audio file.

    Returns:
        bool: True if extraction is successful, False otherwise.
    """
    try:
        logging.info(f"Extracting audio from {video_path} to {audio_path}")
        command = [
            FFMPEG_PATH,
            "-y",  # Overwrite output
            "-i", video_path,
            "-vn",  # Disable video
            "-acodec", "pcm_s16le",
            "-ar", "16000",
            "-ac", "1",
            audio_path
        ]

        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            logging.error(f"FFmpeg error: {result.stderr.decode('utf-8')}")
            return False

        logging.info("Audio extracted successfully.")
        return True
    except Exception as e:
        logging.error(f"Error extracting audio: {e}", exc_info=True)
        return False

def calculate_word_timings(word):
    """
    Calculates start and stop times for a word in seconds with two decimal places.

    Args:
        word (dict): A dictionary containing 'Offset' and 'Duration' in nanoseconds.

    Returns:
        dict: The word dictionary with added 'start_time' and 'stop_time'.
    """
    start_time = round(word["Offset"] / 1_000_000_000, 2)
    stop_time = round((word["Offset"] + word["Duration"]) / 1_000_000_000, 2)
    word["start_time"] = start_time
    word["stop_time"] = stop_time
    return word

def semantic_script_model(transcriptions):
    """
    Processes the transcription object to add start and stop times for words.

    Args:
        transcriptions (dict): Transcription object containing word details.

    Returns:
        list: A list of words with start and stop times included.
    """
    words = transcriptions.get("NBest", [])[0].get("Words", [])
    for word in words:
        calculate_word_timings(word)
    return words

def process_transcription_with_semantics(transcription_response):
    """
    Processes the transcription response by adding word timings.

    Args:
        transcription_response (dict): The transcription response from the API.

    Returns:
        dict: The updated response including semantic word timings.
    """
    transcriptions = transcription_response.get("transcriptions", {})
    words = semantic_script_model(transcriptions)  # Add semantic details
    transcription_response["words"] = words
    return transcription_response

def audio_to_text_cognitive_services(audio_file_path):
    """
    Sends an audio file to the Azure Cognitive Services Speech-to-Text API and processes the response.

    Args:
        audio_file_path (str): Path to the audio file to be sent.

    Returns:
        dict: Processed response with detailed transcription and word-level data.
    """
    headers = {
        'Ocp-Apim-Subscription-Key': COGNITIVE_API_KEY,
        'Content-type': 'audio/wav; codecs=audio/pcm; samplerate=16000'
    }
    
    try:
        with open(audio_file_path, 'rb') as audio_file:
            audio_data = audio_file.read()

        response = requests.post(COGNITIVE_ENDPOINT, headers=headers, data=audio_data)

        if response.status_code == 200:
            response_data = response.json()

            # Process response data
            offset_in_seconds = response_data["Offset"] / 10_000_000  # Convert Offset to seconds
            duration_in_seconds = response_data["Duration"] / 10_000_000  # Convert Duration to seconds
            masked_itn = response_data["NBest"][0]["MaskedITN"]
            censored_words_count = masked_itn.count("****")
            display_audio_to_text = response_data["DisplayText"].replace('.', ' ').replace(',', ' ')
            words_count = len(re.findall(r'\w+', display_audio_to_text))

            return {
                "offset_in_seconds": offset_in_seconds,
                "duration_in_seconds": duration_in_seconds,
                "masked_itn": masked_itn,
                "censored_words_count": censored_words_count,
                "display_audio_to_text": display_audio_to_text,
                "words_count": words_count,
                "transcriptions": response_data
            }
        else:
            logging.error(f"Speech-to-Text error: {response.status_code} {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error processing audio: {e}", exc_info=True)
        return None

def semantic_temporal_script_model(processed_response):
    """
    Converts word timings in the processed response to include start and stop times.

    Args:
        processed_response (dict): The final processed transcription response.

    Returns:
        dict: Updated response with temporal information added.
    """
    if "words" in processed_response:
        for word in processed_response["words"]:
            word["start_time"] = round(word["Offset"] / 1_000_000_000, 2)
            word["stop_time"] = round((word["Offset"] + word["Duration"]) / 1_000_000_000, 2)
    return processed_response

if __name__ == "__main__":
    # Test: Extract audio and send to Cognitive Services
    if extract_audio(VIDEO_INPUT_PATH, AUDIO_OUTPUT_PATH):
        transcription_response = audio_to_text_cognitive_services(AUDIO_OUTPUT_PATH)
        if transcription_response:
            processed_response = process_transcription_with_semantics(transcription_response)
            final_response = semantic_temporal_script_model(processed_response)
            logging.info(f"Final Processed Response:\n{json.dumps(final_response, indent=2)}")
        else:
            logging.error("Failed to process response from Cognitive Services.")