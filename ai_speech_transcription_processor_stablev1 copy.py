import os
import requests
import logging
import json
import re
import base64
import subprocess
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path configuration
FFMPEG_PATH = r"C:\ffmpeg\bin\ffmpeg.exe"
#VIDEO_INPUT_PATH = r"C:\dev\Pod_Cast_Matthew_30s.mp4"
#VIDEO_INPUT_PATH = r"C:\dev\Mujer entre 30 a 35 anos.mp4"
VIDEO_INPUT_PATH = r"C:\Github\AudioToTextTiming\00_Equalizer_90s.mp4"
#AUDIO_OUTPUT_PATH = r"C:\dev\extracted_audio.wav"
AUDIO_OUTPUT_PATH = r"C:\Github\AudioToTextTiming\00_Equalizer_90s_audio.wav"
AUDIO_TRANSCRIPTIONS_PATH = r"C:\Github\AudioToTextTiming"

# Azure Cognitive Services configuration
COGNITIVE_API_KEY = "149a326359244a2faa89acf4ad0d4396"
COGNITIVE_ENDPOINT = "https://eastus.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=en-US&format=detailed"


#def clean_related_files(directory, video_file_name):
 #   """
  #  Deletes files in the specified directory that are related to the input video file.
   # """
    #try:
     #   video_base_name = os.path.splitext(os.path.basename(video_file_name))[0]
      #  files_to_delete = [os.path.join(directory, file) for file in os.listdir(directory) if video_base_name in file]

       # for file_path in files_to_delete:
        #    if os.path.isfile(file_path):
         #       os.remove(file_path)
          #      logging.info(f"Deleted file: {file_path}")

       # if not files_to_delete:
        #    logging.info("No related files found to delete.")
   # except Exception as e:
    #    logging.error(f"Error cleaning related files: {e}", exc_info=True)

def clean_related_files(directory, video_file_name):
    """
    Deletes only temporary .wav audio segment files related to the video input.
    Keeps .json transcription files and the original video.
    """
    try:
        video_base_name = os.path.splitext(os.path.basename(video_file_name))[0]
        files_to_delete = [
            os.path.join(directory, file)
            for file in os.listdir(directory)
            if video_base_name in file and file.endswith(".wav")
        ]

        for file_path in files_to_delete:
            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Deleted audio segment file: {file_path}")

        if not files_to_delete:
            logging.info("No audio segment files found to delete.")
    except Exception as e:
        logging.error(f"Error cleaning temporary audio files: {e}", exc_info=True)


def extract_audio(video_path, audio_path):
    """
    Extracts audio from a video using FFmpeg and saves it to a file.
    """
    try:
        logging.info(f"Extracting audio from {video_path} to {audio_path}")
        command = [
            FFMPEG_PATH,
            "-y",
            "-i", video_path,
            "-vn",
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


def split_audio(audio_path, output_dir, chunk_duration=60):
    """
    Splits an audio file into chunks of the specified duration, naming them based on the input video file name.
    """
    try:
        video_base_name = os.path.splitext(os.path.basename(VIDEO_INPUT_PATH))[0]
        output_pattern = os.path.join(output_dir, f"{video_base_name}_segment_%03d.wav")

        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Splitting audio file: {audio_path} into {chunk_duration}-second chunks.")

        command = [
            FFMPEG_PATH,
            "-i", audio_path,
            "-f", "segment",
            "-segment_time", str(chunk_duration),
            "-ar", "16000",
            "-ac", "1",
            "-c:a", "pcm_s16le",
            output_pattern
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            logging.error(f"FFmpeg error: {result.stderr.decode('utf-8')}")
            return []
        chunks = sorted([os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith(video_base_name)])
        logging.info(f"Generated chunks: {chunks}")
        return chunks
    except Exception as e:
        logging.error(f"Error splitting audio: {e}", exc_info=True)
        return []


def audio_to_text_cognitive_services(audio_file_path):
    """
    Sends an audio file to the Azure Cognitive Services Speech-to-Text API and processes the response.
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

            pron_assessment_params = {
                "ReferenceText": display_audio_to_text,
                "GradingSystem": "HundredMark",
                "Granularity": "Word",
                "Dimension": "Comprehensive"
            }

            pron_assessment_params_json = json.dumps(pron_assessment_params)
            pron_assessment_params_bytes = pron_assessment_params_json.encode('utf-8')
            pron_assessment_header = base64.b64encode(pron_assessment_params_bytes).decode('utf-8')

            headers.update({
                'Pronunciation-Assessment': pron_assessment_header
            })

            response = requests.post(COGNITIVE_ENDPOINT, headers=headers, data=audio_data)
            if response.status_code == 200:
                transcriptions = response.json()
                return {
                    "offset_in_seconds": offset_in_seconds,
                    "duration_in_seconds": duration_in_seconds,
                    "masked_itn": masked_itn,
                    "censored_words_count": censored_words_count,
                    "display_audio_to_text": display_audio_to_text,
                    "words_count": words_count,
                    "transcriptions": transcriptions
                }
            else:
                logging.error(f"Pronunciation assessment error: {response.status_code} {response.text}")
                return None
        else:
            logging.error(f"Speech-to-Text error: {response.status_code} {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error processing audio: {e}", exc_info=True)
        return None


def convert_word_timings_to_seconds(processed_response, chunk_name, chunk_duration=60):
    """
    Converts the offset and duration of each word to mm:ss:ms format,
    and adjusts the timestamps based on the chunk name and chunk duration.

    Args:
        processed_response (dict): The transcription response object from Azure Cognitive Services.
        chunk_name (str): The name of the chunk file.
        chunk_duration (int): The duration of each audio chunk in seconds.

    Returns:
        list: A list of words with updated start and stop times in mm:ss:ms format.
    """
    if "transcriptions" in processed_response:
        nbest = processed_response["transcriptions"].get("NBest", [])
        if nbest:  # Ensure there is at least one item in the NBest list
            words = nbest[0].get("Words", [])

            # Extract the segment index from the chunk name
            segment_index = int(chunk_name.split('_')[-1].split('.')[0])

            # Calculate time offset in milliseconds
            time_offset_ms = chunk_duration * (segment_index - 1) * 1000 if segment_index > 1 else 0

            def format_mm_ss_ms(total_milliseconds):
                """
                Converts total milliseconds into mm:ss:ms format and shifts milliseconds up two places.
                """
                total_seconds = total_milliseconds // 1000
                milliseconds = total_milliseconds % 1000
                minutes = total_seconds // 60
                seconds = total_seconds % 60
                return f"{minutes:02}:{seconds:02}:{milliseconds:03}"

            previous_stop_time_ms = 0  # Track the stop time of the previous word in milliseconds

            # Adjust each word's start and stop time with the calculated time offset
            for word in words:
                start_time_total_ms = (word["Offset"] // 10000) + time_offset_ms
                stop_time_total_ms = ((word["Offset"] + word["Duration"]) // 10000) + time_offset_ms

                # Ensure start and stop times are sequential
                if start_time_total_ms < previous_stop_time_ms:
                    start_time_total_ms = previous_stop_time_ms

                if stop_time_total_ms <= start_time_total_ms:
                    stop_time_total_ms = start_time_total_ms + 100  # Ensure a minimum duration of 100 ms

                # Convert to mm:ss:ms format
                word["start_time"] = format_mm_ss_ms(start_time_total_ms)
                word["stop_time"] = format_mm_ss_ms(stop_time_total_ms)

                # Update previous_stop_time_ms
                previous_stop_time_ms = stop_time_total_ms

            return words
        else:
            logging.error("NBest list is empty in the processed response.")
            return []
    else:
        logging.error("No transcriptions found in processed response.")
        return []


def process_audio_chunks(chunks, base_name=None):
    """
    Processes each audio chunk using cognitive services, adjusts word timings based on segment index,
    generates JSON output for each chunk, and creates a combined JSON file.
    """
    if base_name is None:
        base_name = os.path.splitext(os.path.basename(VIDEO_INPUT_PATH))[0]

    combined_transcription = []
    for index, chunk in enumerate(chunks, start=1):
        # Generate individual transcription path
        individual_output_path = os.path.join(
            AUDIO_TRANSCRIPTIONS_PATH, f"{base_name}_segment_{index:03d}_transcription.json"
        )

        # Process the chunk
        logging.info(f"Processing chunk: {chunk}")
        cognitive_response = audio_to_text_cognitive_services(chunk)

        if cognitive_response:
            # Convert word timings for this chunk
            word_timings = convert_word_timings_to_seconds(cognitive_response, f"{base_name}_segment_{index}")
            combined_transcription.extend(word_timings)

            # Save the individual transcription to a file
            with open(individual_output_path, "w", encoding="utf-8") as individual_file:
                json.dump(word_timings, individual_file, indent=2)
            logging.info(f"Saved individual transcription to {individual_output_path}")

    # Save the combined transcription
    combined_output_path = os.path.join(AUDIO_TRANSCRIPTIONS_PATH, f"{base_name}_combined_transcription.json")
    with open(combined_output_path, "w", encoding="utf-8") as combined_file:
        json.dump(combined_transcription, combined_file, indent=2)
    logging.info(f"Combined transcription saved to {combined_output_path}")



if __name__ == "__main__":
    # Step 1: Clean files related to the input video file
    clean_related_files(AUDIO_TRANSCRIPTIONS_PATH, VIDEO_INPUT_PATH)

    # Step 2: Extract audio and process the workflow
    if extract_audio(VIDEO_INPUT_PATH, AUDIO_OUTPUT_PATH):
        chunks = split_audio(AUDIO_OUTPUT_PATH, AUDIO_TRANSCRIPTIONS_PATH)
        if chunks:
            logging.info(f"Audio chunks created: {chunks}")
            process_audio_chunks(chunks)
        else:
            logging.error("Failed to split audio.")
    else:
        logging.error("Audio extraction failed.")
