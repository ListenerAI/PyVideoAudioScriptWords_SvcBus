# main.py

import ai_speech_transcription_processor_stablev1 as processor
import logging

if __name__ == "__main__":
    logging.info("Starting audio transcription processing pipeline...")

    # Step 1: Clean temporary audio segment files
    processor.clean_related_files(processor.AUDIO_TRANSCRIPTIONS_PATH, processor.VIDEO_INPUT_PATH)

    # Step 2: Extract audio from video
    if processor.extract_audio(processor.VIDEO_INPUT_PATH, processor.AUDIO_OUTPUT_PATH):
        chunks = processor.split_audio(processor.AUDIO_OUTPUT_PATH, processor.AUDIO_TRANSCRIPTIONS_PATH)
        if chunks:
            logging.info(f"Audio chunks created: {chunks}")
            processor.process_audio_chunks(chunks)
        else:
            logging.error("Failed to split audio.")
    else:
        logging.error("Audio extraction failed.")
