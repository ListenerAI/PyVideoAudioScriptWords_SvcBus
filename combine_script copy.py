import os
import json
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def combine_transcript_jsons(directory, id_video):
    output_filename = f"{id_video}-audioTextResult.json"
    output_path = os.path.join(directory, output_filename)

    logging.info(f"📂 Scanning directory: {directory}")
    logging.info(f"🎯 Looking for transcriptions starting with: {id_video}")

    # Match transcription files related to this id_video
    transcript_files = [
        f for f in os.listdir(directory)
        if re.match(rf"^{re.escape(id_video)}-clip-\d{{3}}_transcription\.json$", f)
    ]

    if not transcript_files:
        logging.warning("⚠️ No matching transcription files found.")
        return

    # Sort files by clip number
    transcript_files.sort(key=lambda f: int(re.search(r"clip-(\d{3})", f).group(1)))

    combined_words = []
    for filename in transcript_files:
        path = os.path.join(directory, filename)
        with open(path, "r", encoding="utf-8") as file:
            try:
                words = json.load(file)
                combined_words.extend(words)
                logging.info(f"✅ Merged: {filename}")
            except json.JSONDecodeError:
                logging.warning(f"❌ Skipped invalid JSON: {filename}")

    with open(output_path, "w", encoding="utf-8") as outfile:
        json.dump(combined_words, outfile, indent=2)

    logging.info(f"📄 Combined JSON saved to: {output_path}")
    return output_path


