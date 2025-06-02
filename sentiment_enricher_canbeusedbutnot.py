import os
import json
import requests
import logging
from dotenv import load_dotenv
from difflib import SequenceMatcher

load_dotenv()

AZURE_TEXT_ANALYTICS_KEY = os.getenv("AZURE_TEXT_ANALYTICS_KEY")
AZURE_TEXT_ANALYTICS_ENDPOINT = os.getenv("AZURE_TEXT_ANALYTICS_ENDPOINT")
SENTIMENT_URL = f"{AZURE_TEXT_ANALYTICS_ENDPOINT}/text/analytics/v3.1/sentiment"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_ms(timestamp):
    """Convert mm:ss:ms to milliseconds"""
    try:
        minutes, seconds, ms = map(int, timestamp.split(":"))
        return (minutes * 60 + seconds) * 1000 + ms
    except Exception:
        logging.warning(f"Invalid timestamp format: {timestamp}")
        return 0


def compute_text_similarity(a, b):
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def compute_timestamp_overlap(start1, end1, start2, end2):
    s1 = _parse_ms(start1)
    e1 = _parse_ms(end1)
    s2 = _parse_ms(start2)
    e2 = _parse_ms(end2)

    latest_start = max(s1, s2)
    earliest_end = min(e1, e2)
    overlap = max(0, earliest_end - latest_start)
    duration = min(e1 - s1, e2 - s2)
    return overlap / duration if duration > 0 else 0.0


def call_azure_sentiment_api(text):
    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_TEXT_ANALYTICS_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "documents": [{"id": "1", "language": "en", "text": text}]
    }

    try:
        response = requests.post(SENTIMENT_URL, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()["documents"][0]
    except Exception as e:
        logging.error(f"Azure API Error: {e}")
        return None


def segment_phrases(word_data, time_gap_threshold_ms=800):
    phrases = []
    current_phrase = []
    previous_stop = None

    for word in word_data:
        current_start = _parse_ms(word["start_time"])
        current_stop = _parse_ms(word["stop_time"])
        gap = current_start - previous_stop if previous_stop is not None else 0
        current_phrase.append(word)

        if gap >= time_gap_threshold_ms or word["Word"].endswith((".", "!", "?")):
            if current_phrase:
                phrases.append(current_phrase)
                current_phrase = []
        previous_stop = current_stop

    if current_phrase:
        phrases.append(current_phrase)

    return phrases


def enrich_phrases_with_sentiment(phrases):
    enriched = []

    for phrase_words in phrases:
        text = " ".join(w["Word"] for w in phrase_words).strip()
        start_time = phrase_words[0]["start_time"]
        end_time = phrase_words[-1]["stop_time"]

        is_duplicate = False
        for prev in enriched:
            sim = compute_text_similarity(text, prev["text"])
            overlap = compute_timestamp_overlap(start_time, end_time, prev["start_time"], prev["end_time"])
            if sim >= 0.9 and overlap >= 0.8:
                is_duplicate = True
                break

        if is_duplicate:
            logging.info(f"Skipping duplicate phrase: '{text}'")
            continue

        sentiment_data = call_azure_sentiment_api(text)
        if sentiment_data:
            enriched.append({
                "text": text,
                "start_time": start_time,
                "end_time": end_time,
                "sentiment": sentiment_data["sentiment"],
                "confidenceScores": sentiment_data["confidenceScores"]
            })

    return enriched


def main():
    input_path = "00_Equalizer_90s_combined_transcription.json"
    output_path = "00_Equalizer_90s_phrase_sentiment.json"

    logging.info("Loading combined transcription...")
    with open(input_path, "r", encoding="utf-8") as f:
        word_data = json.load(f)

    logging.info("Segmenting phrases...")
    phrases = segment_phrases(word_data)

    logging.info("Scoring sentiment for each phrase...")
    enriched_phrases = enrich_phrases_with_sentiment(phrases)

    logging.info("Saving phrase-level sentiment output...")
    with open(output_path, "w", encoding="utf-8") as out_f:
        json.dump(enriched_phrases, out_f, indent=2)

    logging.info(f"✅ Done. Output saved to {output_path}")


if __name__ == "__main__":
    main()
