import ffmpeg
import os
import json
import math
import logging
import cv2

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Path configuration
FFMPEG_PATH = r"C:\ffmpeg\bin\ffmpeg.exe"

def get_video_fps(video_path):
    """
    Extract the frames per second (fps) using OpenCV instead of moviepy.
    """
    logging.info(f"Extracting FPS from video using OpenCV: {video_path}")
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise IOError(f"Cannot open video file {video_path}")
    fps = cap.get(cv2.CAP_PROP_FPS)
    cap.release()
    logging.debug(f"[OpenCV] Video FPS: {fps}")
    return fps

# ⬇️ [NO CHANGES TO THESE FUNCTIONS — reused from your current script] ⬇️

def group_words_by_time(transcription_data, frame_timestamps, fps, video_duration):
    group_data = {}
    ms_per_frame = 1000 / fps
    total_frames = int(video_duration * fps)
    for frame_id in range(0, total_frames, int(fps)):
        start_time = frame_id / fps
        group_id = math.floor(start_time * 1000 / 1000)
        if group_id not in group_data:
            group_data[group_id] = {
                "start_frame": frame_id,
                "end_frame": min(frame_id + int(fps), total_frames),
                "words": [],
            }
    for word_data in transcription_data:
        word = word_data["Word"]
        try:
            start_time = float(word_data["start_time"]) * 1000
        except ValueError:
            logging.error(f"Invalid start_time value: {word_data['start_time']}")
            continue
        group_id = math.floor(start_time / 1000)
        if group_id in group_data:
            group_data[group_id]["words"].append(word)
    return list(group_data.values())

def add_grouped_captions_to_video(input_video, grouped_data, output_video, fontsize=50):
    font_path = os.getenv("FFMPEG_FONT_PATH")
    if not font_path or not os.path.exists(font_path):
        raise FileNotFoundError("Font file not set or not found.")
    ffmpeg_filters = []
    fps = get_video_fps(input_video)
    for group in grouped_data:
        words = " ".join(group["words"])
        start_time = group["start_frame"] / fps
        end_time = group["end_frame"] / fps
        ffmpeg_filters.append({
            "text": words,
            "fontfile": font_path,
            "fontcolor": "white",
            "fontsize": fontsize,
            "x": "(w-text_w)/2",
            "y": "(h-text_h)/2",
            "enable": f"between(t,{start_time:.2f},{end_time:.2f})",
            "borderw": 2,
            "bordercolor": "black",
        })
    input_ffmpeg = ffmpeg.input(input_video)
    for filter_config in ffmpeg_filters:
        input_ffmpeg = input_ffmpeg.filter(
            "drawtext",
            text=filter_config["text"],
            fontfile=filter_config["fontfile"],
            fontcolor=filter_config["fontcolor"],
            fontsize=filter_config["fontsize"],
            x=filter_config["x"],
            y=filter_config["y"],
            enable=filter_config["enable"],
            borderw=filter_config["borderw"],
            bordercolor=filter_config["bordercolor"],
        )
    input_ffmpeg.output(output_video, vcodec="libx264", acodec="aac", map=0).run(overwrite_output=True, cmd=FFMPEG_PATH)

def generate_group_word_caption_settings(caption_settings_path, fps, output_path):
    with open(caption_settings_path, "r") as f:
        caption_settings = json.load(f)
    frames_per_group = int(fps)
    grouped_data = []
    frame_groups = {}
    delay_frame_offset = 1
    first_frame_id = min((frame for data in caption_settings for frame in data["frames"]), default=0)
    frame_shift = first_frame_id + delay_frame_offset
    for data in caption_settings:
        word = data["word"]
        for frame in data["frames"]:
            adjusted_frame = frame + frame_shift
            group_id = adjusted_frame // frames_per_group
            if group_id not in frame_groups:
                frame_groups[group_id] = {
                    "start_frame": group_id * frames_per_group,
                    "end_frame": (group_id + 1) * frames_per_group - 1,
                    "words": [],
                }
            if word not in frame_groups[group_id]["words"]:
                frame_groups[group_id]["words"].append(word)
    previous_last_word = None
    for group_id in sorted(frame_groups):
        group_data = frame_groups[group_id]
        if previous_last_word and group_data["words"] and group_data["words"][0] == previous_last_word:
            group_data["words"].pop(0)
        if group_data["words"]:
            previous_last_word = group_data["words"][-1]
        grouped_data.append(group_data)
    output_file = os.path.join(output_path, "group_word_caption_settings.json")
    with open(output_file, "w") as f:
        json.dump(grouped_data, f, indent=4)
    return grouped_data

def main():
    video_path = r"C:\Github\AudioToTextTiming\36-00_Equalizer_60s.mp4"
    caption_settings_path = r"C:\Github\AudioToTextTiming\36-00_Equalizer_60s.json"
    output_path = r"C:\Github\AudioToTextTiming"
    output_video = os.path.join(output_path, "Grouped_Captioned_36-00_Equalizer_60s.mp4")
    try:
        logging.info("Step 1: Get video FPS...")
        fps = get_video_fps(video_path)
        logging.info("Step 2: Generate grouped word caption settings...")
        grouped_data = generate_group_word_caption_settings(caption_settings_path, fps, output_path)
        logging.info("Step 3: Add grouped captions to video...")
        add_grouped_captions_to_video(video_path, grouped_data, output_video)
        logging.info(f"Process completed successfully. Video saved at {output_video}")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
