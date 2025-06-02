import ffmpeg
import os
import json
import math
import logging
from moviepy.editor import VideoFileClip

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Path configuration
FFMPEG_PATH = r"C:\ffmpeg\bin\ffmpeg.exe"


def get_video_fps(video_path):
    """
    Extract the frames per second (fps) from a video file.
    """
    logging.info(f"Extracting FPS from video: {video_path}")
    clip = VideoFileClip(video_path)
    fps = clip.fps
    clip.close()
    logging.debug(f"Video FPS: {fps}")
    return fps


def group_words_by_time(transcription_data, frame_timestamps, fps, video_duration):
    """
    Group words into 1000-millisecond windows and map to frame ranges.
    """
    logging.info("Grouping words into 1000-millisecond windows...")
    group_data = {}
    ms_per_frame = 1000 / fps  # Milliseconds per frame

    # Divide video duration into 1000ms groups
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

    # Add words to the appropriate group based on timestamps
    for word_data in transcription_data:
        word = word_data["Word"]
        try:
            start_time = float(word_data["start_time"]) * 1000  # Convert to milliseconds
        except ValueError:
            logging.error(f"Invalid start_time value: {word_data['start_time']}")
            continue
        group_id = math.floor(start_time / 1000)

        if group_id in group_data:
            group_data[group_id]["words"].append(word)

    logging.info("Words successfully grouped.")
    return list(group_data.values())




def add_grouped_captions_to_video(input_video, grouped_data, output_video, fontsize=50):
    """
    Add grouped captions to video based on group_word_caption_settings.json.
    Ensures the words appear in the correct sequence as in the caption_settings file.
    """
    logging.info(f"Adding grouped captions to video: {input_video}")

    # Get font path from environment variable
    font_path = os.getenv("FFMPEG_FONT_PATH")
    if not font_path:
        raise EnvironmentError("Environment variable 'FFMPEG_FONT_PATH' is not set.")
    if not os.path.exists(font_path):
        raise FileNotFoundError(f"Font file specified in 'FFMPEG_FONT_PATH' not found: {font_path}")

    logging.debug(f"Using font file: {font_path}")

    ffmpeg_filters = []
    fps = get_video_fps(input_video)

    for group in grouped_data:
        # Maintain the sequence of words as in the original group data
        words = " ".join(group["words"])  # Combine words into a single line, preserving order
        start_frame = group["start_frame"]
        end_frame = group["end_frame"]
        start_time = start_frame / fps
        end_time = end_frame / fps

        logging.debug(f"Group caption '{words}' will appear between {start_time:.2f} and {end_time:.2f} seconds.")

        # Create FFmpeg drawtext filter
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

    logging.info("Applying FFmpeg filters...")
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

    logging.info(f"Outputting video to {output_video}...")
    (
        input_ffmpeg
        .output(output_video, vcodec="libx264", acodec="aac", map=0)
        .run(overwrite_output=True, cmd=FFMPEG_PATH)
    )
    logging.info("Video with captions saved successfully.")




def generate_group_word_caption_settings(caption_settings_path, fps, output_path):
    """
    Generate grouped captions from the caption_settings file based on 1000 ms (30-frame) intervals.
    Ensures words are added only once per group, introduces a delay placement in frames,
    and removes duplicated words that span consecutive groups.
    """
    logging.info(f"Generating grouped word captions from {caption_settings_path}...")

    # Load the caption_settings file
    with open(caption_settings_path, "r") as f:
        caption_settings = json.load(f)

    # Group frames into 1000 ms intervals (30 frames per group for 30 FPS)
    frames_per_group = int(fps)  # 30 frames for 30 FPS
    grouped_data = []
    frame_groups = {}

    # Introduce a delay: find the first frame and shift all groupings by 1 frame
    delay_frame_offset = 1
    first_frame_id = None

    for data in caption_settings:
        for frame in data["frames"]:
            if first_frame_id is None or frame < first_frame_id:
                first_frame_id = frame

    logging.debug(f"First frame ID: {first_frame_id}")
    frame_shift = first_frame_id + delay_frame_offset

    for data in caption_settings:
        word = data["word"]
        for frame in data["frames"]:
            adjusted_frame = frame + frame_shift  # Apply the delay shift
            group_id = adjusted_frame // frames_per_group
            if group_id not in frame_groups:
                frame_groups[group_id] = {
                    "start_frame": group_id * frames_per_group,
                    "end_frame": (group_id + 1) * frames_per_group - 1,
                    "words": [],
                }
            # Add the word to the group only if it hasn't been added already
            if word not in frame_groups[group_id]["words"]:
                frame_groups[group_id]["words"].append(word)

    # Convert the frame_groups dictionary to a list and resolve duplicates across groups
    previous_last_word = None
    for group_id, group_data in frame_groups.items():
        # Remove the first word of the group if it matches the last word of the previous group
        if previous_last_word and group_data["words"] and group_data["words"][0] == previous_last_word:
            logging.debug(f"Removing duplicated word '{group_data['words'][0]}' from group {group_id}")
            group_data["words"].pop(0)
        if group_data["words"]:
            previous_last_word = group_data["words"][-1]
        grouped_data.append(group_data)

    # Save the grouped word captions
    output_file = os.path.join(output_path, "group_word_caption_settings.json")
    with open(output_file, "w") as f:
        json.dump(grouped_data, f, indent=4)

    logging.info(f"Grouped word captions saved to {output_file}.")
    return grouped_data


def main():
    #video_path = r"c:\dev\Pod_Cast_Matthew_30s.mp4"
    #caption_settings_path = r"c:\dev\audioTranscriptions\caption_settings.json"
    #output_path = r"c:\dev\audioTranscriptions"
    #output_video = os.path.join(output_path, "Grouped_Captioned_Pod_Cast_Matthew_30s.mp4")
    video_path = r"C:\Github\AudioToTextTiming\36-00_Equalizer_60s.mp4"
    caption_settings_path = r"C:\Github\AudioToTextTiming\36-00_Equalizer_60s.json"
    output_path = r"C:\Github\AudioToTextTiming"
    output_video = os.path.join(output_path, "Grouped_Captioned_36-00_Equalizer_60s.mp4")

    try:
        # Step 1: Generate frame timestamps
        logging.info("Step 1: Generate frame timestamps...")
        fps = get_video_fps(video_path)

        # Step 2: Generate grouped word caption settings
        logging.info("Step 2: Generate grouped word caption settings...")
        grouped_data = generate_group_word_caption_settings(caption_settings_path, fps, output_path)

        # Step 3: Add grouped captions to video
        logging.info("Step 3: Add grouped captions to video...")
        add_grouped_captions_to_video(video_path, grouped_data, output_video)

        logging.info(f"Process completed successfully. Video saved at {output_video}")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main()