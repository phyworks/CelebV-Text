"""
Improved Downloader with ytb_id grouping, rclone integration, and progress tracking
"""

import os
import json
import cv2
from collections import defaultdict
import subprocess
import logging


def download(video_path, ytb_id, proxy=None):
    """
    Download YouTube video
    
    Args:
        video_path: path to save the video
        ytb_id: youtube video id
        proxy: proxy url, default None
    
    Returns:
        bool: True if successful, False otherwise
    """
    if os.path.exists(video_path):
        logging.info(f"Video already exists: {video_path}")
        return True
        
    if proxy is not None:
        proxy_cmd = f"--proxy {proxy}"
    else:
        proxy_cmd = ""
    
    down_video = " ".join([
        "yt-dlp",
        proxy_cmd,
        '-f', "'bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio'",
        '--skip-unavailable-fragments',
        '--merge-output-format', 'mp4',
        f"https://www.youtube.com/watch?v={ytb_id}",
        "--output", f"'{video_path}'",
        "--external-downloader", "aria2c",
        "--external-downloader-args", '"-x 16 -k 1M"'
    ])
    
    success, output = run_command(down_video, f"Downloading video {ytb_id}")
    
    if not success:
        logging.error(f"Failed to download video: {ytb_id}")
        return False
        
    if not os.path.exists(video_path):
        logging.error(f"Download completed but file not found: {video_path}")
        return False
    
    logging.info(f"Successfully downloaded: {ytb_id}")
    return True


def process_ffmpeg(raw_vid_path, save_folder, save_vid_name, bbox, time):
    """
    Process raw video with ffmpeg to crop and trim
    
    Args:
        raw_vid_path: path to raw video
        save_folder: folder to save processed video
        save_vid_name: name of processed video
        bbox: bounding box [top, bottom, left, right] normalized to 0~1
        time: (begin_sec, end_sec)
    
    Returns:
        str: path to processed video file, or None if failed
    """
    def secs_to_timestr(secs):
        hrs = secs // (60 * 60)
        min = (secs - hrs * 3600) // 60
        sec = secs % 60
        end = (secs - int(secs)) * 100
        return "{:02d}:{:02d}:{:02d}.{:02d}".format(int(hrs), int(min), int(sec), int(end))

    def expand(bbox, ratio):
        top, bottom = max(bbox[0] - ratio, 0), min(bbox[1] + ratio, 1)
        left, right = max(bbox[2] - ratio, 0), min(bbox[3] + ratio, 1)
        return top, bottom, left, right

    def to_square(bbox):
        top, bottom, leftx, right = bbox
        h = bottom - top
        w = right - leftx
        c = min(h, w) / 2
        c_h = (top + bottom) / 2
        c_w = (leftx + right) / 2

        top, bottom = c_h - c, c_h + c
        leftx, right = c_w - c, c_w + c
        return top, bottom, leftx, right

    def denorm(bbox, height, width):
        top = round(bbox[0] * height)
        bottom = round(bbox[1] * height)
        left = round(bbox[2] * width)
        right = round(bbox[3] * width)
        return top, bottom, left, right

    out_path = os.path.join(save_folder, save_vid_name)
    
    try:
        cap = cv2.VideoCapture(raw_vid_path)
        if not cap.isOpened():
            logging.error(f"Cannot open video: {raw_vid_path}")
            return None
            
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()
        
        top, bottom, left, right = to_square(denorm(expand(bbox, 0.02), height, width))
        start_sec, end_sec = time

        cmd = f"ffmpeg -i '{raw_vid_path}' -vf crop=w={right - left}:h={bottom - top}:x={left}:y={top} -ss {secs_to_timestr(start_sec)} -to {secs_to_timestr(end_sec)} -loglevel error -y '{out_path}'"
        
        success, output = run_command(cmd, f"Processing video {save_vid_name}")
        
        if success and os.path.exists(out_path):
            logging.info(f"Successfully processed: {save_vid_name}")
            return out_path
        else:
            logging.error(f"Failed to process video: {save_vid_name}")
            return None
            
    except Exception as e:
        logging.error(f"Error processing {save_vid_name}: {e}")
        return None


def load_and_group_data(file_path):
    """
    Load data from JSON and group by ytb_id for efficient processing
    
    Returns:
        dict: {ytb_id: [list of video processing data]}
    """
    with open(file_path) as f:
        data_dict = json.load(f)
    
    grouped_data = defaultdict(list)
    
    for key, val in data_dict.items():
        save_name = key  # Keep original filename
        ytb_id = val['ytb_id']
        time = (val['duration']['start_sec'], val['duration']['end_sec'])
        bbox = [val['bbox']['top'], val['bbox']['bottom'], val['bbox']['left'], val['bbox']['right']]
        
        grouped_data[ytb_id].append({
            'save_name': save_name,
            'time': time,
            'bbox': bbox
        })
    
    return grouped_data


def load_progress(progress_file):
    """Load completed ytb_ids from progress file"""
    if not os.path.exists(progress_file):
        return set()
    
    with open(progress_file, 'r') as f:
        return set(line.strip() for line in f if line.strip())


def save_progress(progress_file, ytb_id):
    """Append completed ytb_id to progress file"""
    with open(progress_file, 'a') as f:
        f.write(f"{ytb_id}\n")


def run_command(cmd, description=""):
    """Run shell command with error handling"""
    logging.info(f"Running: {description}")
    logging.debug(f"Command: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {description}")
        logging.error(f"Error: {e.stderr}")
        return False, e.stderr


def move_to_dropbox(file_path, dropbox_path="dropbox:celebv-text-raw/"):
    """Move processed video to Dropbox using rclone"""
    filename = os.path.basename(file_path)
    dropbox_full_path = f"{dropbox_path}{filename}"
    
    cmd = f"rclone move '{file_path}' '{dropbox_full_path}'"
    success, output = run_command(cmd, f"Moving {filename} to Dropbox")
    
    if success:
        logging.info(f"Successfully moved {filename} to Dropbox")
        return True
    else:
        logging.error(f"Failed to move {filename} to Dropbox: {output}")
        return False


def cleanup_files(*file_paths):
    """Remove local files after successful upload"""
    for file_path in file_paths:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Cleaned up: {file_path}")
            except Exception as e:
                logging.error(f"Failed to cleanup {file_path}: {e}")


def process_ytb_id(ytb_id, video_data_list, raw_vid_root, processed_vid_root, proxy=None):
    """
    Process all videos for a single YouTube ID
    
    Args:
        ytb_id: YouTube video ID
        video_data_list: List of video processing data for this ytb_id
        raw_vid_root: Directory for raw videos
        processed_vid_root: Directory for processed videos
        proxy: Proxy URL if needed
    
    Returns:
        bool: True if all videos processed successfully
    """
    raw_vid_path = os.path.join(raw_vid_root, f"{ytb_id}.mp4")
    
    # Download raw video
    if not download(raw_vid_path, ytb_id, proxy):
        logging.error(f"Failed to download {ytb_id}, skipping all related videos")
        return False
    
    processed_files = []
    all_success = True
    
    # Process all videos for this ytb_id
    for video_data in video_data_list:
        save_name = video_data['save_name']
        time = video_data['time']
        bbox = video_data['bbox']
        
        logging.info(f"Processing {save_name} from {ytb_id}")
        
        processed_path = process_ffmpeg(raw_vid_path, processed_vid_root, save_name, bbox, time)
        
        if processed_path:
            processed_files.append(processed_path)
        else:
            logging.error(f"Failed to process {save_name}")
            all_success = False
    
    # Move processed files to Dropbox
    moved_files = []
    for processed_path in processed_files:
        if move_to_dropbox(processed_path):
            moved_files.append(processed_path)
        else:
            all_success = False
    
    # Cleanup files (both raw and successfully moved processed files)
    cleanup_files(raw_vid_path, *moved_files)
    
    return all_success


if __name__ == '__main__':
    # Configuration
    json_path = 'celebvtext_info.json'  # JSON file path
    raw_vid_root = './downloaded_celebvtext/raw/'  # Download raw video path
    processed_vid_root = './downloaded_celebvtext/processed/'  # Processed video path
    progress_file = 'progress.txt'  # Progress tracking file
    proxy = None  # Proxy URL example, set to None if not used
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('download_process.log'),
            logging.StreamHandler()
        ]
    )
    
    # Create directories
    os.makedirs(raw_vid_root, exist_ok=True)
    os.makedirs(processed_vid_root, exist_ok=True)
    
    try:
        # Load and group data by ytb_id
        logging.info("Loading and grouping data by ytb_id...")
        grouped_data = load_and_group_data(json_path)
        logging.info(f"Loaded data for {len(grouped_data)} YouTube videos")
        
        # Load progress to skip already completed videos
        completed_ytb_ids = load_progress(progress_file)
        logging.info(f"Found {len(completed_ytb_ids)} already completed videos")
        
        # Process each ytb_id
        total_ytb_ids = len(grouped_data)
        processed_count = 0
        
        for ytb_id, video_data_list in grouped_data.items():
            if ytb_id in completed_ytb_ids:
                logging.info(f"Skipping already completed: {ytb_id}")
                continue
            
            processed_count += 1
            logging.info(f"Processing ytb_id {processed_count}/{total_ytb_ids}: {ytb_id} ({len(video_data_list)} videos)")
            
            try:
                success = process_ytb_id(ytb_id, video_data_list, raw_vid_root, processed_vid_root, proxy)
                
                if success:
                    # Save progress
                    save_progress(progress_file, ytb_id)
                    logging.info(f"Successfully completed: {ytb_id}")
                else:
                    logging.error(f"Some errors occurred while processing: {ytb_id}")
                    
            except Exception as e:
                logging.error(f"Unexpected error processing {ytb_id}: {e}")
                continue
        
        logging.info("Processing completed!")
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise
