import os
import tarfile
import subprocess
import shutil
import logging
import sys
from datetime import datetime

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Setup formatters
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Setup normal log
    normal_handler = logging.FileHandler('logs/merge_video.log')
    normal_handler.setLevel(logging.INFO)
    normal_handler.setFormatter(formatter)
    
    # Setup error log
    error_handler = logging.FileHandler('logs/merge_video_error.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # Setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Configure logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(normal_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    return logger

def load_progress():
    """Load progress from file"""
    progress_file = 'progress.txt'
    completed = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            completed = set(line.strip() for line in f if line.strip())
    return completed

def save_progress(completed_files):
    """Save progress to file"""
    with open('progress.txt', 'w') as f:
        for file in sorted(completed_files):
            f.write(f"{file}\n")

def copy_from_gdrive(tar_filename, raw_dir, logger):
    """Copy tar file from Google Drive"""
    try:
        gdrive_path = f"gdrive:CelebV-Text/video/{tar_filename}"
        subprocess.run([
            'rclone', 'copy', '--drive-shared-with-me', gdrive_path, raw_dir, '-P'
        ], check=True)
        logger.info(f"Successfully copied {tar_filename} from Google Drive")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error copying {tar_filename} from Google Drive: {e}")
        return False

def process_tar_file(tar_filename, logger):
    """Process a single tar file"""
    # Define directories
    raw_dir = 'celebvtext_video_raw'
    video_dir = 'celebvtext_video'
    audio_dir = 'celebvtext_audio'
    merged_dir = 'celebvtext_merged'
    
    tar_path = os.path.join(raw_dir, tar_filename)
    
    try:
        # Clean up video directory
        if os.path.exists(video_dir):
            shutil.rmtree(video_dir)
        os.makedirs(video_dir, exist_ok=True)

        # Clean up merged directory
        if os.path.exists(merged_dir):
            shutil.rmtree(merged_dir)
        os.makedirs(merged_dir, exist_ok=True)

        # Extract tar file
        with tarfile.open(tar_path, 'r') as tar:
            tar.extractall(path=video_dir)
            logger.info(f"Extracted files from {tar_filename} to {video_dir}")

        # Process each video file
        merged_count = 0
        for root, dirs, files in os.walk(video_dir):
            for file in files:
                if file.endswith('.mp4'):
                    video_path = os.path.join(root, file)
                    base_name = os.path.splitext(file)[0]
                    audio_file = f"{base_name}.m4a"
                    audio_path = os.path.join(audio_dir, audio_file)
                    
                    if not os.path.exists(audio_path):
                        logger.warning(f"Audio file {audio_file} not found, skipping {file}")
                        continue
                    
                    output_file = os.path.join(merged_dir, file)
                    
                    # Merge using ffmpeg
                    try:
                        subprocess.run([
                            'ffmpeg',
                            '-i', video_path,
                            '-i', audio_path,
                            '-c:v', 'copy',
                            '-c:a', 'copy',
                            '-map', '0:v:0',
                            '-map', '1:a:0',
                            '-shortest',
                            '-y',  # Overwrite output file
                            output_file
                        ], check=True, capture_output=True)
                        logger.info(f"Merged {file} successfully")
                        merged_count += 1
                    except subprocess.CalledProcessError as e:
                        logger.error(f"Error merging {file}: {e}")

        logger.info(f"Merged {merged_count} files from {tar_filename}")

        # Move merged files to Dropbox
        if merged_count > 0:
            subprocess.run([
                'rclone',
                'move',
                merged_dir + '/',
                'dropbox:celebv_merged/', '-P'
            ], check=True)
            logger.info(f"All merged files from {tar_filename} moved to Dropbox")
        
        # Remove the raw tar file
        os.remove(tar_path)
        logger.info(f"Removed raw tar file: {tar_filename}")
        
        return True

    except Exception as e:
        logger.error(f"Error processing {tar_filename}: {str(e)}")
        return False

def main():
    logger = setup_logging()
    logger.info("Starting merge video process")
    
    # Create raw directory
    raw_dir = 'celebvtext_video_raw'
    os.makedirs(raw_dir, exist_ok=True)
    
    # Load progress
    completed_files = load_progress()
    logger.info(f"Loaded progress: {len(completed_files)} files already completed")
    
    # Generate list of tar files to process
    tar_files = [f"sp_{i:04d}.tar" for i in range(2, 70)]
    
    # Filter out already completed files
    remaining_files = [f for f in tar_files if f not in completed_files]
    logger.info(f"Processing {len(remaining_files)} remaining files")
    
    for i, tar_filename in enumerate(remaining_files, 1):
        logger.info(f"Processing file {i}/{len(remaining_files)}: {tar_filename}")
        
        # Copy from Google Drive
        if not copy_from_gdrive(tar_filename, raw_dir, logger):
            logger.error(f"Failed to copy {tar_filename}, skipping to next file")
            continue
        
        # Process the tar file
        if process_tar_file(tar_filename, logger):
            completed_files.add(tar_filename)
            save_progress(completed_files)
            logger.info(f"Successfully completed processing {tar_filename}")
        else:
            logger.error(f"Failed to process {tar_filename}")
    
    logger.info("Merge video process completed")

if __name__ == '__main__':
    main()