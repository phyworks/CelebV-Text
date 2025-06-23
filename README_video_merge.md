# CelebV-Text Video Merge Script Guide

## Overview

The `merge_video.py` script automates the process of merging video and audio files from the CelebV-Text dataset. It downloads video tar files from Google Drive, extracts them, merges them with corresponding audio files, and uploads the merged results to Dropbox.

## What It Does

1. **Downloads** video tar files (`sp_0002.tar` to `sp_0069.tar`) from Google Drive
2. **Extracts** video files from tar archives
3. **Merges** video files with corresponding audio files using FFmpeg
4. **Uploads** merged files to Dropbox
5. **Tracks progress** to resume interrupted operations
6. **Logs** all operations for monitoring and debugging

## Prerequisites

### Required Tools
- Python 3.x
- FFmpeg (for video/audio merging)
- rclone (for Google Drive and Dropbox operations)

### Directory Structure
```
celebvtext_audio/          # Contains .m4a audio files
celebvtext_video_raw/      # Temporary storage for downloaded tar files
celebvtext_video/          # Temporary extraction directory
celebvtext_merged/         # Temporary storage for merged files
logs/                      # Log files
progress.txt              # Progress tracking file
```

### rclone Configuration
You need rclone configured with:
- `gdrive:` remote for Google Drive access
- `dropbox:` remote for Dropbox access

## Usage

### Basic Execution
```bash
python merge_video.py
```

### What Happens During Execution

1. **Initialization**: Creates necessary directories and sets up logging
2. **Progress Loading**: Checks `progress.txt` for previously completed files
3. **File Processing**: For each tar file:
   - Downloads from Google Drive
   - Extracts video files
   - Merges with audio files using FFmpeg
   - Uploads merged files to Dropbox
   - Cleans up temporary files
   - Updates progress

### Resuming Interrupted Operations

The script automatically resumes from where it left off using the `progress.txt` file. No manual intervention needed.

## Output

### Log Files
- `logs/merge_video.log`: General operation logs
- `logs/merge_video_error.log`: Error-specific logs
- Console output with real-time progress

### File Locations
- **Source Audio**: `celebvtext_audio/*.m4a`
- **Downloaded Videos**: Temporary (deleted after processing)
- **Merged Output**: Uploaded to `dropbox:celebv_merged/`

## Configuration

### File Range
Currently processes files `sp_0002.tar` to `sp_0069.tar`. To modify:
```python
tar_files = [f"sp_{i:04d}.tar" for i in range(START, END)]
```

### FFmpeg Parameters
Video and audio are copied without re-encoding for speed:
- Video codec: copy (no re-encoding)
- Audio codec: copy (no re-encoding)
- Uses shortest stream duration

## Monitoring

### Real-time Progress
- Console shows current file being processed
- Progress counter (e.g., "Processing file 25/68")
- rclone progress bars for transfers

### Error Handling
- Continues processing if individual files fail
- Logs all errors for later review
- Skips files with missing audio

## Troubleshooting

### Common Issues
1. **Missing audio files**: Script logs warnings and skips videos
2. **rclone errors**: Check Google Drive/Dropbox connectivity
3. **FFmpeg errors**: Verify FFmpeg installation and file formats
4. **Disk space**: Monitor available space for temporary files

### Recovery
- Script automatically resumes from `progress.txt`
- Manual recovery: edit `progress.txt` to mark files as complete/incomplete
- Check error logs for specific failure reasons

## Performance Notes

- Processing time depends on file sizes and network speed
- Temporary files are cleaned up after each tar file
- Only one tar file is processed at a time to manage disk space
- Network transfers show progress bars via rclone