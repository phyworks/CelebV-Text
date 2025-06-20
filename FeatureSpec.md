# Feature Improvements

1. Load the data from json and group the video processing data by ytb_id first. So that the following download and video processing can be performed by ybt_id
2. Download the raw video file and process the video based on video process data 
3. Move the processed video using rclone to dropbox `rclone move [file name]  dropbox:celebv-text-raw/[file name]` 
4. After finished, append the youtube video id to a progress file, and remove the raw video and processed video file 
5. Support multi-threading


# Script Configuration

The `download_and_process.py` script now supports multi-threading to process multiple YouTube videos concurrently, significantly improving performance.

## Cookie or PO Token

Reference: 

- https://github.com/yt-dlp/yt-dlp/wiki/Extractors
- https://github.com/yt-dlp/yt-dlp/wiki/PO-Token-Guide


## Configuration Options

You can configure the script using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CELEBV_MAX_WORKERS` | 4 | Number of concurrent YouTube videos to process |
| `CELEBV_JSON_PATH` | celebvtext_info.json | Path to the input JSON file |
| `CELEBV_RAW_ROOT` | ./downloaded_celebvtext/raw/ | Directory for raw video downloads |
| `CELEBV_PROCESSED_ROOT` | ./downloaded_celebvtext/processed/ | Directory for processed videos |
| `CELEBV_PROGRESS_FILE` | progress.txt | File to track processing progress |
| `CELEBV_PROXY` | None | Proxy URL if needed |
| `CELEBV_DROPBOX_PATH` | dropbox:celebv-text-raw/ | Dropbox destination path |

## Usage Examples

### Basic Usage
```bash
# Run with default 4 workers
python3 download_and_process.py
```

### Custom Worker Count
```bash
# Run with 2 workers (conservative)
CELEBV_MAX_WORKERS=2 python3 download_and_process.py

# Run with 8 workers (aggressive)
CELEBV_MAX_WORKERS=8 python3 download_and_process.py
```

### Using Sample Data
```bash
# Process sample data with 2 workers
CELEBV_MAX_WORKERS=2 CELEBV_JSON_PATH=celebvtext_info-sample.json python3 download_and_process.py
```

### Full Configuration
```bash
# Complete custom configuration
CELEBV_MAX_WORKERS=6 \
CELEBV_JSON_PATH=my_data.json \
CELEBV_RAW_ROOT=./my_raw/ \
CELEBV_PROCESSED_ROOT=./my_processed/ \
CELEBV_PROGRESS_FILE=my_progress.txt \
CELEBV_DROPBOX_PATH=dropbox:my-custom-path/ \
python3 download_and_process.py
```

## Performance Considerations

- **CPU-bound processing**: More workers help with video processing (ffmpeg operations)
- **I/O-bound downloads**: More workers help with YouTube downloads and rclone uploads
- **Memory usage**: Each worker processes one YouTube video at a time, which may include multiple output videos
- **Network bandwidth**: More workers will increase network usage for downloads and uploads
- **Recommended starting point**: 4 workers for most systems, adjust based on your hardware

## Progress Tracking

The script now includes:
- Thread-safe progress tracking
- Real-time progress updates with percentages
- Per-thread logging with thread identifiers
- Comprehensive statistics at completion
- Automatic resume capability (skips already completed videos)

## Logging

Each thread logs with a unique identifier (e.g., `[YTWorker-1]`) making it easy to track individual video processing in the logs.

## Error Handling

- Individual video failures don't stop other threads
- Progress is saved immediately upon successful completion
- Failed videos can be retried by running the script again
- Comprehensive error logging with thread information

# Server Setup

Preprae

## Install tools and configuration

__Install apt packages__

```
apt install -y rclone ffmpeg iftop python3-pip git unzip screen
```

__Install yt-dlp__

```
curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp
chmod a+rx  /usr/local/bin/yt-dlp
```

__Configuration__
```
vi ~/.config/rclone/rclone.conf
vi ~/.screenrc
```

## create a virtual environment and install the dependency

```
python3 -m venv path/to/venv
./bin/pip3 install -r requirements.txt
```