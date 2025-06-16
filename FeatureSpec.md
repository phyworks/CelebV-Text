Improve this`download_and_process.py` file's logic:

1. Load the data from json and group the video processing data by ytb_id first. So that the following download and video processing can be performed by ybt_id
2. Download the raw video file and process the video based on video process data 
3. Move the processed video using rclone to dropbox `rclone move [file name]  dropbox:celebv-text-raw/[file name]` 
4. After finished, append the youtube video id to a progress file, and remove the raw video and processed video file 
5. Support downloading in multi-thread, so that it can download 5 files (configurable) at the same time
