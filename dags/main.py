
from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlistId, get_video_id, extract_video_data, save_to_json


# Define the local timezone
local_tz = pendulum.timezone("Asia/Kolkata")


# Default Args:
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_run": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo= local_tz),
    # "end_date": datetime(2025, 1, 1, tzinfo= local_tz)
    }


with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description= "DAG to produce JSON file with raw data",
    schedule= "0 21 * * *",
    catchup= False
) as dag:
    
    # Define tasks
    playlist_id = get_playlistId()
    video_ids = get_video_id(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)


    # Define dependicies
    playlist_id >> video_ids >> extract_data >> save_to_json_task
