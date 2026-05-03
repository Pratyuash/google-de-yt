import requests
import json
from datetime import date
from airflow.decorators import task
from airflow.models import Variable

# import os 
# from dotenv import load_dotenv
# load_dotenv(dotenv_path= "./.env")

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResult = 50


@task
def get_playlistId():

    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)                        # Sends an HTTP GET request to the given url
        response.raise_for_status()                         # Checks if the request failed. If there’s an error, it raises an exception


        data = response.json()                              # Converts the response into usable Python data
        #print(json.dumps(data, indent=4))                  #json.dumps is used to get python object in json format

        channel_items = data['items'][0]
        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']
        #print(channel_playlistId)
        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e



@task
def get_video_id(playlistId):

    video_ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResult}&playlistId={playlistId}&key={API_KEY}"

    try:
        while True:
            url = base_url

            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):                      # here [] denotes empty list to ensure the code doesn't break when no key is found
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken')

            if not pageToken:
                break
        
        return video_ids
    
    except requests.exceptions.RequestException as e:
        raise e
    

    

@task
def extract_video_data(video_ids):

    extracted_data = []

    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id: video_id + batch_size] 

    

    try:
        for batch in batch_list(video_ids, maxResult):
            video_ids_str = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get('viewCount', None),
                    "likeCount": statistics.get('likeCount', None),
                    "commentCount": statistics.get('commentCount', None)
                }

                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e



@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)



if __name__ == "__main__":
    playlistId = get_playlistId()
    video_ids = get_video_id(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)