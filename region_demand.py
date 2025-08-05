import pandas as pd
import time
import json
import os
import datetime
import logging
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

#Slack Webhook
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

def send_slack_message(message):
    try:
        payload = {"text": message}
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            logging.warning(f"Slack Notification missed: {response.text}")
    except Exception as e:
        logging.error(f"Slack Notification Error: {e}")

#Logging Configuration
logging.basicConfig(
    filename='log.txt',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

#Constants and Configuration
DEVELOPER_KEY = 'Your API KEY' 
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

MAX_RESULTS = 50
MAX_PAGES = 1
CHUNK_COUNT = 5
LAST_FETCH_FILE = 'last_fetch.json'
Quota_Notify_Interval = 100000

#Main Processing Function
def main():
    logging.info("Demand Data Conllection Started")
    send_slack_message("Youtube Demand Data Collection has started")

    try:
        #Initialize YouTube API
        youtube = build(
            YOUTUBE_API_SERVICE_NAME,
            YOUTUBE_API_VERSION,
            developerKey=DEVELOPER_KEY
        )

        #Load Configuration File
        with open('regions code.txt', 'r', encoding='utf-8') as f:
            regions = [line.strip() for line in f if line.strip()]

        with open('concepts.txt', 'r', encoding='utf-8') as f:
            all_concepts = [line.strip() for line in f if line.strip()]

        with open('query_words.json', 'r', encoding='utf-8') as f:
            concept_queries = json.load(f)

        #Load Last Fetch timestamps
        if os.path.exists(LAST_FETCH_FILE):
            with open(LAST_FETCH_FILE, 'r', encoding='utf-8') as f:
                last_fetch = json.load(f)
        else:
            last_fetch = {}

        #Rotate Concepts Daily Based on Data
        today = datetime.date.today()
        day_index = today.toordinal() % CHUNK_COUNT
        chunk_size = len(all_concepts) // CHUNK_COUNT + 1
        concepts = all_concepts[day_index * chunk_size : (day_index + 1) * chunk_size]

        total_quota = 0
        concept_counter = 0

        for concept in concepts:
            concept_counter += 1
            query_words = concept_queries.get(concept, [concept])
            published_after = last_fetch.get(concept, '2024-04-01T00:00:00Z')

            logging.info(f"▶ Concept: '{concept}' since {published_after}")
            all_results = []

            for region in regions:
                logging.info(f"  → Region: {region}")
                total_view = total_like = total_comment = 0

                for query in query_words:
                    next_page_token = None
                    for page in range(MAX_PAGES):
                        try:
                            search_response = youtube.search().list(
                                q=query,
                                part='id',
                                type='video',
                                maxResults=MAX_RESULTS,
                                regionCode=region,
                                order='relevance',
                                publishedAfter=published_after,
                                pageToken=next_page_token
                            ).execute()
                            total_quota += 100 #Quota Consumption of "search.list"

                            video_ids = [
                                item['id']['videoId']
                                for item in search_response.get('items', [])
                                if item['id'].get('kind') == 'youtube#video' and 'videoId' in item['id']
                            ]

                            if not video_ids:
                                break

                            video_response = youtube.videos().list(
                                part='statistics',
                                id=','.join(video_ids)
                            ).execute()
                            total_quota += len(video_ids) #1point = 1video Consumption

                            for item in video_response.get('items', []):
                                stats = item.get('statistics', {})
                                total_view += int(stats.get('viewCount', 0))
                                total_like += int(stats.get('likeCount', 0))
                                total_comment += int(stats.get('commentCount', 0))

                            next_page_token = search_response.get('nextPageToken')
                            if not next_page_token:
                                break

                            time.sleep(1)

                        except HttpError as e:
                            reason = e.error_details[0].get('reason', '') if hasattr(e, 'error_details') else ''
                            if e.resp.status == 403 and "quotaExceeded" in str(e):
                                send_slack_message("Quota Exceeded! Collection Interrupted")
                                logging.error("Quota exceeded")
                                return
                            send_slack_message(f"API Error: query='{query}', region='{region}': {e}")
                            logging.error(f"    Error: query='{query}', region='{region}': {e}")
                            break

                all_results.append({
                    'concept': concept,
                    'region': region,
                    'viewCount': total_view,
                    'likeCount': total_like,
                    'commentCount': total_comment
                })

            #Save Results
            df = pd.DataFrame(all_results)
            timestamp = today.isoformat().replace("-", "")
            filename = f'youtube_demand_{concept}_{timestamp}.csv'
            df.to_csv(filename, index=False)
            logging.info(f"Saved: {filename}")
            send_slack_message(f"Completed: {concept} has processed({len(query_words)} queries * {len(regions)} regions)")

            #Interim Notice
            if total_quota // Quota_Notify_Interval > (total_quota - len(query_words) * len(regions) * 150) // Quota_Notify_Interval:
                send_slack_message(f"Quota Usage is about {total_quota:,} / 1000000")

            #Update Fetch Timestamp
            now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            last_fetch[concept] = now_iso

        #Save Updated Timestamps
        with open(LAST_FETCH_FILE, 'w', encoding='utf-8') as f:
            json.dump(last_fetch, f, ensure_ascii=False, indent=2)

        logging.info("All concepts processed successfully")
        send_slack_message(f"Collection Completed. {concept_counter} concepts has processed today. Total Usage is about {total_quota:,}")

    except Exception as e:
        logging.exception(f"Error occurred: {e}")
        send_slack_message(f"Fatal Error:{e}")


#Entry Point
if __name__ == "__main__":
    main()

