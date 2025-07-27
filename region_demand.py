from googleapiclient.discovery import build
import pandas as pd
import time

DEVELOPER_KEY = 'Your API KEY'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

youtube = build(
    YOUTUBE_API_SERVICE_NAME,
    YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY
)

with open('regions code.txt', 'r', encoding='utf-8') as f:
    regions = [line.strip() for line in f if line.strip()]

concept = "soccer"  #representive word
query_words = ["soccer", "サッカー", "fútbol", "Fußball", "calcio", "futebol", "футбол", "كرة_القدم", "足球", "축구"]  #multiple languages

all_results = []

for region in regions:
    print(f"Processing concept: '{concept}' in region: {region}")
    total_view = total_like = total_comment = 0

    for query in query_words:
        try:

            search_response = youtube.search().list(
                q=query,
                part='id',
                type='video',
                maxResults=50,
                regionCode=region,
                order='relevance'
            ).execute()

            video_ids = [
                item['id']['videoId']
                for item in search_response.get('items', [])
                if item['id'].get('kind') == 'youtube#video' and 'videoId' in item['id']
            ]

            if not video_ids:
                continue

            video_response = youtube.videos().list(
                part='statistics',
                id=','.join(video_ids)
            ).execute()

            for item in video_response.get('items', []):
                stats = item.get('statistics', {})
                total_view += int(stats.get('viewCount', 0))
                total_like += int(stats.get('likeCount', 0))
                total_comment += int(stats.get('commentCount', 0))

            time.sleep(1)

        except Exception as e:
            print(f"Error with query='{query}', region='{region}': {e}")

    all_results.append({
        'concept': concept,
        'region': region,
        'viewCount': total_view,
        'likeCount': total_like,
        'commentCount': total_comment
    })

df = pd.DataFrame(all_results)
df.to_csv(f'youtube_demand_{concept}.csv', index=False)
print(f"Data collection completed. Output: youtube_demand_{concept}.csv")
