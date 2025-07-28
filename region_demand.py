from googleapiclient.discovery import build
import pandas as pd
import time

DEVELOPER_KEY = 'Your API Key'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

youtube = build(
    YOUTUBE_API_SERVICE_NAME,
    YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY
)

with open('regions code.txt', 'r', encoding='utf-8') as f:
    regions = [line.strip() for line in f if line.strip()]

concept = "soccer" #representive word
query_words = ["soccer", "サッカー", "fútbol", "Fußball", "calcio", "futebol", "футбол", "كرة_القدم", "足球", "축구"] #multiple languages

#検索設定
max_results = 50 #1ページあたりの動画数
max_pages = 1 #検索ページ数（1以上、増やすとより多くの動画を収集）
results_per_query = max_results * max_pages

all_results = []

for region in regions:
    print(f"Processing concept: '{concept}' in region: {region}")
    total_view = total_like = total_comment = 0

    for query in query_words:
        next_page_token = None
        for page in range(max_pages):
            try:
                search_response = youtube.search().list(
                    q=query,
                    part='id',
                    type='video',
                    maxResults=max_results,
                    regionCode=region,
                    order='viewCount', #relevant→viewCountにした
                    publishedAfter ='2024-04-01T00:00:00Z', #直近3か月に限定にしてみた
                    pageToken=next_page_token
                ).execute()

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

                for item in video_response.get('items', []):
                    stats = item.get('statistics', {})
                    total_view += int(stats.get('viewCount', 0))
                    total_like += int(stats.get('likeCount', 0))
                    total_comment += int(stats.get('commentCount', 0))

                next_page_token = search_response.get('nextPageToken')
                if not next_page_token:
                    break

                time.sleep(1)

            except Exception as e:
                print(f"Error: query='{query}', region='{region}', page={page+1}: {e}")
                break

    all_results.append({
        'concept': concept,
        'region': region,
        'viewCount': total_view,
        'likeCount': total_like,
        'commentCount': total_comment
    })

df = pd.DataFrame(all_results)
df.to_csv(f'youtube_demand_{concept}_revised.csv', index=False)
print(f"Data collection completed. Output: youtube_demand_{concept}.csv")
