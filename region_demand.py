from googleapiclient.discovery import build
import pandas as pd
import numpy as np
import time

# API configuration
DEVELOPER_KEY = 'YOUR_API_KEY'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

youtube = build(
    YOUTUBE_API_SERVICE_NAME,
    YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY
)

# Target regions and queries
regions = ['US', 'JP']
queries = ['サッカー', 'kpop']

# Store all results
all_results = []

# Data collection loop
for query in queries:
    for region in regions:
        print(f"Processing: query={query}, region={region}")
        try:
            # Search for videos matching the query in the specified region
            search_response = youtube.search().list(
                q=query,
                part='id',
                type='video',
                maxResults=50,
                regionCode=region,
                order='relevance'
            ).execute()

            # Extract video IDs
            video_ids = [
                item['id']['videoId']
                for item in search_response.get('items', [])
                if item['id'].get('kind') == 'youtube#video' and 'videoId' in item['id']
            ]

            if not video_ids:
                print(f"No videos found for query={query}, region={region}")
                continue

            # Retrieve video statistics and metadata
            video_response = youtube.videos().list(
                part='snippet,statistics',
                id=','.join(video_ids)
            ).execute()

            for item in video_response.get('items', []):
                stats = item.get('statistics', {})
                snippet = item.get('snippet', {})
                all_results.append({
                    'query': query,
                    'region': region,
                    'videoId': item['id'],
                    'title': snippet.get('title'),
                    'channel': snippet.get('channelTitle'),
                    'publishedAt': snippet.get('publishedAt'),
                    'viewCount': int(stats.get('viewCount', 0)),
                    'likeCount': int(stats.get('likeCount', 0)),
                    'commentCount': int(stats.get('commentCount', 0))
                })

            time.sleep(1)  # Prevent hitting the API quota limit
        except Exception as e:
            print(f"Error in query={query}, region={region}: {e}")

# Convert to DataFrame and save
df = pd.DataFrame(all_results)
df.to_csv('youtube_multiquery_demand_data.csv', index=False)

# Aggregate demand matrix: sum statistics by region and query
grouped = df.groupby(['region', 'query'])[['viewCount', 'likeCount', 'commentCount']].sum()
demand_matrix_view = grouped['viewCount'].unstack(fill_value=0)

# Display summary
print("Data collection and aggregation completed. Sample data:")
print(df.head())
