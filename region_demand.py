import pandas as pd
import time
import json
import os
import random
import datetime
import logging
import requests
import math
import pytz
import argparse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configuration
SLACK_WEBHOOK_URL = "Your Slack Webhook"
DEVELOPER_KEY = 'Your API Key'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

#PARAMETERS (tweakable)
MAX_RESULTS = 50
MAX_PAGES = 1
CHUNK_COUNT = 5
QUOTA_USAGE_FILE = 'quota_usage.json'
Quota_Notify_Interval = 100000

#Scoring hyperparameters
MAX_RANK = 10
W_RANK = 0.30
W_POP = 0.60
W_LOCAL = 0.10

#Uniqueness smoothing / cap
OCCURRENCE_CAP = 50
UNIQUENESS_EXP = 0.3

# Fixed period for publishedAfter
FIXED_DAYS = 30
today = datetime.date.today()
published_after = (today - datetime.timedelta(days=FIXED_DAYS)).isoformat() + "T00:00:00Z"

# small mapping region -> language (expand as needed)
REGION_TO_LANG = {
    'JP': 'ja', 'US': 'en', 'GB': 'en', 'IN': 'hi', 'BR': 'pt', 'FR': 'fr',
    'DE': 'de', 'ES': 'es', 'KR': 'ko', 'CN': 'zh', 'TW': 'zh', 'IT': 'it',
    'RU': 'ru', 'MX': 'es', 'CA': 'en', 'AU': 'en', 'ID': 'id'
}

# Logging
logging.basicConfig(
    filename='log.txt',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Slack helper
def send_slack_message(message):
    try:
        payload = {"text": message}
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning(f"Slack Notification missed: {r.text}")
    except Exception as e:
        logging.warning(f"Slack notify failed: {e}")

# utility: chunk list into pieces
def chunked_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# Reset quota per Pacific day
def reset_quota_if_new_day(quota_usage):
    pacific = pytz.timezone("US/Pacific")
    now_pacific = datetime.datetime.now(pacific)
    quota_day_key = now_pacific.strftime("%Y-%m-%d")
    if quota_day_key not in quota_usage:
        logging.info("New Pacific day detected. Resetting quota usage.")
        quota_usage.clear()
        quota_usage[quota_day_key] = 0
    return quota_day_key, quota_usage

# compute per-video scoring (inner + uniqueness)
def compute_scores_for_videos(video_rows, video_occurrence, global_denom_log):
    scored = []
    denom = global_denom_log if global_denom_log > 0 else 1.0
    for v in video_rows:
        vid = v['videoId']
        rank = v['rank']
        view_count = v['viewCount']
        local_hint = v.get('local_hint', 0)
        # rank_score (0..1)
        rank_score = max(0.0, min(1.0, (MAX_RANK - rank + 1) / MAX_RANK))
        # popularity normalized by chosen denom (log10)
        if view_count <= 0:
            popularity = 0.0
        else:
            popularity = math.log10(view_count + 1) / denom
            popularity = max(0.0, min(popularity, 1.0))
        # occurrence / uniqueness (cap and smoothing)
        occ = min(video_occurrence.get(vid, 0), OCCURRENCE_CAP)
        uniqueness = 1.0 / (1.0 + (occ ** UNIQUENESS_EXP))
        # inner weighted sum
        inner = W_RANK * rank_score + W_POP * popularity + W_LOCAL * local_hint
        final_score = inner * uniqueness
        scored.append({
            'videoId': vid,
            'region': v['region'],
            'rank': rank,
            'viewCount': view_count,
            'rank_score': round(rank_score, 6),
            'popularity': round(popularity, 6),
            'local_hint': local_hint,
            'occurrence_count': video_occurrence.get(vid, 0),
            'uniqueness': round(uniqueness, 6),
            'inner_score': round(inner, 6),
            'final_score': round(final_score, 8)
        })
    return scored

#Inspection
def parse_args():
    parser = argparse.ArgumentParser(description="YouTube region demand collector (rank+pop+uniqueness)")
    parser.add_argument("--concept", type=str, default=None,
                        help="(ADDED) If set, process only this single concept (no rotation).")
    parser.add_argument("--regions-file", type=str, default=None,
                        help="(ADDED) Optional: path to a file with region codes (one per line). If omitted, uses 'regions code.txt'.")
    parser.add_argument("--limit-regions", type=int, default=0,
                        help="(ADDED) Optional: limit number of regions processed (useful for tests). 0 means all.")
    return parser.parse_args()

def main():
    args = parse_args() 
    logging.info("Demand Data Collection (rank+pop+local+uniqueness) started")
    send_slack_message("Demand Collection (rank+pop+local+uniqueness) started")

    try:
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

        # load region codes (allow override from CLI) 
        regions_source = args.regions_file if args.regions_file else 'regions code.txt'
        with open(regions_source, 'r', encoding='utf-8') as f:
            regions = [line.strip() for line in f if line.strip()]

        # allow region limiting for tests (CLI)
        if args.limit_regions and args.limit_regions > 0:
            regions = regions[:args.limit_regions]
            logging.info(f"(TEST MODE) Limiting regions to first {args.limit_regions}: {regions}")

        with open('concepts.txt', 'r', encoding='utf-8') as f:
            all_concepts = [line.strip() for line in f if line.strip()]

        with open('query_words.json', 'r', encoding='utf-8') as f:
            concept_queries = json.load(f)

        try:
            with open(QUOTA_USAGE_FILE, 'r', encoding='utf-8') as f:
                quota_usage = json.load(f)
        except Exception as e:
            logging.warning(f"Failed to load quota file: {e}")
            quota_usage = {}

        quota_day_key, quota_usage = reset_quota_if_new_day(quota_usage)
        total_quota = quota_usage[quota_day_key]

        # Concept selection: if --concept provided then process only that concept (no rotation)
        if args.concept:
            concepts = [args.concept]
            logging.info(f"(SINGLE CONCEPT MODE) Will process only: {args.concept}")
        else:
            # rotate concepts
            day_index = today.toordinal() % CHUNK_COUNT
            chunk_size = len(all_concepts) // CHUNK_COUNT + 1
            concepts = all_concepts[day_index * chunk_size : (day_index + 1) * chunk_size]
            logging.info(f"(ROTATION MODE) Processing {len(concepts)} concepts today.")

        for concept in concepts:
            logging.info(f"Processing concept: {concept}")
            query_words = concept_queries.get(concept, [concept])

            # Step A: For each region, collect ordered unique top list (merge queries)
            region_top_lists = {}
            region_top_rows = []
            for region in regions:
                logging.info(f"  -> Searching region: {region}")
                ordered_vids = []
                for query in query_words:
                    next_page_token = None
                    for page in range(MAX_PAGES):
                        if total_quota >= 1000000:
                            send_slack_message("Quota limit reached. Stopping execution.")
                            logging.warning("Quota limit reached before next request.")
                            return

                        try:
                            search_res = youtube.search().list(
                                q=query,
                                part='id',
                                type='video',
                                maxResults=MAX_RESULTS,
                                regionCode=region,
                                order='relevance',
                                publishedAfter=published_after,
                                pageToken=next_page_token
                            ).execute()
                            total_quota += 100  # approximate
                            items = search_res.get('items', [])
                            for it in items:
                                vid = it.get('id', {}).get('videoId')
                                if vid and vid not in ordered_vids:
                                    ordered_vids.append(vid)
                                    region_top_rows.append({'region': region, 'videoId': vid, 'query': query})
                            next_page_token = search_res.get('nextPageToken')
                            if not next_page_token:
                                break
                            time.sleep(0.5)
                        except HttpError as e:
                            logging.error(f"Search API error region={region} query={query}: {e}")
                            break
                        except Exception as e:
                            logging.error(f"Unexpected search error: {e}")
                            break

                region_top_lists[region] = ordered_vids[:MAX_RANK]
                logging.info(f"    top {MAX_RANK} videos for {region}: {region_top_lists[region]}")
                time.sleep(random.uniform(0.5, 1.5))

            # Save region top list for debugging
            try:
                df_top_debug = pd.DataFrame(region_top_rows)
                timestamp = today.isoformat().replace('-', '')
                df_top_debug.to_csv(f'region_toplist_{concept}_{timestamp}.csv', index=False)
            except Exception as e:
                logging.warning(f"Failed to save region_toplist debug file: {e}")

            # Step B: Build global occurrence_count across regions
            video_occurrence = {}
            for region, top_list in region_top_lists.items():
                for vid in top_list:
                    video_occurrence[vid] = video_occurrence.get(vid, 0) + 1

            unique_videos = list(video_occurrence.keys())
            logging.info(f"Unique videos collected for concept {concept}: {len(unique_videos)}")
            if not unique_videos:
                logging.info(f"No videos found for concept {concept}, skipping.")
                continue

            # Step C: Fetch video statistics + snippet for all unique videos (batched)
            video_stats = {}
            for chunk in chunked_list(unique_videos, 50):
                try:
                    resp = youtube.videos().list(
                        part='statistics,snippet',
                        id=','.join(chunk),
                        maxResults=len(chunk)
                    ).execute()
                    total_quota += len(chunk)
                    for item in resp.get('items', []):
                        vid = item.get('id')
                        stats = item.get('statistics', {})
                        snippet = item.get('snippet', {})
                        view_count = int(stats.get('viewCount', 0)) if stats.get('viewCount') else 0
                        channel_id = snippet.get('channelId')
                        default_lang = snippet.get('defaultLanguage') or snippet.get('defaultAudioLanguage')
                        video_stats[vid] = {
                            'viewCount': view_count,
                            'channelId': channel_id,
                            'defaultLanguage': default_lang
                        }
                except HttpError as e:
                    logging.error(f"videos.list error: {e}")
                except Exception as e:
                    logging.error(f"Unexpected videos.list error: {e}")
                time.sleep(0.5)

            # Step D: Fetch channel country
            channel_ids = list({v.get('channelId') for v in video_stats.values() if v.get('channelId')})
            channel_country = {}
            for chunk in chunked_list(channel_ids, 50):
                try:
                    resp = youtube.channels().list(
                        part='snippet',
                        id=','.join(chunk),
                        maxResults=len(chunk)
                    ).execute()
                    total_quota += len(chunk)
                    for ch in resp.get('items', []):
                        cid = ch.get('id')
                        sn = ch.get('snippet', {})
                        country = sn.get('country')
                        if country:
                            channel_country[cid] = country.upper()
                except HttpError as e:
                    logging.error(f"channels.list error: {e}")
                except Exception as e:
                    logging.error(f"Unexpected channels.list error: {e}")
                time.sleep(0.5)

            # Step E: Build per-region video_rows
            all_video_rows = []
            for region in regions:
                top_list = region_top_lists.get(region, [])
                if not top_list:
                    continue
                for idx, vid in enumerate(top_list, start=1):
                    view_count = video_stats.get(vid, {}).get('viewCount', 0)
                    local_hint = 0
                    ch_id = video_stats.get(vid, {}).get('channelId')
                    if ch_id and channel_country.get(ch_id):
                        if channel_country.get(ch_id) == region.upper():
                            local_hint = 1
                    else:
                        default_lang = video_stats.get(vid, {}).get('defaultLanguage')
                        region_lang = REGION_TO_LANG.get(region.upper())
                        if default_lang and region_lang and default_lang.lower().startswith(region_lang):
                            local_hint = 1
                    all_video_rows.append({
                        'videoId': vid,
                        'rank': idx,
                        'viewCount': view_count,
                        'region': region,
                        'local_hint': local_hint
                    })

            # compute denom: use 95th percentile of viewCounts of collected videos to avoid single outlier
            view_counts = [r['viewCount'] for r in all_video_rows if r['viewCount']>0]
            if not view_counts:
                global_denom_log = 1.0
            else:
                p95 = int(pd.Series(view_counts).quantile(0.95))
                global_denom_log = math.log10(p95 + 1) if p95 > 0 else 1.0

            # compute scored videos
            scored_videos = compute_scores_for_videos(all_video_rows, video_occurrence, global_denom_log)

            # aggregate per-region final scores
            region_score_map = {}
            for sv in scored_videos:
                region_score_map[sv['region']] = region_score_map.get(sv['region'], 0.0) + sv['final_score']

            region_scores = [{'concept': concept, 'region': r, 'region_score': round(s, 8)} for r, s in region_score_map.items()]

            # Step F: Save outputs (region-level and video-level detail)
            timestamp = today.isoformat().replace('-', '')
            region_df = pd.DataFrame(region_scores)
            detail_df = pd.DataFrame(scored_videos)

            out_region = f'region_score_{concept}_{timestamp}.csv'
            out_detail = f'video_score_{concept}_{timestamp}.csv'
            region_df.to_csv(out_region, index=False)
            detail_df.to_csv(out_detail, index=False)

            logging.info(f"Saved {out_region} and {out_detail}")
            send_slack_message(f"Completed concept {concept}. Saved {out_region} and {out_detail}")

            # update quota file
            quota_usage[quota_day_key] = total_quota
            with open(QUOTA_USAGE_FILE, 'w', encoding='utf-8') as f:
                json.dump(quota_usage, f, ensure_ascii=False, indent=2)

    except Exception as e:
        logging.exception(f"Fatal Error: {e}")
        send_slack_message(f"Fatal Error: {e}")

if __name__ == "__main__":
    main()
