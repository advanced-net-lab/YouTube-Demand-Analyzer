#!/usr/bin/env python3
import os
import time
import json
import random
import datetime
import logging
import requests
import math
import pytz
import argparse
import csv

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Optional dependency for language detection
from langdetect import detect_langs, LangDetectException

DEVELOPER_KEY = os.environ.get('YOUTUBE_API_KEY')
SLACK_WEBHOOK_URL = os.environ.get('YOUTUBE_SLACK_WEBHOOK_URL')

if not DEVELOPER_KEY:
    raise RuntimeError("YOUTUBE_API_KEY is not set in environment. Please set it permanently on each PC.")

YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

# Cooldown Setting
COOLDOWN_HOURS = 10  # 直近10時間以内なら取得済みのconceptをスキップ

# PARAMETERS
MAX_RESULTS = 25         # per search / mostPopular fetch
MAX_RANK = 10
W_RANK = 0.30
W_POP = 0.45
W_LOCAL = 0.25

# Uniqueness params (TF-IDF style will be used)
OCCURRENCE_CAP = 9999

# Quota file
QUOTA_USAGE_FILE = 'quota_usage.json'
LAST_FETCH_FILE = 'last_fetch.json'

# Quota cost approximations (tweak if you know exact)
SEARCH_COST = 100
VIDEOS_COST = 1
CHANNELS_COST = 1
MOSTPOPULAR_COST = 200

# small mapping region -> ISO language (for simple match)
REGION_TO_LANG = {
    'JP': 'ja', 'US': 'en', 'GB': 'en', 'IN': 'hi', 'BR': 'pt', 'FR': 'fr',
    'DE': 'de', 'ES': 'es', 'KR': 'ko', 'CN': 'zh', 'TW': 'zh', 'IT': 'it',
    'RU': 'ru', 'MX': 'es', 'CA': 'en', 'AU': 'en', 'ID': 'id'
}

# ---------------------------
# Logging setup (file + console)
# ---------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# avoid duplicate handlers on re-import
if not logger.handlers:
    fh = logging.FileHandler('log.txt', encoding='utf-8')
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)

# Slack helper
def send_slack_message(message):
    if not SLACK_WEBHOOK_URL:
        logging.debug("No SLACK_WEBHOOK_URL set, skipping slack notify.")
        return
    try:
        payload = {"text": message}
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning(f"Slack Notification missed: {r.status_code} {r.text}")
    except Exception as e:
        logging.warning(f"Slack notify failed: {e}")

# safe execute wrapper
def safe_execute(request_func, max_retries=5, initial_backoff=1.0):
    backoff = initial_backoff
    for attempt in range(1, max_retries + 1):
        try:
            return request_func()
        except HttpError as e:
            status = None
            try:
                status = e.resp.status
            except Exception:
                pass
            logging.warning(f"HttpError on attempt {attempt}: status={status} error={e}")
            if attempt == max_retries:
                raise
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff *= 2
        except Exception as e:
            logging.warning(f"Unexpected error on API call attempt {attempt}: {e}")
            if attempt == max_retries:
                raise
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff *= 2
    raise RuntimeError("safe_execute reached unreachable point")

# language detection utility: returns probability that text language == target_lang (0..1)
def lang_prob_matches(text, target_lang):
    if not text or not target_lang:
        return 0.0
    try:
        langs = detect_langs(text)
        for l in langs:
            code = l.lang
            prob = l.prob
            if code.lower().startswith(target_lang.lower()):
                return float(prob)
        return 0.0
    except LangDetectException:
        return 0.0
    except Exception as e:
        logging.debug(f"langdetect failed: {e}")
        return 0.0

# compute uniqueness TF-IDF style
def compute_uniqueness_tf_idf(occurrence, N_regions):
    if N_regions <= 1:
        return 1.0
    occ = max(1, occurrence)
    idf = math.log((N_regions + 1) / (1 + occurrence))
    denom = math.log(N_regions + 1)
    if denom <= 0:
        return 1.0
    uniqueness = max(0.0, idf / denom)
    return uniqueness

# compute scores; now accepts region_denom_log and trending_set for trend boost
def compute_scores_for_videos(video_rows, video_occurrence, N_regions, region_denom_log, trending_set):
    scored = []
    for v in video_rows:
        vid = v['videoId']
        rank = v['rank']
        view_count = v['viewCount']
        local_hint = v.get('local_hint', 0.0)

        rank_clamped = max(1, min(rank, MAX_RANK))
        rank_score = (MAX_RANK - rank_clamped + 1) / MAX_RANK

        denom = region_denom_log.get(v['region'], 1.0)
        if denom <= 0:
            denom = 1.0

        if view_count <= 0:
            popularity = 0.0
        else:
            popularity = math.log10(view_count + 1) / denom
            popularity = max(0.0, min(popularity, 1.0))

        occ = video_occurrence.get(vid, 0)
        uniqueness = compute_uniqueness_tf_idf(occ, N_regions)

        inner = W_RANK * rank_score + W_POP * popularity + W_LOCAL * local_hint

        # trend boost if trending_set contains vid
        trend_boost = 1.0 + (0.10 if vid in trending_set else 0.0)  # 10% boost

        final_score = inner * uniqueness * trend_boost

        scored.append({
            'videoId': vid,
            'region': v['region'],
            'rank': rank,
            'viewCount': view_count,
            'rank_score': round(rank_score, 6),
            'popularity': round(popularity, 6),
            'local_hint': round(local_hint, 6),
            'local_hint_source': v.get('local_hint_source', ''),
            'occurrence_count': occ,
            'uniqueness': round(uniqueness, 6),
            'inner_score': round(inner, 6),
            'final_score': round(final_score, 8)
        })
    return scored

# helpers
def chunked_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def reset_quota_if_new_day(quota_usage):
    pacific = pytz.timezone("US/Pacific")
    now_pacific = datetime.datetime.now(pacific)
    quota_day_key = now_pacific.strftime("%Y-%m-%d")
    if quota_day_key not in quota_usage:
        quota_usage[quota_day_key] = 0
    return quota_day_key, quota_usage

# CLI
def parse_args():
    parser = argparse.ArgumentParser(description="YouTube region demand improved collector (patched)")
    parser.add_argument("--concept", type=str, default=None)
    parser.add_argument("--regions-file", type=str, default=None)
    parser.add_argument("--limit-regions", type=int, default=0)
    # NEW: cooldown override
    parser.add_argument("--force", action="store_true",
                        help="Ignore cooldown and force-run concepts.")
    return parser.parse_args()

def load_last_fetch():
    if os.path.exists(LAST_FETCH_FILE):
        try:
            with open(LAST_FETCH_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Failed to parse {LAST_FETCH_FILE}: {e}")
            return {}
    return {}

def save_last_fetch(last_fetch):
    try:
        with open(LAST_FETCH_FILE, 'w', encoding='utf-8') as f:
            json.dump(last_fetch, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Failed to save last_fetch: {e}")

def main():
    args = parse_args()
    logging.info("Demand Data Collection (patched) started")
    send_slack_message("Demand Collection (patched) started")

    last_fetch = load_last_fetch()
    today = datetime.date.today()

    # for cooldown comparison
    now_utc = datetime.datetime.utcnow()
    cooldown_delta = datetime.timedelta(hours=COOLDOWN_HOURS)

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

    regions_source = args.regions_file if args.regions_file else 'regions code.txt'
    if not os.path.exists(regions_source):
        logging.warning(f"Regions file {regions_source} not found. Using default subset.")
        regions = list(REGION_TO_LANG.keys())
    else:
        with open(regions_source, 'r', encoding='utf-8') as f:
            regions = [line.strip() for line in f if line.strip()]

    if args.limit_regions and args.limit_regions > 0:
        regions = regions[:args.limit_regions]

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
    total_quota = quota_usage.get(quota_day_key, 0)

    # decide concepts chunking (same as before)
    day_index = today.toordinal() % 5
    chunk_size = max(1, len(all_concepts) // 5)
    concepts = all_concepts[day_index * chunk_size : (day_index + 1) * chunk_size]
    if args.concept:
        concepts = [args.concept]

    for concept in concepts:
        # NEW: cooldown skip (unless --force)
        last_time = last_fetch.get(concept)
        if last_time and not args.force:
            try:
                last_dt = datetime.datetime.strptime(last_time, "%Y-%m-%dT%H:%M:%SZ")
                if (now_utc - last_dt) < cooldown_delta:
                    msg = (f"Skip concept '{concept}': last fetched at {last_dt} UTC "
                           f"(within {COOLDOWN_HOURS}h cooldown). Use --force to override.")
                    logging.info(msg)
                    send_slack_message(msg)
                    continue
            except Exception as e:
                logging.warning(f"Failed to parse last_fetch for {concept}: {e}")

        logging.info(f"Processing concept: {concept}")
        query_words = concept_queries.get(concept, [concept])
        logging.info(f"Concept `{concept}` queries: {query_words}")
        print(f"[DEBUG] concept={concept} queries={query_words}")

        if last_time:
            published_after = last_time
        else:
            published_after = (datetime.date.today() - datetime.timedelta(days=30)).isoformat() + "T00:00:00Z"

        # collect per-region lists (merged search + mostPopular)
        region_top_lists = {}
        all_region_sources = {}  # region -> {vid: set(sources)}
        trending_set = set()     # for trend_boost

        for region in regions:
            logging.info(f" Searching region {region} for concept {concept}")
            vids_ordered = []
            region_sources = {}

            # 1) query-based searches
            for q in query_words:
                next_page_token = None
                for page in range(1):  # limited pages for speed; tune as needed
                    try:
                        def do_search():
                            return youtube.search().list(
                                q=q,
                                part='id',
                                type='video',
                                maxResults=MAX_RESULTS,
                                regionCode=region,
                                order='relevance',
                                publishedAfter=published_after,
                                pageToken=next_page_token
                            ).execute()
                        res = safe_execute(do_search)
                        total_quota += SEARCH_COST
                        for it in res.get('items', []):
                            vid = it.get('id', {}).get('videoId')
                            if vid and vid not in vids_ordered:
                                vids_ordered.append(vid)
                                region_sources.setdefault(vid, set()).add('search')
                        next_page_token = res.get('nextPageToken')
                        if not next_page_token:
                            break
                        time.sleep(0.3)
                    except Exception as e:
                        logging.warning(f"Search failed region={region} q={q} : {e}")
                        break

            # 2) mostPopular fetch for this region (trending)
            try:
                def do_mostpopular():
                    return youtube.videos().list(
                        part='id,snippet,statistics',
                        chart='mostPopular',
                        regionCode=region,
                        maxResults=MAX_RESULTS
                    ).execute()
                mp_res = safe_execute(do_mostpopular)
                total_quota += MOSTPOPULAR_COST
                for item in mp_res.get('items', []):
                    vid = item.get('id')
                    if vid:
                        trending_set.add(vid)
                        if vid not in vids_ordered:
                            vids_ordered.append(vid)
                        region_sources.setdefault(vid, set()).add('trending')
            except Exception as e:
                logging.warning(f"mostPopular failed for region {region}: {e}")

            # limit to top MAX_RANK
            region_top_lists[region] = vids_ordered[:MAX_RANK]
            all_region_sources[region] = region_sources
            time.sleep(random.uniform(0.2, 0.6))

        # save region_toplist debug CSV (shows per-region video sources)
        dbg_file = f"region_toplist_debug_{concept}_{today.isoformat().replace('-','')}.csv"
        try:
            with open(dbg_file, 'w', newline='', encoding='utf-8') as fh:
                writer = csv.DictWriter(fh, fieldnames=['concept','region','videoId','sources'])
                writer.writeheader()
                for r, mapping in all_region_sources.items():
                    for vid, sources in mapping.items():
                        writer.writerow({'concept': concept, 'region': r, 'videoId': vid, 'sources': ';'.join(sorted(sources))})
            logging.info(f"Saved region_toplist debug to {dbg_file}")
            print(f"[DEBUG] saved region_toplist debug to {dbg_file}")
        except Exception as e:
            logging.warning(f"Failed writing debug top list: {e}")

        # build occurrence counts
        video_occurrence = {}
        for region, lst in region_top_lists.items():
            for vid in lst:
                video_occurrence[vid] = video_occurrence.get(vid, 0) + 1

        unique_videos = list(video_occurrence.keys())
        if not unique_videos:
            logging.info(f"No videos found for concept {concept}")
            continue

        # fetch video stats & snippet in chunks
        video_stats = {}
        for chunk in chunked_list(unique_videos, 50):
            try:
                def do_videos_list():
                    return youtube.videos().list(
                        part='statistics,snippet',
                        id=','.join(chunk)
                    ).execute()
                resp = safe_execute(do_videos_list)
                total_quota += VIDEOS_COST * len(chunk)
                for item in resp.get('items', []):
                    vid = item.get('id')
                    stats = item.get('statistics', {})
                    sn = item.get('snippet', {})
                    view_count = int(stats.get('viewCount', 0)) if stats.get('viewCount') else 0
                    channel_id = sn.get('channelId')
                    title = sn.get('title') or ''
                    description = sn.get('description') or ''
                    default_lang = sn.get('defaultLanguage') or sn.get('defaultAudioLanguage')
                    video_stats[vid] = {
                        'viewCount': view_count,
                        'channelId': channel_id,
                        'title': title,
                        'description': description,
                        'defaultLanguage': default_lang
                    }
            except Exception as e:
                logging.warning(f"videos.list failed chunk: {e}")
            time.sleep(0.3)

        # fetch channel countries
        channel_ids = list({v.get('channelId') for v in video_stats.values() if v.get('channelId')})
        channel_country = {}
        for chunk in chunked_list(channel_ids, 50):
            try:
                def do_channels_list():
                    return youtube.channels().list(
                        part='snippet',
                        id=','.join(chunk)
                    ).execute()
                resp = safe_execute(do_channels_list)
                total_quota += CHANNELS_COST * len(chunk)
                for ch in resp.get('items', []):
                    cid = ch.get('id')
                    sn = ch.get('snippet', {})
                    country = sn.get('country')
                    if country:
                        channel_country[cid] = country.upper()
            except Exception as e:
                logging.warning(f"channels.list failed: {e}")
            time.sleep(0.3)

        # build per-region rows with local_hint (continuous) and source
        all_video_rows = []
        for region in regions:
            target_lang = REGION_TO_LANG.get(region.upper())
            top_list = region_top_lists.get(region, [])
            for idx, vid in enumerate(top_list, start=1):
                view_count = video_stats.get(vid, {}).get('viewCount', 0)
                ch_id = video_stats.get(vid, {}).get('channelId')
                title = video_stats.get(vid, {}).get('title', '')
                desc = video_stats.get(vid, {}).get('description', '')
                default_lang = video_stats.get(vid, {}).get('defaultLanguage')

                channel_match = 0.0
                local_hint_source = ''
                if ch_id and channel_country.get(ch_id):
                    if channel_country.get(ch_id).upper() == region.upper():
                        channel_match = 1.0
                        local_hint_source = 'channel_country'

                lang_match_prob = 0.0
                # first, try snippet default language
                if default_lang and target_lang and default_lang.lower().startswith(target_lang):
                    lang_match_prob = 0.95
                    if not local_hint_source:
                        local_hint_source = 'default_lang'
                else:
                    # fallback to language detection on title+desc (short text)
                    sample_text = (title + ' ' + desc).strip()
                    lang_match_prob = lang_prob_matches(sample_text, target_lang) if sample_text else 0.0
                    if not local_hint_source:
                        local_hint_source = 'langdetect' if sample_text else 'none'

                # combine into continuous local_hint
                if channel_match >= 1.0:
                    local_hint = 1.0
                else:
                    # scale language prob to a value up to 0.9
                    local_hint = min(0.9, lang_match_prob * 0.9)
                    # small boost if default_lang existed
                    if default_lang:
                        local_hint = max(local_hint, 0.6)

                all_video_rows.append({
                    'videoId': vid,
                    'rank': idx,
                    'viewCount': view_count,
                    'region': region,
                    'local_hint': local_hint,
                    'local_hint_source': local_hint_source
                })

        # compute region-wise denom (p95) for popularity normalization
        import pandas as pd
        region_view_counts = {r: [] for r in regions}
        for row in all_video_rows:
            region_view_counts[row['region']].append(row['viewCount'])
        region_denom_log = {}
        for r, vlist in region_view_counts.items():
            if vlist:
                p95 = int(pd.Series(vlist).quantile(0.95))
                region_denom_log[r] = math.log10(p95 + 1) if p95 > 0 else 1.0
            else:
                region_denom_log[r] = 1.0

        # compute scores (pass trending_set and region_denom_log)
        N_regions = len(regions)
        scored_videos = compute_scores_for_videos(all_video_rows, video_occurrence, N_regions, region_denom_log, trending_set)

        # aggregate per-region
        region_score_map = {}
        for sv in scored_videos:
            region_score_map[sv['region']] = region_score_map.get(sv['region'], 0.0) + sv['final_score']

        region_scores = [{'concept': concept, 'region': r, 'region_score': round(s, 8)} for r, s in region_score_map.items()]

        # save outputs (include local_hint_source column in detail CSV)
        timestamp = today.isoformat().replace('-', '')
        region_df = pd.DataFrame(region_scores)
        detail_df = pd.DataFrame(scored_videos)

        out_region = f'region_score_{concept}_{timestamp}.csv'
        out_detail = f'video_score_{concept}_{timestamp}.csv'
        try:
            region_df.to_csv(out_region, index=False)
            detail_df.to_csv(out_detail, index=False)
            logging.info(f"Saved {out_region} and {out_detail}")
            send_slack_message(f"Completed concept {concept}. Saved {out_region} and {out_detail}")
        except Exception as e:
            logging.warning(f"Failed saving outputs: {e}")

        # update last_fetch
        last_fetch[concept] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        save_last_fetch(last_fetch)

        # update quota usage file
        quota_usage[quota_day_key] = total_quota
        try:
            with open(QUOTA_USAGE_FILE, 'w', encoding='utf-8') as f:
                json.dump(quota_usage, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.warning(f"Failed to update quota file: {e}")

    logging.info("All done.")
    send_slack_message("Improved Demand Collection (patched) finished")

if __name__ == "__main__":
    main()
