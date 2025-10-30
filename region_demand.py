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

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- optional langdetect ----------
try:
    from langdetect import detect_langs, LangDetectException
    _HAS_LANGDETECT = True
except Exception:
    _HAS_LANGDETECT = False

# ---------- env & constants ----------
DEVELOPER_KEY = os.environ.get('YOUTUBE_API_KEY')
SLACK_WEBHOOK_URL = os.environ.get('YOUTUBE_SLACK_WEBHOOK_URL')
if not DEVELOPER_KEY:
    raise RuntimeError("YOUTUBE_API_KEY is not set in environment. Please set it permanently on each PC.")

YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

# Round-robin: 1回で何件の concept を処理するか
TARGET_COUNT = 6

# Parameters
MAX_RESULTS = 25         # per search / mostPopular fetch
MAX_RANK = 10
W_RANK = 0.30
W_POP = 0.45
W_LOCAL = 0.25

# Paths (absolute) — 状態・ログはスクリプト隣の state/ に固定
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)

QUOTA_USAGE_FILE = os.path.join(STATE_DIR, 'quota_usage.json')
LAST_FETCH_FILE  = os.path.join(STATE_DIR, 'last_fetch.json')   # publishedAfter 用に成功時刻を保持
RR_STATE_FILE    = os.path.join(STATE_DIR, 'rr_state.json')     # ラウンドロビンの次位置

# Quota cost (rough)
SEARCH_COST = 100
VIDEOS_COST = 1
CHANNELS_COST = 1
MOSTPOPULAR_COST = 200

# small mapping region -> ISO language
REGION_TO_LANG = {
    'JP': 'ja', 'US': 'en', 'GB': 'en', 'IN': 'hi', 'BR': 'pt', 'FR': 'fr',
    'DE': 'de', 'ES': 'es', 'KR': 'ko', 'CN': 'zh', 'TW': 'zh', 'IT': 'it',
    'RU': 'ru', 'MX': 'es', 'CA': 'en', 'AU': 'en', 'ID': 'id'
}

# ---------- logging (file + console) ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(os.path.join(STATE_DIR, 'log.txt'), encoding='utf-8')
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)

# ---------- utilities ----------
def send_slack_message(message: str):
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

def safe_execute(request_func, max_retries=5, initial_backoff=1.0):
    backoff = initial_backoff
    for attempt in range(1, max_retries + 1):
        try:
            return request_func()
        except HttpError as e:
            status = getattr(getattr(e, "resp", None), "status", None)
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

def lang_prob_matches(text: str, target_lang: str) -> float:
    if not text or not target_lang or not _HAS_LANGDETECT:
        return 0.0
    try:
        langs = detect_langs(text)
        for l in langs:
            if l.lang.lower().startswith(target_lang.lower()):
                return float(l.prob)
        return 0.0
    except Exception:
        return 0.0

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

# --- 追加: 出力先フォルダ(JST)を自動生成 ---
def ensure_output_dir(base_dir: str) -> str:
    """
    base_dir/Current Version/demand_YYMM/demand_YYMMDD を（JST基準で）自動生成して返す
    例: 2025-10-29 JST -> base_dir/Current Version/demand_2510/demand_251029
    """
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.datetime.now(jst)
    yy = now_jst.year % 100
    mm = now_jst.month
    dd = now_jst.day

    root = os.path.join(base_dir, "Current Version")
    month_dir = os.path.join(root, f"demand_{yy:02d}{mm:02d}")
    day_dir   = os.path.join(month_dir, f"demand_{yy:02d}{mm:02d}{dd:02d}")
    os.makedirs(day_dir, exist_ok=True)
    return day_dir

# ---------- last_fetch i/o (publishedAfter 用に成功時刻のみ) ----------
def load_last_fetch():
    if os.path.exists(LAST_FETCH_FILE):
        try:
            with open(LAST_FETCH_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 旧 {"concept":"ts"} → 新 {"concept":{"last_success":ts}}
                for k, v in list(data.items()):
                    if isinstance(v, str):
                        data[k] = {"last_success": v}
                return data
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

# ---------- round-robin state ----------
def _load_rr_state():
    try:
        with open(RR_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"next_idx": 0}

def _save_rr_state(state):
    try:
        with open(RR_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Failed to save rr_state: {e}")

def select_concepts_round_robin(all_concepts, target_count=TARGET_COUNT):
    """
    concepts.txt の順にラウンドロビンで target_count 件を取得。
    wrap-around し、次インデックスを rr_state.json に保存。
    """
    n = len(all_concepts)
    if n == 0:
        return []

    state = _load_rr_state()
    start_idx = state.get("next_idx", 0) % n

    picked = []
    i = start_idx
    for _ in range(min(target_count, n)):
        picked.append(all_concepts[i])
        i = (i + 1) % n

    _save_rr_state({"next_idx": i})
    logging.info(f"[rr] start_idx={start_idx} -> next_idx={i} picked={picked}")
    return picked

# ---------- scoring ----------
def compute_uniqueness_tf_idf(occurrence, N_regions):
    if N_regions <= 1:
        return 1.0
    try:
        idf = math.log((N_regions + 1) / (1 + max(0, occurrence)))
        denom = math.log(N_regions + 1)
        if denom <= 0:
            return 1.0
        return max(0.0, idf / denom)
    except Exception:
        return 1.0

def compute_scores_for_videos(video_rows, video_occurrence, N_regions, region_denom_log, trending_set):
    scored = []
    for v in video_rows:
        vid = v['videoId']
        rank = v['rank']
        view_count = v['viewCount']
        local_hint = v.get('local_hint', 0.0)

        rank_clamped = max(1, min(rank, MAX_RANK))
        rank_score = (MAX_RANK - rank_clamped + 1) / MAX_RANK

        denom = region_denom_log.get(v['region'], 1.0) or 1.0
        popularity = 0.0 if view_count <= 0 else max(0.0, min(math.log10(view_count + 1) / denom, 1.0))

        occ = video_occurrence.get(vid, 0)
        uniqueness = compute_uniqueness_tf_idf(occ, N_regions)

        inner = W_RANK * rank_score + W_POP * popularity + W_LOCAL * local_hint
        trend_boost = 1.0 + (0.10 if vid in trending_set else 0.0)
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

# ---------- CLI ----------
def parse_args():
    parser = argparse.ArgumentParser(description="YouTube region demand collector (simple round-robin) with JST output folders")
    parser.add_argument("--concept", type=str, default=None, help="Run a single concept (overrides round-robin).")
    parser.add_argument("--regions-file", type=str, default=None, help="Path to region codes (one per line).")
    parser.add_argument("--limit-regions", type=int, default=0, help="Limit number of regions processed.")
    parser.add_argument("--target-count", type=int, default=TARGET_COUNT, help="Concepts to process per run.")
    return parser.parse_args()

# ---------- main ----------
def main():
    args = parse_args()
    logging.info("Demand Data Collection (simple RR, JST folders) started")
    send_slack_message("Demand Collection (simple RR, JST folders) started")

    # publishedAfter 用の成功時刻
    last_fetch = load_last_fetch()
    today = datetime.date.today()

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

    # regions
    regions_source = args.regions_file if args.regions_file else 'regions code.txt'
    if not os.path.exists(regions_source):
        logging.warning(f"Regions file {regions_source} not found. Using default subset.")
        regions = list(REGION_TO_LANG.keys())
    else:
        with open(regions_source, 'r', encoding='utf-8') as f:
            regions = [line.strip() for line in f if line.strip()]
    if args.limit_regions and args.limit_regions > 0:
        regions = regions[:args.limit_regions]

    # concepts & queries
    with open('concepts.txt', 'r', encoding='utf-8') as f:
        all_concepts = [line.strip() for line in f if line.strip()]
    with open('query_words.json', 'r', encoding='utf-8') as f:
        concept_queries = json.load(f)

    # quota
    try:
        with open(QUOTA_USAGE_FILE, 'r', encoding='utf-8') as f:
            quota_usage = json.load(f)
    except Exception:
        quota_usage = {}
    quota_day_key, quota_usage = reset_quota_if_new_day(quota_usage)
    total_quota = quota_usage.get(quota_day_key, 0)

    # concept selection: 単体指定がなければラウンドロビン
    if args.concept:
        concepts = [args.concept]
    else:
        concepts = select_concepts_round_robin(all_concepts, target_count=args.target_count)

    plan_msg = f"[plan] concepts to run: {concepts}"
    logging.info(plan_msg)
    send_slack_message(plan_msg)

    # unique (order-preserving)
    seen = set()
    concepts = [c for c in concepts if not (c in seen or seen.add(c))]

    # --- 追加: 保存先ディレクトリ（JST）を一度だけ作成 ---
    target_dir = ensure_output_dir(BASE_DIR)

    for concept in concepts:
        logging.info(f"Processing concept: {concept}")
        query_words = concept_queries.get(concept, [concept])
        logging.info(f"Concept `{concept}` queries: {query_words}")

        last_success = last_fetch.get(concept, {}).get("last_success")
        if last_success:
            published_after = last_success
        else:
            published_after = (datetime.date.today() - datetime.timedelta(days=30)).isoformat() + "T00:00:00Z"

        # -------- collect (search + mostPopular) --------
        region_top_lists = {}
        all_region_sources = {}  # region -> {vid: set('search'/'trending')}
        trending_set = set()

        for region in regions:
            logging.info(f" Searching region {region} for concept {concept}")
            vids_ordered = []
            region_sources = {}

            # 1) query searches
            for q in query_words:
                next_page_token = None
                for _ in range(1):  # 1 page (tunable)
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

            # 2) mostPopular
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

            region_top_lists[region] = vids_ordered[:MAX_RANK]
            all_region_sources[region] = region_sources
            time.sleep(random.uniform(0.2, 0.6))

        # --- 修正: デバッグCSVも日付フォルダへ保存 ---
        dbg_file = os.path.join(
            target_dir,
            f"region_toplist_debug_{concept}_{today.isoformat().replace('-','')}.csv"
        )
        try:
            with open(dbg_file, 'w', newline='', encoding='utf-8') as fh:
                writer = csv.DictWriter(fh, fieldnames=['concept','region','videoId','sources'])
                writer.writeheader()
                for r, mapping in all_region_sources.items():
                    for vid, sources in mapping.items():
                        writer.writerow({'concept': concept, 'region': r, 'videoId': vid, 'sources': ';'.join(sorted(sources))})
            logging.info(f"Saved region_toplist debug to {dbg_file}")
        except Exception as e:
            logging.warning(f"Failed writing debug top list: {e}")

        # occurrence / unique videos
        video_occurrence = {}
        for region, lst in region_top_lists.items():
            for vid in lst:
                video_occurrence[vid] = video_occurrence.get(vid, 0) + 1
        unique_videos = list(video_occurrence.keys())
        if not unique_videos:
            logging.info(f"No videos found for concept {concept}")
            continue

        # stats & snippet
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

        # channel countries
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

        # assemble rows with local_hint
        all_video_rows = []
        for region in regions:
            target_lang = REGION_TO_LANG.get(region.upper())
            top_list = region_top_lists.get(region, [])
            for idx, vid in enumerate(top_list, start=1):
                vs = video_stats.get(vid, {})
                view_count = vs.get('viewCount', 0)
                ch_id = vs.get('channelId')
                title = vs.get('title', '')
                desc = vs.get('description', '')
                default_lang = vs.get('defaultLanguage')

                channel_match = 0.0
                local_hint_source = ''
                if ch_id and channel_country.get(ch_id) and channel_country.get(ch_id).upper() == region.upper():
                    channel_match = 1.0
                    local_hint_source = 'channel_country'

                # language matches
                lang_match_prob = 0.0
                if default_lang and target_lang and default_lang.lower().startswith(target_lang):
                    lang_match_prob = 0.95
                    if not local_hint_source:
                        local_hint_source = 'default_lang'
                else:
                    sample_text = (title + ' ' + desc).strip()
                    lang_match_prob = lang_prob_matches(sample_text, target_lang) if sample_text else 0.0
                    if not local_hint_source:
                        local_hint_source = 'langdetect' if (sample_text and _HAS_LANGDETECT) else 'none'

                # combine
                if channel_match >= 1.0:
                    local_hint = 1.0
                else:
                    local_hint = min(0.9, lang_match_prob * 0.9)
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

        # region-wise denom (p95)
        region_view_counts = {r: [] for r in regions}
        for row in all_video_rows:
            if row['ViewCount'] if False else row['viewCount'] > 0:  # keep casing safe
                region_view_counts[row['region']].append(row['viewCount'])
        region_denom_log = {}
        for r, vlist in region_view_counts.items():
            if vlist:
                p95 = int(pd.Series(vlist).quantile(0.95))
                region_denom_log[r] = math.log10(p95 + 1) if p95 > 0 else 1.0
            else:
                region_denom_log[r] = 1.0

        # score
        N_regions = len(regions)
        scored_videos = compute_scores_for_videos(all_video_rows, video_occurrence, N_regions, region_denom_log, trending_set)

        # aggregate per-region
        region_score_map = {}
        for sv in scored_videos:
            region_score_map[sv['region']] = region_score_map.get(sv['region'], 0.0) + sv['final_score']

        region_scores = [{'concept': concept, 'region': r, 'region_score': round(s, 8)} for r, s in region_score_map.items()]

        # --- 修正: 出力CSVを日付フォルダへ保存 ---
        timestamp = today.isoformat().replace('-', '')
        region_df = pd.DataFrame(region_scores)
        detail_df = pd.DataFrame(scored_videos)

        out_region = os.path.join(target_dir, f'region_score_{concept}_{timestamp}.csv')
        out_detail = os.path.join(target_dir, f'video_score_{concept}_{timestamp}.csv')
        try:
            region_df.to_csv(out_region, index=False)
            detail_df.to_csv(out_detail, index=False)
            logging.info(f"Saved {out_region} and {out_detail}")
            send_slack_message(f"Completed concept {concept}. Saved {out_region} and {out_detail}")
        except Exception as e:
            logging.warning(f"Failed saving outputs: {e}")

        # 成功時刻を記録（次回 publishedAfter に利用）
        last_fetch.setdefault(concept, {})
        last_fetch[concept]["last_success"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        save_last_fetch(last_fetch)

        # quota 保存
        quota_usage[quota_day_key] = total_quota
        try:
            with open(QUOTA_USAGE_FILE, 'w', encoding='utf-8') as f:
                json.dump(quota_usage, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.warning(f"Failed to update quota file: {e}")

    logging.info("All done.")
    send_slack_message("Demand Collection (simple RR, JST folders) finished")

if __name__ == "__main__":
    main()
