import requests
import sys
from datetime import datetime
from pathlib import Path
from slugify import slugify
from typing import List, Dict, Optional
import random
import time
import json


SUBREDDIT_NAME = 'castaneda'
MAX_RETRIES = 5
ARCHIVE_DIR = Path('./archive')
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1"
}


# scrolls through all posts on the /hot page and gets the post_ids
# unfortunately reddit has a limit of only showing the 1000 most recent posts
# any posts before that cannot be retrieved this way and must be accessed through id directly
# in the case of /r/castaneda this is not an issue as a post index has been kept in the wiki
# different method below for archival based on index
def get_post_ids_from_scraping(stop_at_id=None) -> List[str]:
    ids = []
    count = 25

    def get_url(after_id):
        return f"https://www.reddit.com/r/{SUBREDDIT_NAME}/hot.json?count={count}&after={after_id}"

    def get_posts(after_id):
        posts = []
        url = get_url(after_id)
        print("request:", url)
        print("")
        resp = get_json(url)

        if not resp:
            print("no resp")
            return
        d = resp['data']
        for post in d.get('children', []):
            p = post['data']
            posts.append(p)
        return posts

    print(f"scraping post ids of /r/{SUBREDDIT_NAME}")
    print("-" * 33)
    def fetch_loop():
        current_after_id = ""
        while posts := get_posts(current_after_id):
            for i, post in enumerate(posts):
                if stop_at_id is not None and post['id'] == stop_at_id:
                    print("reached stop_at_id thats it folks thx")
                    return
                print(post['id'], post['title'])
                ids.append(post['id'])
            current_after_id = posts[-1]['name']  # query only works with 'name' field and not 'id'
    fetch_loop()
    return ids


def get_post_ids_from_index() -> List[str]:
    print("fetching complete_post_index and extracting ids")
    page = "https://old.reddit.com/r/castaneda/wiki/index/complete_post_index"
    post_index = requests.get(page, headers=HEADERS)
    html = post_index.text
    ids = []
    num_ids = 0
    num_errors = 0

    def extract_id_from_link(link) -> Optional[str]:
        s = link.split("/")
        post_id = s[-4] if s[-4] != "comments" else s[-3] 
        if len(post_id) not in (6, 7):
            print(f"wrong format. whatsup? post_id: {post_id}, link: {link}")
            return None
        return post_id

    for line in html.split('\n'):
        line = line.strip()
        if not line.startswith(f"https://www.reddit.com/r/{SUBREDDIT_NAME}"):
            continue
        if " " in line:
            line = line.split(" ")[0]
        if not line.endswith("/"):  # some end with a slash and some don't, make it consisent here for the split
            line += "/"
        post_id = extract_id_from_link(line)
        if post_id is None:
            num_errors += 1
            continue
        num_ids += 1
        ids.append(post_id)
    ids = list(set(ids))
    print(f"total: {num_ids}, post_dedup: {len(ids)}, parse errors: {num_errors}")
    return ids


def get_ids_already_in_archive() -> List[str]:
    ids = []
    for post in sorted(ARCHIVE_DIR.rglob('*.json')):
        post_id = post.stem.split("_")[1]
        ids.append(post_id)
    return ids


def get_json(url, retry_n=0) -> Optional[Dict]:
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        print('error code', r.status_code)
        if r.status_code == 429:
            print("too many requests. sleepy sleepy for a minute")
            time.sleep(60)
            retry_n += 1
            if retry_n < MAX_RETRIES:
                return requestJSON(url, retry_n=retry_n)
            else:
                print("stop retrying.. no bueno mr alfredo")
                return None
        return None
    return r.json()


def get_post(post_id) -> Optional[Dict]:
    post_url = f"https://reddit.com/r/{SUBREDDIT_NAME}/comments/{post_id}.json"
    post_json = get_json(post_url)
    if not post_json:
        print("woopie doopie something is spooky could not get json for post:", post_id)
        return None
    return post_json
 

def datestamp_from_timestamp(timestamp: int) -> str:
    d = datetime.fromtimestamp(timestamp)
    return d.strftime("%Y%m%d%H%M%S")


# json files are saved as "<DATESTAMP>_<POST_ID>_post-title-without-spaces-and-possibly-truncated.json"
def save_post(post) -> str:
    post_data = post[0]['data']['children'][0]['data']
    datestamp = datestamp_from_timestamp(post_data['created'])
    sluggy_title = slugify(post_data['title'])[:70]  # limit to 70 chars, OS cannot handle longer ones sometimes
    filename = f"{datestamp}_{post_data['id']}_{sluggy_title}.json"
    with open(f"{str(ARCHIVE_DIR)}/{filename}", "w+") as f:
        f.write(json.dumps(post))
    return filename


def main():
    args = sys.argv[1:]
    fetch_mode = args[0] if len(args) > 0 else "index"
    ids_in_archive = get_ids_already_in_archive()
    ids_to_archive = set()
    print(f"archive contains {len(ids_in_archive)} posts")
    print(f"using fetch mode: {fetch_mode}")
    if fetch_mode == "index":
        ids_from_post_index = get_post_ids_from_index()
        ids_to_archive = set(ids_from_post_index) - set(ids_in_archive)
    elif fetch_mode == "scrape":
        stop_at_id = ids_in_archive[-1] if len(ids_in_archive) else None
        ids_from_scraping = get_post_ids_from_scraping(stop_at_id=stop_at_id)
        ids_to_archive = set(ids_from_scraping) - set(ids_in_archive)
    else:
        print("err: fetch mode must be either 'index' or 'scrape'")
        sys.exit()

    print(f"fetching {len(ids_to_archive)} posts not yet archived")
    for num, post_id in enumerate(ids_to_archive):
        post = get_post(post_id)
        filename = save_post(post)
        print(f"[{num + 1}/{len(ids_to_archive)}] {filename}")


if __name__ == '__main__':
    main()

