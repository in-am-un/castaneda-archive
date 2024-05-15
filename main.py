import requests
import sys
import pprint
from datetime import datetime
from pathlib import Path
from slugify import slugify
from typing import List, Dict, Optional
import random
import time
import json
from tqdm import tqdm


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
        current_after_id = stop_at_id or ""
        while posts := get_posts(current_after_id):
            for i, post in enumerate(posts):
                if stop_at_id is not None and post['id'] == stop_at_id:
                    print("reached stop_at_id thats it folks thx")
                    return
                if not post['stickied']:
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
        tqdm.write("woopie doopie something is spooky could not get json for post:", post_id)
        return None
    return post_json
 

def datestamp_from_timestamp(timestamp: int) -> str:
    d = datetime.fromtimestamp(timestamp)
    return d.strftime("%Y%m%d%H%M%S")


def get_archive() -> List[Dict]:
    all_posts = []
    for post in sorted(ARCHIVE_DIR.rglob('*.json')):
        post_dict = {}
        with open(post, 'r') as p:
            post_dict = json.loads(p.read())
            all_posts.append(post_dict)
    return all_posts


def list_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n]


def download_archive_media(archive: List[Dict]):
    import asyncio
    import aiohttp

    # image does not readily block on too many requests so here is fine to use async cocurrency
    async def fetch_media():
        async with aiohttp.ClientSession() as session:
            progress = tqdm(
                desc="download_archive_media",
                total=len(archive),
                position=0
            )
            with progress:
                for pos, posts in enumerate(list_chunks(archive, 100)):
                    async def dl(*args):
                        await download_post_media(*args)
                        progress.update(1)
                    await asyncio.gather(*[dl(session, post) for post in posts])
    asyncio.run(fetch_media())


async def download_post_media(session, post):
    meta = post[0]['data']['children'][0]['data']
    datestamp = datestamp_from_timestamp(meta['created'])
    post_media = meta.get('media_metadata') or {}
    
    def download_comments(comments: List[Dict]):
        for comment in comments:
            comment_media = comment.get('media_metadata')
            if comment_media is not None:
                for media_id in comment_media:
                    post_media[media_id] = {
                        'comment_id': comment['id'],
                        'comment_permalink': comment['permalink'],
                        'comment_url': f"https://reddit.com{comment['permalink']}",
                        **comment_media[media_id]
                    }
            if comment.get('replies'):
                download_comments(c['data'] for c in comment['replies']['data']['children'])

    download_comments([c['data'] for c in post[1]['data']['children']])
    for idx, media_id in enumerate(post_media):
        media = post_media[media_id]
        media_kind = media['e']
        if media_kind in ["Image", "AnimatedImage"]:
            img_kind = media['m']
            if img_kind == "image/jpg":
                ext = "jpg"
            elif img_kind == "image/jpeg":
                ext = "jpeg"
            elif img_kind == "image/png":
                ext = "png"
            elif img_kind == "image/gif":
                ext = "gif"
            if external := media.get("ext"):
                url = external
                if "giphy" in external:
                    #url = url.replace("https://giphy.com/gifs/", "https://giphy.com/embed/")
                    continue
            else:
                url = f"https://i.redd.it/{media_id}.{ext}"
        elif media_kind == "RedditVideo":
            #dash_url = media[media_id]['dashUrl']
            #video_id = dash_url.split('/')[6]
            ext = "mp4"
            size = media['y']
            url = f"https://v.redd.it/{media['id']}/DASH_{size}.mp4"
        else:
            print("not handled:", media_kind, datestamp, meta['id'])
            continue
        if comment_id := media.get('comment_id'):
            media_id = f"{comment_id}_{media_id}"
        permalink = media.get('comment_permalink') or meta["permalink"]
        media_path = f"{str(ARCHIVE_DIR)}/{datestamp}_{meta['id']}_{media_id}.{ext}"
        if Path(media_path).exists():
            continue

        async with session.get(url, headers=HEADERS) as r:
            if r.status != 200:
                #tqdm.write(f"{[r.status]} media download failed: https://old.reddit.com{permalink}")
                tqdm.write(f"{[r.status]} media download failed: {url}")
                tqdm.write(pprint.pformat(media, indent=4))
                continue
            size = int(r.headers.get('content-length', 0)) or None
            progress = tqdm(
                desc=media_path,
                total=size,
                unit='B',
                unit_scale=True,
                leave=False
            )
            with open(media_path, 'wb') as f, progress:
                async for chunk in r.content.iter_chunked(512):
                    f.write(chunk)
                    progress.update(len(chunk))
        

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
        return sys.exit()
    if len(ids_to_archive) == 0:
        print("nothing new to archive. bye.")
        return sys.exit()

    print(f"fetching {len(ids_to_archive)} posts not yet archived")
    progress = tqdm(
        desc="posts",
        total=len(ids_to_archive),
        position=0,
    )
    with progress:
        for num, post_id in enumerate(ids_to_archive):
            post = get_post(post_id)
            filename = save_post(post)
            tqdm.write(filename)
            progress.update(1)


if __name__ == '__main__':
    #main()
    archive = get_archive()
    archive.reverse()
    download_archive_media(archive)


