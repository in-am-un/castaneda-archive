# castaneda archive
post archive and scraper script for downloading all posts/comments from the castaneda subreddit

#### setup
written using python 3.11
`python -m pip install -r requirements.txt`

#### running
to download based on the [complete_post_index](https://old.reddit.com/r/castaneda/wiki/index/complete_post_index)
`python main.py index`

to download based on scraping the latest posts
`python main.py scrape`

by default will create and save json post data to dir archive/ in cwd.
it will only download posts not found in archive already and can thus be run on an interval to archive new posts.

