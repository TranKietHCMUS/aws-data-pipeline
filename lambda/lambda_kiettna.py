import logging
import traceback
import requests
import bs4
import sys
import pandas as pd
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)

today = datetime.date.today()
day = today.day
month = today.month
year = today.year


def crawl_data(url):
    total_stories = []

    num_page = 0

    i1 = requests.get(url, headers={})
    s1 = bs4.BeautifulSoup(i1.content, 'html.parser')
    pag = s1.find("div", {"id": "pagination"})
    list_a = pag.find_all("a")
    for a_tag in list_a:
        text = a_tag.text
        if (text == 'Cuối'):
            num_page = int(a_tag['data-page'])

    for page in range(1, num_page + 1):
        if page >= 2:
            new_url = url + "page/" + str(page) + "/"
        else:
            new_url = url
        info = requests.get(new_url, headers={})
        soup = bs4.BeautifulSoup(info.content, 'html.parser')
        stories = soup.find_all("div", {"class": "home-truyendecu"})

        for story in stories:
            story_link = story.find("a")["href"] if True else "null"
            story_name = story.find("h3").text if True else "null"

            sub_info = requests.get(story_link, headers={})
            sub_soup = bs4.BeautifulSoup(sub_info.content, 'html.parser')
            detail_infos = sub_soup.find_all("div", {"class": "info-chitiet"})

            author = detail_infos[0].find("a").text if True else "null"
            story_type = detail_infos[1].find("a").text if True else "null"
            view = detail_infos[4].find("span", {"class": "text-primary"}).text if True else "null"

            story_dict = {
                "story_name": story_name,
                "story_link": story_link,
                "author": author,
                "story_type": story_type,
                "view": view
            }
            total_stories.append(story_dict)

    return total_stories


def lambda_handler(event, context):
    try:
        logger.info(f"EVENT: {event}")
        logger.info(f"CONTEXT: {context}")

        url = event['url']
        bucket_name = event['bucket_name']
        object_key = event['object_key']

        total_stories = crawl_data(url)

        df = pd.DataFrame.from_dict(total_stories, orient='columns')

        file_name = object_key + f"comic_{day}-{month}-{year}.csv"

        df.to_csv('s3://' + bucket_name + '/' + file_name, index=False, encoding='utf-8')

        return {"status": "SUCCESS"}
    except Exception as ex:
        logger.error(f'FATAL ERROR: {ex} %s')
        logger.error('TRACEBACK:')
        logger.error(traceback.format_exc())
        return {"status": "FAIL", "error": f"{ex}"}
    