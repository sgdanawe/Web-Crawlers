import json
import logging
import sqlite3
import time
import random

import pandas as pd
import requests
from lxml import html
from base_scraper import Scraper


def first_el(x):
    if len(x) != 0:
        return x[0]
    else:
        return None


def get_popular_dishes(html_tree):
    try:
        dishes = []
        for dish_el in html_tree.xpath(
                "//div[@class=' dishPassport__09f24__dapXe border-color--default__09f24__NPAKY background-color--white__09f24__ulvSM']"):
            dish = {}
            dish["image"] = first_el(dish_el.xpath(".//img[@class=' dishImageV2__09f24__VT6Je']//@src"))
            dish["name"] = first_el(dish_el.xpath(".//img[@class=' dishImageV2__09f24__VT6Je']//@alt"))
            dishes.append(dish)
        return dishes
    except:
        return []


def get_reviews(html_tree):
    try:
        reviews = []
        for review_el in html_tree.xpath("//div[@class=' review__09f24__oHr9V border-color--default__09f24__NPAKY']"):
            review = {}
            review["content"] = \
                first_el(review_el.xpath(
                    ".//p[@class='comment__09f24__gu0rG css-qgunke']//span[@class=' raw__09f24__T4Ezm']/text()"))
            review["creator"] = first_el(review_el.xpath(".//a[@class='css-1kb4wkh']//text()"))
            review["images"] = review_el.xpath(
                ".//div[@class=' margin-t3__09f24__riq4X margin-b2__09f24__CEMjT border-color--default__09f24__NPAKY']//img/@src")
            review["review_date"] = first_el(review_el.xpath(".//span[@class=' css-chan6m']//text()"))
            review["stars"] = first_el(review_el.xpath(
                ".//div[@class=' i-stars__09f24__M1AR7 i-stars--regular-4__09f24__qui79 border-color--default__09f24__NPAKY overflow--hidden__09f24___ayzG']/@aria-label"))
            if review['stars'] is not None:
                review["stars"] = float(review['stars'].split(" ")[0])
            reviews.append(review)
        return reviews
    except:
        return []


def get_price(html_tree):
    try:
        return html_tree.xpath("//span[@class=' css-1ir4e44']/text()")[0]
    except:
        return "NA"


def get_hours(html_tree):
    try:
        hours = pd.read_html(html.tostring(html_tree.xpath(
            "//table[@class=' hours-table__09f24__KR8wh table__09f24__J2OBP table--simple__09f24__vy16f']")[0]))[0][
            ["Unnamed: 0", "Unnamed: 1"]]
        hours = hours.dropna(subset=["Unnamed: 0", "Unnamed: 1"])
        return dict(zip(hours["Unnamed: 0"], hours["Unnamed: 1"]))
    except:
        return {}


def get_contact_details(html_tree):
    try:
        data = {}
        desc = html_tree.xpath(
            "//section[@class=' margin-b3__09f24__l9v5d border-color--default__09f24__NPAKY']/div[@class=' css-xp8w2v padding-t2__09f24__Y6duA padding-r2__09f24__ByXi4 padding-b2__09f24__F0z5y padding-l2__09f24__kf_t_ border--top__09f24__exYYb border--right__09f24__X7Tln border--bottom__09f24___mg5X border--left__09f24__DMOkM border-radius--regular__09f24__MLlCO background-color--white__09f24__ulvSM']")[
            0]

        data["website"] = desc.xpath(".//a[@class='css-1um3nx']/text()")[0]

        data["phone"] = desc.xpath(".//p[@class=' css-1p9ibgf']/text()")[0]

        data["address"] = desc.xpath(".//p[@class=' css-qyp8bo']/text()")[0]
        return data
    except Exception as e:
        logging.info(e)
        return {}


def query_scraper(query):
    time.sleep(random.randint(3, 10))
    link = "https://www.yelp.com" + query["link"]
    logging.info(f"Fetching page {link}")
    resp = requests.get(link)
    html_tree = html.fromstring(resp.text)
    logging.info("Fetched page")
    yield {
        **query,
        **get_contact_details(html_tree),
        "open_hours": json.dumps(get_hours(html_tree)),
        "price": get_price(html_tree),
        "reviews": get_reviews(html_tree),
        "popular_dishes": get_popular_dishes(html_tree)
    }


def query_generator():
    cnx = sqlite3.connect('../data/yelp/data.db')
    df = pd.read_sql_query("SELECT * FROM main", cnx)
    for i in df.to_dict(orient='records'):
        yield i


if __name__ == "__main__":
    dummy_data = {
        "img": "",
        "name": "",
        "link": "",
        "stars": "",
        "rev_count": "",
        "tags": "",
        "service": "",
        "pincode": "",
        "phone": "",
        "website": "",
        "address": "",
        "open_hours": "",
        "price": "",
        "reviews": "",
        "popular_dishes": ""
    }
    s = Scraper(
        file_name="../data/yelp/result.db",
        num_workers=1,
        query_generator=query_generator,
        query_scraper=query_scraper,
        dummy_data=dummy_data,
        cmt_interval=1,
        timeout=60
    )
    s.start_scraping()
