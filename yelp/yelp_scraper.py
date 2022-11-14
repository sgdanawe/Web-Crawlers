import json
import logging
import time

import pandas as pd
import requests
from lxml import html
import random
from base_scraper import Scraper

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

services = ["Delivery", "Burgers", "Chinese", "Italian", "Reservations", "Japanese", "Mexican", "Thai", "Contractors",
            "Electricians", "Home Cleaners", "HVAC", "Landscaping", "Locksmiths", "Movers", "Plumbers", "Auto Repair",
            "Auto Detailing", "Body Shops", "Car Wash", "Car Dealers", "Oil Change", "Parking", "Towing",
            "Dry Cleaning", "Phone Repair", "Bars", "Nightlife", "Hair Salons", "Gyms", "Massage", "Shopping"]


def get_zips(city="New York"):
    df = pd.read_csv("../data/yelp/uszips.csv")
    df = df[df["city"] == city]
    return list(df["zip"])


def first_el(x):
    if len(x) != 0:
        return x[0]
    else:
        return None


def query_scraper(query):
    logger.info(f"Scraping {query}")
    time.sleep(random.randint(3, 10))
    resp = requests.get("https://www.yelp.com/search", params=query)
    html_tree = html.fromstring(resp.text)
    cards = []
    for el in html_tree.xpath("//div[@class=' arrange__09f24__LDfbs border-color--default__09f24__NPAKY']"):
        dct = {
            "service": query["find_desc"], "pincode": query["find_loc"],
            'img': first_el(el.xpath(".//img[@class=' css-xlzvdl']/@src")),
            'name': first_el(el.xpath(".//a[@class='css-1m051bw']//text()")),
            'link': first_el(el.xpath(".//a[@class='css-1m051bw']//@href")),
            'rev_count': first_el(el.xpath(""".//span[@class=' css-chan6m']//text()""")),
            'tags': ','.join(el.xpath(".//button[@class='css-9dl18g']//span[@class='css-11bijt4']//text()")),
            'stars': first_el(el.xpath(
                """.//div[@class=' five-stars__09f24__mBKym five-stars--regular__09f24__DgBNj display--inline-block__09f24__fEDiJ border-color--default__09f24__NPAKY']//@aria-label"""))
        }

        if dct["stars"] is not None:
            dct["stars"] = float(dct['stars'].split(" ")[0])

        # if dct["rev_count"] is not None:
        #     dct["rev_count"] = int(dct["rev_count"])

        if dct["img"] is not None:
            cards.append(dct)

    logger.info(f"Scraped query: {query} with len: {len(cards)}")
    logger.info(cards)
    for card in cards:
        yield card


def query_generator():
    random.shuffle(services)
    for zip_code in get_zips():
        for service in services:
            params = {"find_desc": service, "find_loc": zip_code, "start": 0}
            resp = requests.get("https://www.yelp.com/search", params=params)
            html_tree = html.fromstring(resp.text)
            total_string = html_tree.xpath(
                """//div[@class=' border-color--default__09f24__NPAKY text-align--center__09f24__fYBGO']
                /span[@class=' css-chan6m']/text()""")
            try:
                total = int(total_string[0].split(" ")[-1])
            except:
                total = 1
            for i in range(total):
                yield {"find_desc": service, "find_loc": zip_code, "start": i * 10}


if __name__ == "__main__":
    dummy_data = {
        "img": "",
        "name": "",
        "link": "",
        "stars": "",
        "rev_count": "",
        "tags": "",
        "service": "",
        "pincode": ""
    }
    s = Scraper(
        file_name="../data/yelp/data.db",
        num_workers=1,
        query_generator=query_generator,
        query_scraper=query_scraper,
        dummy_data=dummy_data,
        cmt_interval=1,
        timeout=60
    )
    s.start_scraping()
