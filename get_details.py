import json
import random

import requests
from lxml import html

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0'
]


class InvalidURLException(Exception):
    pass


def get_amazon_details(tree):
    try:
        title = tree.xpath('//span[@id="productTitle"]/text()')[0].strip()
        availability = tree.xpath('//div[@id="availability"]/span/text()')[0]
        if 'Currently unavailable' in availability:
            return {
                'price': None,
                'title': title
            }
        price = tree.xpath('//span[@id="priceblock_dealprice"]/text()')
        if not price:
            price = tree.xpath('//span[@id="priceblock_ourprice"]/text()')
        price = price[0].translate(str.maketrans('', '', ',₹')).strip()
        price = int(float(price))
    except Exception:
        raise InvalidURLException('url is not a valid amazon.in product page')
    return {
        'price': price,
        'title': title
    }


def get_flipkart_details(tree):
    try:
        price = tree.xpath('//div[@class="_1vC4OE _3qQ9m1"]/text()')[0]
        price = int(price[1:].replace(',', ''))
        title = tree.xpath('//span[@class="_35KyD6"]/text()')[0].strip()
    except Exception:
        raise InvalidURLException('url is not a valid flipkart.com product page')
    return {
        'price': price,
        'title': title
    }


def get_details(url):
    if 'amazon.in' not in url and 'flipkart.in' not in url:
        raise InvalidURLException('url is not a valid amazon.in or flipkart.com product page')
    try:
        page = requests.get(url, headers={'User-Agent': random.choice(user_agents)})
    except Exception:
        raise InvalidURLException('url is not accessible')
    try:
        tree = html.fromstring(page.content)
        canonical_link = tree.xpath('//link[@rel="canonical"]/@href')[0]
    except IndexError:
        raise InvalidURLException('url is not a valid amazon.in or flipkart.com product page')
    if 'www.amazon.in' in canonical_link:
        details = get_amazon_details(tree)
    elif 'www.flipkart.com' in canonical_link:
        details = get_flipkart_details(tree)
    else:
        raise InvalidURLException('url is not a valid amazon.in or flipkart.com product page')
    return {
        'price': details['price'],
        'url': canonical_link,
        'title': details['title']
    }


if __name__ == '__main__':
    url = input('[*] Enter URL: ')
    details = get_details(url)
    print(json.dumps(details, indent=2))
