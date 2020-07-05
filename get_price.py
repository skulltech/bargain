import requests
from lxml import html

requests_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
}


class InvalidURLException(Exception):
    pass


def get_amazon_price(tree):
    try:
        price = tree.xpath('//span[@id="priceblock_dealprice"]/text()')
        if not price:
            price = tree.xpath('//span[@id="priceblock_ourprice"]/text()')
        price = int(float(price[0][2:].replace(',', '')))
    except Exception as e:
        raise InvalidURLException('url is not a valid amazon.in product page')
    return price


def get_flipkart_price(tree):
    try:
        price = tree.xpath('//div[@class="_1vC4OE _3qQ9m1"]/text()')[0]
        price = int(price[1:].replace(',', ''))
    except Exception as e:
        raise InvalidURLException('url is not a valid flipkart.com product page')
    return price


def get_price(url):
    page = requests.get(url, headers=requests_headers)
    tree = html.fromstring(page.content)
    canonical_link = tree.xpath('//link[@rel="canonical"]/@href')[0]
    if 'www.amazon.in' in canonical_link:
        price = get_amazon_price(tree)
    elif 'www.flipkart.com' in canonical_link:
        price = get_flipkart_price(tree)
    else:
        raise InvalidURLException('url is not a valid amazon.in or flipkart.com product page')
    return canonical_link, price


if __name__ == '__main__':
    url = input('[*] Enter URL: ')
    price = get_price(url)
    print(price)
