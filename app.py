import json
import os
import string
from decimal import Decimal

import boto3
from flask import Flask, jsonify, request

from get_price import get_price, InvalidURLException

app = Flask(__name__)
BARGAINS_TABLE = os.environ['BARGAINS_TABLE']
IS_OFFLINE = os.environ.get('IS_OFFLINE')

if IS_OFFLINE:
    db = boto3.resource(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
else:
    db = boto3.resource('dynamodb')
table = db.Table(BARGAINS_TABLE)
sns = boto3.client('sns')


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o)
        return super(DecimalEncoder, self).default(o)


app.json_encoder = DecimalEncoder


@app.route('/api/bargains')
def get_bargain():
    email = request.args.get('email')
    if not email:
        return jsonify({'error': 'Please provide email'}), 400

    response = table.query(
        IndexName='emailIndex',
        KeyConditionExpression='email = :email',
        ExpressionAttributeValues={':email': email}
    )
    return jsonify(response['Items'])


def create_topic(email):
    translator = str.maketrans(string.punctuation, '-' * len(string.punctuation))
    topic_name = email.translate(translator)
    topic_arn = sns.create_topic(Name=topic_name)['TopicArn']
    return topic_arn


@app.route('/api/bargains', methods=['POST'])
def create_bargain():
    product_url = request.json.get('productUrl')
    email = request.json.get('email')
    if not product_url:
        return jsonify({'error': 'Please provide productUrl'}), 400
    if not email:
        return jsonify({'error': 'Please provide email'}), 400
    try:
        product_url, price = get_price(product_url)
    except InvalidURLException:
        return jsonify({'error': 'Invalid productUrl'}), 400

    try:
        response = table.put_item(
            ConditionExpression='productUrl <> :productUrl AND email <> :email',
            Item={
                'productUrl': product_url,
                'latestPrice': price,
                'email': email
            },
            ExpressionAttributeValues={
                ':productUrl': product_url,
                ':email': email
            })
    except db.meta.client.exceptions.ConditionalCheckFailedException as e:
        return jsonify({'error': 'Bargain already added'}), 400

    sns.subscribe(TopicArn=create_topic(email), Protocol='email', Endpoint=email)

    return jsonify({
        'productUrl': product_url,
        'latestPrice': price,
        'email': email
    })


def send_notifications(event, context):
    response = table.scan()
    items = response['Items']
    latest_prices = dict()
    topic_arns = dict()
    price_updated = set()

    for item in items:
        latest_price = latest_prices.get(item['productUrl'], get_price(item['productUrl'])[1])
        topic_arn = topic_arns.get(item['email'], create_topic(item['email']))
        # if latest_price != item['latestPrice']:
        if True:
            sns.publish(Message=f'{item["productUrl"]} now costs {latest_price}', TopicArn=topic_arn)
            if item['productUrl'] not in price_updated:
                table.update_item(
                    Key={
                        'productUrl': item['productUrl'],
                        'email': item['email']
                    },
                    UpdateExpression='set latestPrice=:p',
                    ExpressionAttributeValues={':p': latest_price}
                )
                price_updated.add(item['productUrl'])

    body = {
        'message': 'Successfully updated the prices and accordingly notified the clients',
        'input': event
    }
    response = {
        'statusCode': 200,
        'body': json.dumps(body)
    }
    return response
