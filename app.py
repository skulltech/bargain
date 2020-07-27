import json
import logging
import os
import string
from decimal import Decimal
from hashlib import md5

import boto3
from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin

from get_details import get_details, InvalidURLException

logging.getLogger().setLevel(logging.INFO)
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

BARGAINS_TABLE = os.environ['BARGAINS_TABLE']
SUBSCRIPTIONS_TABLE = os.environ['SUBSCRIPTIONS_TABLE']
PRODUCTS_TABLE = os.environ['PRODUCTS_TABLE']
QUEUE = os.environ['QUEUE']
IS_OFFLINE = os.environ.get('IS_OFFLINE')

if IS_OFFLINE:
    db = boto3.resource(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
    queue = None
else:
    db = boto3.resource('dynamodb')
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=QUEUE)
bargains_table = db.Table(BARGAINS_TABLE)
subscriptions_table = db.Table(SUBSCRIPTIONS_TABLE)
products_table = db.Table(PRODUCTS_TABLE)
sns = boto3.client('sns')

NOTIFICATION_TEMPLATE = '''
Price change notification for {title}
{price_old} ⟶ {price_new}
Check the product @ {url}
'''


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o)
        return super(DecimalEncoder, self).default(o)


app.json_encoder = DecimalEncoder


def create_topic(email):
    translator = str.maketrans(string.punctuation, '-' * len(string.punctuation))
    topic_name = email.translate(translator)
    topic_arn = sns.create_topic(Name=topic_name)['TopicArn']
    return topic_arn


@app.route('/api/bargains')
@cross_origin()
def get_bargains():
    email = request.args.get('email')
    if not email:
        return jsonify({'error': 'Please provide email'}), 400

    response = bargains_table.query(
        IndexName='emailIndex',
        KeyConditionExpression='email = :email',
        ExpressionAttributeValues={':email': email}
    )
    return jsonify(response['Items'])


@app.route('/api/bargains/<string:bargain_id>')
@cross_origin()
def get_bargain(bargain_id):
    response = bargains_table.get_item(Key={'bargainId': bargain_id})
    item = response.get('Item')
    if not item:
        return jsonify({'error': 'Bargain does not exist'}), 404
    return jsonify(response['Item'])


@app.route('/api/bargains', methods=['POST'])
@cross_origin()
def create_bargain():
    product_url = request.json.get('productUrl')
    email = request.json.get('email')
    if not product_url:
        return jsonify({'error': 'Please provide productUrl'}), 400
    if not email:
        return jsonify({'error': 'Please provide email'}), 400
    try:
        details = get_details(product_url)
    except InvalidURLException:
        return jsonify({'error': 'Invalid productUrl'}), 400
    else:
        product_url = details['url']
        price = details['price']
        title = details['title']
    bargain_id = md5(f'{email}::{product_url}'.encode('UTF-8')).hexdigest()

    sub = subscriptions_table.get_item(Key={'email': email}).get('Item')
    if not sub or not sub['subscribed']:
        response = sns.subscribe(TopicArn=create_topic(email), Protocol='email', Endpoint=email,
                                 ReturnSubscriptionArn=True)
        sub_arn = response['SubscriptionArn']
        subscriptions_table.update_item(
            Key={'email': email},
            UpdateExpression='set subscribed=:s, subArn=:sa',
            ExpressionAttributeValues={':s': True, ':sa': sub_arn}
        )

    products_table.update_item(
        Key={'productUrl': product_url},
        UpdateExpression='set productTitle=:title, latestPrice=:price',
        ExpressionAttributeValues={':title': title, ':price': price}
    )

    try:
        bargains_table.put_item(
            ConditionExpression='bargainId <> :bargainId',
            Item={
                'bargainId': bargain_id,
                'productUrl': product_url,
                'email': email,
                'productTitle': title
            },
            ExpressionAttributeValues={
                ':bargainId': bargain_id
            })
    except db.meta.client.exceptions.ConditionalCheckFailedException as e:
        return jsonify({'error': 'Bargain already added'}), 400

    return jsonify({
        'productUrl': product_url,
        'email': email,
        'productTitle': title,
        'bargainId': bargain_id
    })


@app.route('/api/bargains/<string:bargain_id>', methods=['DELETE'])
@cross_origin()
def delete_bargain(bargain_id):
    bargains_table.delete_item(Key={'bargainId': bargain_id})
    return jsonify({'success': 'Bargain deleted'})


@app.route('/api/subscriptions/<string:email>')
@cross_origin()
def get_subscription(email):
    response = subscriptions_table.get_item(Key={'email': email})
    item = response.get('Item')
    if not item:
        return jsonify({'error': 'Subscription does not exist'}), 404
    return jsonify(response['Item'])


@app.route('/api/subscriptions/<string:email>', methods=['PUT'])
@cross_origin()
def update_subscription(email):
    subscribed = request.json.get('subscribed') or False
    sub = subscriptions_table.get_item(Key={'email': email}).get('Item')
    if not sub:
        response = sns.subscribe(TopicArn=create_topic(email), Protocol='email', Endpoint=email,
                                 ReturnSubscriptionArn=True)
        sub_arn = response['SubscriptionArn']
    else:
        sub_arn = sub['subArn']

    subscriptions_table.update_item(
        Key={'email': email},
        UpdateExpression='set subscribed=:s, subArn=:sa',
        ExpressionAttributeValues={':s': subscribed, ':sa': sub_arn}
    )

    return jsonify({
        'email': email,
        'subscribed': subscribed,
        'subArn': sub_arn
    })


@app.route('/api/products')
@cross_origin()
def get_product():
    product_url = request.args.get('productUrl')
    if not product_url:
        return jsonify({'error': 'Please provide productUrl'}), 400
    response = products_table.get_item(Key={'productUrl': product_url})
    item = response.get('Item')
    if not item:
        return jsonify({'error': 'Product does not exist'}), 404
    return jsonify(response['Item'])


def handle_task(event, context):
    body = event['Records'][0]['body']
    product = json.loads(body)
    logging.info(f'Handling {product["productTitle"]}')

    response = bargains_table.query(
        IndexName='productUrlIndex',
        KeyConditionExpression='productUrl = :p',
        ExpressionAttributeValues={':p': product['productUrl']}
    )
    bargains = response['Items']
    emails = [b['email'] for b in bargains if
              subscriptions_table.get_item(Key={'email': b['email']}).get('Item')['subscribed']]

    if bargains:
        latest_price = get_details(product['productUrl'])['price']
        logging.info(f'Latest price is {latest_price}')

        if latest_price != product.get('latestPrice', None):
            for email in emails:
                logging.info(f'Sending notification to {email}')
                message = NOTIFICATION_TEMPLATE.format(title=product['productTitle'],
                                                       price_old=product['latestPrice'],
                                                       price_new=latest_price,
                                                       url=product['productUrl'])
                sns.publish(Message=message, TopicArn=create_topic(email))
            products_table.update_item(
                Key={'productUrl': product['productUrl']},
                UpdateExpression='set latestPrice=:price',
                ExpressionAttributeValues={':price': latest_price}
            )
            message = 'Price changed; successfully updated price and notified users'
        else:
            message = 'Price has not changed'
    else:
        message = 'No user to notify'

    body = {
        'message': message,
        'input': event
    }
    response = {
        'statusCode': 200,
        'body': json.dumps(body)
    }
    return response


def enqueue_tasks(event, context):
    products = products_table.scan()['Items']
    for product in products:
        queue.send_message(MessageBody=json.dumps(product, cls=DecimalEncoder))

    body = {
        'message': f'Successfully queued {len(products)} tasks',
        'input': event
    }
    response = {
        'statusCode': 200,
        'body': json.dumps(body)
    }
    return response
