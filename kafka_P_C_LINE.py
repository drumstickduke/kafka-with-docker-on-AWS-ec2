#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import sys
import traceback
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaException, KafkaError
import os.path
import csv
# import flask related
from flask import Flask, request, abort
# import linebot related
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    LocationSendMessage, ImageSendMessage, StickerSendMessage
)
import time




app = Flask(__name__)

# your linebot message API - Channel access token (from LINE Developer)
line_bot_api = LineBotApi('FqrX2gYIUqk13KQXEAcy+5lM6rQakPXBR80U2HEKHvcXymiKJbj61r+jfz/vD6qHP331Q2vBsvsK6iYP9Hy0ZPK+Lj10aHSNmQEJnHVttlInkwcZFann46S7QhZKaS2GeiT/9DkrEQgYRMRRM/KhvwdB04t89/1O/w1cDnyilFU=')
# your linebot message API - Channel secret
handler = WebhookHandler('8bb5ea1f06dcd76ad7823d3cdc9c0eca')

def error_cb(err):
    sys.stderr.write(f'Error: {err}')


# 轉換 msg_key 或 msg_value 成為 utf-8 的字串
def try_decode_utf8(data):
    return data.decode('utf-8') if data else None


# 指定要從哪個 partition, offset 開始讀資料
def my_assign(consumer_instance, partitions):
    for p in partitions:
        p.offset = 0

    print(f'assign: {partitions}')
    consumer_instance.assign(partitions)


def error_callback(err):
    sys.stderr.write(f'Error: {err}')
    

KAFKA_BROKER_URL = '13.113.192.71:9092'
props = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'error_cb': error_callback
}
producer = Producer(props)

topic_name = 'linebot'
props_C = {
    'bootstrap.servers': '13.113.192.71:9092',  # Kafka 集群在那裡? (置換成要連接的 Kafka 集群)
    'group.id': 'STUDENT_ID',  # ConsumerGroup 的名稱 (置換成你/妳的學員 ID)
    'auto.offset.reset': 'earliest',  # Offset 從最前面開始
    'session.timeout.ms': 6000,  # consumer超過6000ms沒有與 kafka 連線，會被認為掛掉了
    'error_cb': error_cb  # 設定接收 error 訊息的 callback 函數
}
# 產生一個 Kafka 的 Consumer 的實例
consumer = Consumer(props_C)

# 指定想要訂閱訊息的 topic 名稱


# 讓 Consumer 向 Kafka 集群訂閱指定的 topic
consumer.subscribe([topic_name], on_assign=my_assign)


@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)

    # handle webhook body
    try:
        print('receive msg')
        handler.handle(body, signature)
    except InvalidSignatureError:
        print("Invalid signature. Please check your channel access token/channel secret.")
        abort(400)
    return 'OK'



@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    # get user info & message
    user_id = event.source.user_id
    msg = event.message.text
    user_name = line_bot_api.get_profile(user_id).display_name

    timestamp = event.timestamp  # 設定timeStamp
    struct_time = time.localtime(timestamp/1000) # 轉成時間元組
    t = time.strftime("%Y-%m-%d,%H:%M:%S", struct_time)  # 轉成字串


    # print(topic_name)
    producer.produce(topic=topic_name, value=f'{user_name},{user_id},{t},{msg}')
    producer.poll()

    # get msg details
    print('msg from [', user_name, '](', user_id, ')[', t, ']: ', msg)

    try:
        while True:
            # 請求 Kafka 把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取

            if not records:
                continue

            for record in records:
                if not record:
                    continue

                # 檢查是否有錯誤
                if record.error() and record.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(record.error())

                else:
                    # ** 在這裡進行商業邏輯與訊息處理 **

                    # 取出相關的 metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()

                    # 取出 msg_key 與 msg_value
                    msg_key = try_decode_utf8(record.key())
                    msg_value = try_decode_utf8(record.value())

                    # 秀出 metadata 與 msg_key & msg_value 訊息
                    print('{}-{}-{} : ({} , {})'.format(
                        topic, partition, offset, msg_key, msg_value)
                    )

                    msg = msg_value.split(",")
                    # print(msg_value)
                    # print(msg[3])
                    file_exists = os.path.isfile('line_record.csv')
                    with open('line_record.csv', 'a',encoding='utf-8') as csvfile:
                        headers = ['name', 'id', 'date', 'time', 'text']
                        # writer = csv.DictWriter(csvfile, fieldnames=headers)
                        writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                        if not file_exists:
                            writer.writeheader()  # file doesn't exist yet, write a header

                        writer.writerow({'name': msg[0], 'id': msg[1], 'date': msg[2], 'time':msg[3], 'text':msg[4] })
    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')

    except Exception as e:
        sys.stderr.write(str(e))

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=12345)