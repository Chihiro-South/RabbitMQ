# -*- coding: utf-8 -*-
"""
Software        PyCharm
FileName        RabbitMQ_Send_Pika.py
Create Time     2020-12-31
System User     Administrator
Author          Smile_yang
"""

import json
import datetime
import pika

user = 'guest'
password = 'guest'
host = '127.0.0.1'
port = 5672
virtual_host = 'li_tong_qu'
heartbeat = 60


# 生产者 -- 生成队列
def rabbit_mq_send(data_info):
    # 建立一个到RabbitMQ服务器的连接
    credentials = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host, port, virtual_host, credentials, heartbeat=heartbeat
        )
    )
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # 创建一个名为"real_data"的队列用来将消息投递进去
    channel.queue_declare(queue='real_data', durable=True)

    # 在RabbitMQ中，消息是不能直接发送到队列中的，这个过程需要通过交换机（exchange）来进行
    # 使用由空字符串表示的默认交换机
    # 默认交换机比较特别，它允许我们指定消息究竟需要投递到哪个具体的队列中，
    # 队列名字需要在routing_key参数中指定
    # 为了不让队列消失，需要把队列声明为持久化（durable）
    channel.basic_publish(exchange='',
                          routing_key='real_data',
                          body=data_info,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))

    # 在退出程序之前，我们需要确认网络缓冲已经被刷写、消息已经投递到RabbitMQ。通过安全关闭连接可以做到这一点
    connection.close()


if __name__ == '__main__':
    i = 0
    while True:
        i += 1
        dicts = {"index":i,"name":"seek", "code":"123456", "gender":"男"}
        rabbit_mq_send(json.dumps(dicts))
