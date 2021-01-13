# -*- coding: utf-8 -*-
"""
Software        PyCharm
FileName        RabbitMQ_Receive_Pika.py
Create Time     2020-12-31
System User     Administrator
Author          Smile_yang
"""

import datetime
import pika
import time
import gevent
from multiprocessing import Process, Pool

user = 'guest'
password = 'guest'
host = '127.0.0.1'
port = 5672
virtual_host = 'li_tong_qu'
heartbeat = 60


# 从队列中获取消息相对来说稍显复杂。需要为队列定义一个回调（callback）函数。
# 当我们获取到消息的时候，Pika库就会调用此回调函数。这个回调函数会将接收到的消息内容输出到屏幕上
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

    ch.basic_ack(delivery_tag=method.delivery_tag)


# 消费者 -- 处理队列中的数据
def rabbit_mq_receive():
    # 建立一个到RabbitMQ服务器的连接
    credentials = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host, port, virtual_host, credentials, heartbeat=heartbeat
        )
    )
    # connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # 我们需要确认队列是存在的。
    # 可以多次使用queue_declare命令来创建同一个队列，但是只有一个队列会被真正的创建
    channel.queue_declare(queue='real_data', durable=True)

    # 再同一时刻，不要发送超过1条消息给一个工作者（worker），直到它已经处理了上一条消息并且作出了响应
    channel.basic_qos(prefetch_count=1)

    # 告诉RabbitMQ这个回调函数将会从名为"real_data"的队列中接收消息
    # 消息响应默认是开启的。之前的例子中我们可以使用no_ack = True标识把它关闭
    channel.basic_consume('real_data', callback)

    # 运行一个用来等待消息数据并且在需要的时候运行回调函数的无限循环
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    rabbit_mq_receive()
