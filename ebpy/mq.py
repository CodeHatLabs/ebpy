from importlib import import_module
import json
from uuid import uuid4

import boto3

from ebpy import conf


class NotQueueReceiver(Exception):
    pass


def get_worker(worker_key):
    """obtain a worker function (worker_key uses dot path notation)"""
    keyparts = worker_key.split('.')
    module_name = '.'.join(keyparts[0: -1])
    module = import_module(module_name)
    worker_function = getattr(module, keyparts[-1])
    return worker_function


def dispatch_message(msg_dict):
    """load a worker function and call it"""
    worker_function = get_worker(msg_dict['worker_key'])
    worker_function(*msg_dict['args'], **msg_dict['kwargs'])


def _sns_publish(msg_dict):
    if conf.settings.EBSQS_RUN_LOCAL or not conf.settings.EBSQS_TOPIC_ARN:
        return
    if 'cron' in msg_dict:
        subject = '{} ({})'.format(msg_dict['status'], msg_dict['cron'])
    else:
        subject = msg_dict['status']
    sns = boto3.client('sns', conf.settings.EBSQS_REGION)
    sns.publish(
        TopicArn = conf.settings.EBSQS_TOPIC_ARN,
        Subject = subject,
        Message = str(msg_dict)
        )


def receive_message(raw_http_content):
    """receive message handler"""
    if not conf.settings.EBSQS_IS_RECEIVER:
        raise NotQueueReceiver()
    # decode the message
    msg_dict = json.loads(raw_http_content.decode())
    # log the message to sns
    msg_dict['status'] = 'Received'
    _sns_publish(msg_dict)
    # call the worker
    dispatch_message(msg_dict)
    msg_dict['status'] = 'Complete'
    _sns_publish(msg_dict)


class ebsqs_worker(object):
    """decorator class for ebsqs worker functions"""

    def __init__(self, worker_function):
        worker_key = '{}.{}'.format(
            worker_function.__module__,
            worker_function.__name__
            )
        self.worker_key = worker_key
        self.worker_function = worker_function

    def __call__(self, *args, **kwargs):
        self.worker_function(*args, **kwargs)

    def queue(self, *args, delay_seconds=0, **kwargs):
        if conf.settings.EBSQS_RUN_LOCAL or not conf.settings.EBSQS_MQ_URL:
            self.__call__(*args, **kwargs)
        else:
            msg_dict = {
                'msg_id': str(uuid4()),
                'worker_key': self.worker_key,
                'args': args,
                'kwargs': kwargs
                }
            if delay_seconds:
                msg_dict['delay_seconds'] = delay_seconds
            # jsonify the message and convert to bytes
            message_body = json.dumps(msg_dict)
            sqs = boto3.client('sqs', conf.settings.EBSQS_REGION)
            response = sqs.send_message(
                QueueUrl = conf.settings.EBSQS_MQ_URL,
                MessageBody = message_body,
                DelaySeconds = delay_seconds
                )
            # log the message to sns
            msg_dict['status'] = 'Sent'
            _sns_publish(msg_dict)


class ebsqs_cron(object):
    """decorator class for ebsqs cron functions"""

    def __init__(self, cron_function, **kwargs):
        self.__dict__.update(kwargs)
        self.cron_function = cron_function
        self.cron_name = '{}.{}'.format(
            cron_function.__module__,
            cron_function.__name__
            )

    def __call__(self, request):
        if not conf.settings.EBSQS_IS_RECEIVER:
            if hasattr(self, 'http404_exception_class'):
                raise self.http404_exception_class()
            if hasattr(self, 'http404_response'):
                return self.http404_response
            raise NotQueueReceiver()
        sns_dict = {
            'status': 'Launch Cron',
            'cron': self.cron_name,
            'uuid': str(uuid4())
            }
        _sns_publish(sns_dict)
        self.cron_function(request)
        sns_dict['status'] = 'Complete Cron'
        _sns_publish(sns_dict)
        return self.get_response()

    def get_response(self):
        return getattr(self, 'http_response', True)


