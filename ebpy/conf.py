import os


class Settings(object):

    EBSQS_IS_RECEIVER = os.environ.get('EBSQS_IS_RECEIVER', False)
    EBSQS_MQ_URL = os.environ.get('EBSQS_MQ_URL', '')
    EBSQS_REGION = os.environ.get('EBSQS_REGION', '')
    EBSQS_RUN_LOCAL = os.environ.get('EBSQS_RUN_LOCAL', False)
    EBSQS_SECRET_BYTES = os.environ.get('EBSQS_SECRET_BYTES', b'')
    EBSQS_TOPIC_ARN = os.environ.get('EBSQS_TOPIC_ARN', '')


settings = Settings()


