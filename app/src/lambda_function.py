import logging
from modules.sns import get_sns_client, publish_message_to_sns


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def lambda_handler(event):
    try:
        topic_arn = 'arn:aws:sns:us-east-1:123456789012:MyTopic'
        message = 'Hello, SNS!'
        region = "sa-east-1"

        sns_client = get_sns_client(region)
        publish_message_to_sns(sns_client, topic_arn, message)
    
    except Exception as e:
        logger.error(f"Lambda Proccess Error: {str(e)}")
        return {'statusCode': 500}

    return {'statusCode': 200}

lambda_handler('teste')