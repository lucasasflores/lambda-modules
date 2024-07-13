import boto3
import logging


logger = logging.getLogger(__name__)


def get_sns_client(region: str) -> boto3.client:
    """
    Initializes and returns a boto3 SNS client for a specific AWS region.

    Args:
        region (str): AWS region where the SNS client should be created.

    Returns:
        boto3.client: SNS client object.

    Raises:
        Exception: If AWS credentials or region configuration is missing.
    """
    try:
        logger.info("Initializing SNS client...")
        sns_client = boto3.client('sns', region_name=region)
        logger.info("SNS client initialized.")
        return sns_client
    except Exception as e:
        raise logger.error(f"Failed to initialize SNS client: {e}")
    

def publish_message_to_sns(sns_client: boto3.client, topic_arn: str, message: str) -> dict:
    """
    Publishes a message to a specified Amazon SNS topic and verifies success.

    Args:
        sns_client (boto3.client): The boto3 SNS client object.
        topic_arn (str): The Amazon Resource Name (ARN) of the SNS topic.
        message (str): The message to be published.

    Returns:
        dict: Response dictionary from SNS publish API call.

    Raises:
        Exception: If there was an error publishing the message or if HTTP status is not 200.
    """
    try:
        logger.info(f"Publishing message to SNS topic {topic_arn}...")
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message
        )
        logger.info(f"Message published to SNS topic {topic_arn}. MessageId: {response['MessageId']}")
        http_status_code = response['ResponseMetadata']['HTTPStatusCode']
        if http_status_code == 200:
            logger.info(f"Message published to SNS topic {topic_arn} with HTTP status code 200.")
            return response
        else:
            raise logger.error(f"Failed to publish message to SNS topic {topic_arn}. HTTP status code: {http_status_code}")
    except Exception as e:
        raise logger.error(f"Failed to publish message to SNS topic {topic_arn}: {e}")