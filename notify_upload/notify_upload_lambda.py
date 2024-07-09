import json
import boto3
from common import utils


def lambda_handler(event, context):
    s3 = boto3.client('s3')

    try:
        bucket_name, object_key = utils.get_s3_event_details(event)
        folder_uuid = object_key.split('/')[0]
        spreadsheet_name = object_key.split('/')[-1]

        project_uuid = utils.get_project_uuid_from_tags(s3, bucket_name, folder_uuid)
        file_size, file_last_modified = utils.get_object_metadata(s3, bucket_name, object_key, project_uuid)

        notification_message = utils.prepare_notification(bucket_name, folder_uuid, spreadsheet_name, project_uuid,
                                                          file_size, file_last_modified)
        sns = boto3.client('sns')
        utils.send_notification(sns, notification_message, project_uuid, context)

        return {
            'statusCode': 200,
            'body': json.dumps(f'Notification sent successfully for project UUID: {project_uuid} ')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error for project UUID: {project_uuid}: {str(e)}')
        }
