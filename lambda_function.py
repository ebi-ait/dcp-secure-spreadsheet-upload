import json
import boto3
from botocore.exceptions import ClientError
import requests
from werkzeug.datastructures import FileStorage
import os

from ingest.api.ingestapi import IngestApi
from ingest.utils.s2s_token_client import S2STokenClient, ServiceCredential
from ingest.utils.token_manager import TokenManager


def base_url(environment):
    environment = f'{environment}.' if environment != 'prod' else ''
    return f"https://ingest.{environment}archive.data.humancellatlas.org"


def upload_to_ingest(object_key, spreadsheet_name, token, environment, project_uuid, is_update=False,
                     submission_uuid=None):
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(bucket_name='hca-util-upload-area', key=object_key)

    headers = {'Authorization': f'Bearer {token}'}
    url = f'{base_url(environment)}/api_upload'
    print(f"API URL: {url} for project UUID: {project_uuid}.")

    params = {}
    if is_update:
        params['isUpdate'] = is_update

    if project_uuid:
        params['projectUuid'] = project_uuid

    if submission_uuid:
        params['submissionUuid'] = submission_uuid

    data = {
        'params': json.dumps(params)
    }

    full_object = s3_obj.get()
    spreadsheet = FileStorage(
        stream=full_object['Body'],
        filename=spreadsheet_name,
        name=spreadsheet_name,
        content_type=full_object['ContentType'],
        content_length=full_object['ContentLength']
    )

    response = requests.post(url, data=data, files={'file': spreadsheet}, allow_redirects=False, headers=headers)
    if response.status_code != requests.codes.found and response.status_code != requests.codes.created:
        raise RuntimeError(f"POST {url} response was {response.status_code}: {response.content} for project UUID: "
                           f"{project_uuid} and submission ID: "
                           f"{json.loads(response.content)['details']['submission_id']}")
    return json.loads(response.content)['details']


def delete_existing_submission(project_uuid, auth_headers, environment):
    search_url = f'https://api.ingest.staging.archive.data.humancellatlas.org/projects/search/findByUuid?uuid={project_uuid}'
    response = requests.get(search_url, headers=auth_headers)
    response.raise_for_status()
    print(f'Retrieved existing submission: {response.json()} for project UUID: {project_uuid}')
    submissions = response.json()['_embedded']['submissionEnvelopes']
    print(f'Found {len(submissions)} submissions for project UUID: {project_uuid}: {submissions}')

    for submission in submissions:
        submission_url = submission['_links']['self']['href']
        delete_url = f'{submission_url}?force=true'
        print(f'Deleted existing submission: {submission_url} for project UUID: {project_uuid}')


def get_s3_event_details(event):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    print(f'Received event for bucket: {bucket_name}, key: {object_key}')
    return bucket_name, object_key


def get_project_uuid_from_tags(s3, bucket_name, folder_uuid):
    try:
        response = s3.get_object_tagging(Bucket=bucket_name, Key=f'{folder_uuid}/')
        tags = {tag['Key']: tag['Value'] for tag in response['TagSet']}
        project_uuid = tags.get('name')
        print(f'Retrieved Project UUID from tags: {project_uuid}')

        if not project_uuid:
            raise ValueError('Project UUID not found in tags.')
        return project_uuid
    except ClientError as e:
        print(f'Error retrieving tags for folder {folder_uuid}: {e}')
        raise
    except ValueError as e:
        print(e)
        raise


def get_object_metadata(s3, bucket_name, object_key, project_uuid):
    try:
        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        file_size = response['ContentLength']
        file_last_modified = response['LastModified']
        print(f'File {object_key}')
        print(f'File size: {file_size} bytes')
        print(f'Last modified: {file_last_modified}')
        return file_size, file_last_modified
    except ClientError as e:
        print(f'Error retrieving object metadata for {object_key} in project UUID: {project_uuid}: {e}')
        raise


def load_config():
    try:
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)
        environment = config.get('environment', 'staging')
        secret_env_value = config.get('secret_name')
        print(f'Running in environment: {environment}')
        return environment, secret_env_value
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f'Error loading configuration: {e}')
        raise


def get_audience(environment):
    if environment == 'prod':
        return 'https://data.humancellatlas.org/'
    else:
        return 'https://dev.data.humancellatlas.org/'


def prepare_notification(bucket_name, folder_uuid, submission_id, project_uuid, object_key, file_size,
                         file_last_modified, result, environment):
    notification_message = {
        'bucket': bucket_name,
        'folder': folder_uuid,
        'project_uuid': project_uuid,
        'submission_id': submission_id,
        'file_name': object_key.split('/')[-1],
        'file_size': file_size,
        'last_modified': file_last_modified.strftime("%Y-%m-%d %H:%M:%S"),
        'upload_result': result,
        'environment': environment,
        'message': f"A new spreadsheet named '{object_key.split('/')[-1]}' has been uploaded to the folder "
                   f"'{folder_uuid}' in the bucket '{bucket_name}' in environment '{environment}'."
    }
    print(f'Notification: {json.dumps(notification_message)}')
    return notification_message


def send_notification(sns, notification_message, project_uuid, submission_id, context):
    try:
        topic_name = os.environ['TOPIC_NAME']
        account_id = context.invoked_function_arn.split(":")[4]
        if account_id != "Fake":
            print(f"Sending notification to {topic_name} for project UUID: {project_uuid} "
                  f"and submission ID: {submission_id}.")
            topic_arn = f"arn:aws:sns:{os.environ['MY_AWS_REGION']}:{account_id}:{topic_name}"
            sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps(notification_message, indent=4),
                Subject=f"Spreadsheet Upload Notification: {project_uuid}",
            )
        else:
            print(f"Skipping notification as this is a fake account for project UUID: {project_uuid}")
    except ClientError as e:
        print(f'Error sending notification for project UUID: {project_uuid}: {e}')
        raise


def authenticate(secret_env_value, audience, environment, project_uuid):
    try:
        if environment == 'prod':
            ingest_api_url = f"https://api.ingest.archive.data.humancellatlas.org"
        else:
            ingest_api_url = f"https://api.ingest.{environment}.archive.data.humancellatlas.org"

        credential = ServiceCredential.from_env_var(secret_env_value)
        print(f"Successfully loaded JSON credentials from environment variable for project UUID: {project_uuid}.")

        s2s_token_client = S2STokenClient(credential, audience)
        token_manager = TokenManager(s2s_token_client)
        ingest_client_api = IngestApi(url=ingest_api_url, token_manager=token_manager)

        r = ingest_client_api.get(ingest_api_url)
        r.raise_for_status()

        auth_headers = ingest_client_api.get_headers()
        token = auth_headers['Authorization'].split(' ')[1]
        print(f'Token generated successfully for project UUID: {project_uuid}.')

        return token
    except Exception as e:
        print(f'Error generating authentication token for project UUID: {project_uuid}: {e}')
        raise


def lambda_handler(event, context):
    s3 = boto3.client('s3')

    try:
        bucket_name, object_key = get_s3_event_details(event)
        folder_uuid = object_key.split('/')[0]
        spreadsheet_name = object_key.split('/')[-1]

        project_uuid = get_project_uuid_from_tags(s3, bucket_name, folder_uuid)
        file_size, file_last_modified = get_object_metadata(s3, bucket_name, object_key, project_uuid)

        environment, secret_env_value = load_config()
        audience = get_audience(environment)

        # Generate the authentication header
        token = authenticate(secret_env_value, audience, environment, project_uuid)

        # Upload the spreadsheet
        result = upload_to_ingest(object_key, spreadsheet_name, token, environment, project_uuid)
        submission_id = result['submission_id']

        notification_message = prepare_notification(bucket_name, folder_uuid, submission_id, project_uuid, object_key,
                                                    file_size, file_last_modified, result, environment)
        sns = boto3.client('sns')
        send_notification(sns, notification_message, project_uuid, submission_id, context)

        return {
            'statusCode': 200,
            'body': json.dumps(f'Notification sent successfully for project UUID: {project_uuid} '
                               f'and submission ID: {submission_id}.')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error for project UUID: {project_uuid}: {str(e)}')
        }