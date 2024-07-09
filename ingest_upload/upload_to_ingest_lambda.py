import json
import boto3
from botocore.exceptions import ClientError
import requests
from werkzeug.datastructures import FileStorage

from ingest.api.ingestapi import IngestApi
from ingest.utils.s2s_token_client import S2STokenClient, ServiceCredential
from ingest.utils.token_manager import TokenManager
from common import utils


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
    bucket_name = 'hca-util-upload-area'
    folder_uuid = event.get('folder_name', None)
    spreadsheet_name = event.get('file_name', None)
    object_key = f'{folder_uuid}/{spreadsheet_name}' if folder_uuid and spreadsheet_name else None
    project_uuid = None

    try:
        if not folder_uuid or not spreadsheet_name:
            raise ValueError("Both 'folder_name' and 'file_name' must be provided in the event.")

        project_uuid = utils.get_project_uuid_from_tags(s3, bucket_name, folder_uuid)
        file_size, file_last_modified = utils.get_object_metadata(s3, bucket_name, object_key, project_uuid)

        environment, secret_env_value = load_config()
        audience = get_audience(environment)

        # Generate the authentication header
        token = authenticate(secret_env_value, audience, environment, project_uuid)

        # Upload the spreadsheet
        result = upload_to_ingest(object_key, spreadsheet_name, token, environment, project_uuid)
        submission_id = result['submission_id']

        notification_message = utils.prepare_notification(bucket_name, folder_uuid, spreadsheet_name,
                                                          project_uuid, file_size, file_last_modified,
                                                          submission_id=submission_id, result=result,
                                                          environment=environment)
        sns = boto3.client('sns')
        utils.send_notification(sns, notification_message, project_uuid, context, submission_id=submission_id)

        return {
            'statusCode': 200,
            'body': json.dumps(f'Notification sent successfully for project UUID: {project_uuid} '
                               f'and submission ID: {submission_id}.')
        }

    except ClientError as e:
        error_message = (f"ClientError occurred while processing {object_key} in bucket {bucket_name} "
                         f"for project UUID: {project_uuid}: {e}")
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'errorType': 'ClientError',
                'errorMessage': error_message,
                'projectUUID': project_uuid,
                'bucketName': bucket_name,
                'objectKey': object_key
            })
        }

    except RuntimeError as e:
        error_message = str(e)
        print(error_message)
        return {
            'statusCode': 404,
            'body': json.dumps({
                'errorType': 'RuntimeError',
                'errorMessage': error_message,
                'projectUUID': project_uuid,
                'bucketName': bucket_name,
                'objectKey': object_key
            })
        }

    except ValueError as e:
        error_message = str(e)
        print(error_message)
        return {
            'statusCode': 400,
            'body': json.dumps({
                'errorType': 'ValueError',
                'errorMessage': error_message,
                'projectUUID': project_uuid,
                'bucketName': bucket_name,
                'objectKey': object_key
            })
        }

    except Exception as e:
        error_message = f"Error for project UUID: {project_uuid}: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'errorType': 'Exception',
                'errorMessage': error_message,
                'projectUUID': project_uuid,
                'bucketName': bucket_name,
                'objectKey': object_key
            })
        }
