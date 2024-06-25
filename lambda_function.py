import base64
import json
import boto3
from botocore.exceptions import ClientError
import requests
from werkzeug.datastructures import FileStorage
import time
import jwt
import os

from token_manager import TokenManager


# Token management and upload code
class ServiceCredential:
    def __init__(self, value):
        self.value = value

    @classmethod
    def from_secret(cls, secret_name):
        client = boto3.client('secretsmanager')
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            service_credentials = json.loads(secret)
            print("Successfully loaded JSON content from AWS Secrets Manager.")
        except ClientError as e:
            print(f'Error retrieving secret: {e}')
            raise e
        return cls(service_credentials)

    @classmethod
    def from_file(cls, file_path):
        with open(file_path, 'r') as fh:
            service_credentials = json.load(fh)
            print("Loaded JSON content:", json.dumps(service_credentials, indent=2))
        return cls(service_credentials)


class S2STokenClient:
    def __init__(self, credential, audience):
        self._credentials = credential
        self._audience = audience

    def retrieve_token(self):
        if not self._audience:
            raise ValueError('The audience must be set.')
        return self.get_service_jwt(service_credentials=self._credentials.value, audience=self._audience)

    @staticmethod
    def get_service_jwt(service_credentials, audience):
        iat = time.time()
        exp = iat + 3600
        payload = {
            'iss': service_credentials["client_email"],
            'sub': service_credentials["client_email"],
            'aud': audience,
            'iat': iat,
            'exp': exp,
            'https://auth.data.humancellatlas.org/email': service_credentials["client_email"],
            'https://auth.data.humancellatlas.org/group': 'hca',
            'scope': ["openid", "email", "offline_access"]
        }
        additional_headers = {'kid': service_credentials["private_key_id"]}
        signed_jwt = jwt.encode(payload, service_credentials["private_key"], headers=additional_headers,
                                algorithm='RS256')
        return signed_jwt


class IngestAuthAgent:
    def __init__(self, secret_name, audience):
        # credential = ServiceCredential.from_file(credentials_file)
        credential = ServiceCredential.from_secret(secret_name)
        self.s2s_token_client = S2STokenClient(credential, audience)
        self.token_manager = TokenManager(token_client=self.s2s_token_client)

    def _get_auth_token(self):
        """Generate self-issued JWT token

        :return string auth_token: OAuth0 JWT token
        """
        try:
            auth_token = self.token_manager.get_token()
            print("Token generated successfully.")
            return auth_token
        except Exception as e:
            print(f"Failed to generate token: {e}")
            return None

    def make_auth_header(self):
        """Make the authorization headers to communicate with endpoints which implement Auth0 authentication API.

        :return dict headers: A header with necessary token information to talk to Auth0 authentication required
        endpoints.
        """
        headers = {
            "Authorization": f"Bearer {self._get_auth_token()}"
        }
        return headers


def base_url(environment):
    environment = f'{environment}.' if environment != 'prod' else ''
    return f"https://ingest.{environment}archive.data.humancellatlas.org"


def upload_to_ingest(object_key, spreadsheet_name, token, environment, project_uuid, update_project):
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(bucket_name='hca-util-upload-area', key=object_key)

    headers = {'Authorization': f'Bearer {token}'}
    url = f'{base_url(environment)}/api_upload'
    print("API URL: " + url)
    params = json.dumps({
        'projectUuid': project_uuid,
        'isUpdate': False,
        'updateProject': update_project
    })

    try:
        full_object = s3_obj.get()
        spreadsheet = FileStorage(
            stream=full_object['Body'],
            filename=spreadsheet_name,
            name=spreadsheet_name,
            content_type=full_object['ContentType'],
            content_length=full_object['ContentLength']
        )

        response = requests.post(
            url=url,
            data={'params': params},
            files={'file': spreadsheet},
            headers=headers
        )
        response.raise_for_status()
        return response.json()
    except ClientError as e:
        print(f'Error uploading to ingest: {e}')
        raise


def delete_existing_submission(project_uuid, auth_headers, environment):
    search_url = f'https://api.ingest.staging.archive.data.humancellatlas.org/projects/search/findByUuid?uuid={project_uuid}'
    response = requests.get(search_url, headers=auth_headers)
    response.raise_for_status()
    print(f'Retrieved existing submission: {response.json()}')
    submissions = response.json()['_embedded']['submissionEnvelopes']
    print(f'Found {len(submissions)} submissions: {submissions}')

    for submission in submissions:
        submission_url = submission['_links']['self']['href']
        delete_url = f'{submission_url}?force=true'
        # delete_response = requests.delete(delete_url, headers=auth_headers)
        # delete_response.raise_for_status()
        print(f'Deleted existing submission: {submission_url}')


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
        print(f'Error retrieving tags for folder: {e}')
        raise
    except ValueError as e:
        print(e)
        raise


def get_object_metadata(s3, bucket_name, object_key):
    try:
        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        file_size = response['ContentLength']
        file_last_modified = response['LastModified']
        print(f'File {object_key} uploaded to bucket {bucket_name}')
        print(f'File size: {file_size} bytes')
        print(f'Last modified: {file_last_modified}')
        return file_size, file_last_modified
    except ClientError as e:
        print(f'Error retrieving object metadata: {e}')
        raise


def load_config():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    environment = config.get('environment', 'staging')
    secret_name = config[environment]['secret_name']
    topic_arn = config['topic_arn']
    print(f'Running in environment: {environment}')
    return environment, secret_name, topic_arn


def get_audience(environment):
    if environment == 'prod':
        return 'https://data.humancellatlas.org/'
    else:
        return 'https://dev.data.humancellatlas.org/'


def prepare_notification(bucket_name, folder_uuid, project_uuid, object_key, file_size, file_last_modified, result,
                         environment):
    notification_message = {
        'bucket': bucket_name,
        'folder': folder_uuid,
        'project_uuid': project_uuid,
        'file_name': object_key.split('/')[-1],
        'file_size': file_size,
        'last_modified': file_last_modified.strftime("%Y-%m-%d %H:%M:%S"),
        'upload_result': result,
        'environment': environment,
        'message': f"A new spreadsheet named '{object_key.split('/')[-1]}' has been uploaded to the folder '{folder_uuid}' in the bucket '{bucket_name}' in environment '{environment}'."
    }
    print('Notification:', json.dumps(notification_message))
    return notification_message


def send_notification(sns, topic_arn, notification_message, project_uuid):
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(notification_message, indent=4),
        Subject=f"Spreadsheet Upload Notification: {project_uuid}",
    )
    print('Notification sent successfully')


def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # Get the bucket and object key from the event
    bucket_name, object_key = get_s3_event_details(event)

    # Extract the folder (UUID) from the object key
    folder_uuid = object_key.split('/')[0]
    spreadsheet_name = object_key.split('/')[-1]

    # Get the tags for the folder to retrieve the project UUID
    try:
        project_uuid = get_project_uuid_from_tags(s3, bucket_name, folder_uuid)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }

    # Get object metadata
    try:
        file_size, file_last_modified = get_object_metadata(s3, bucket_name, object_key)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }

    # Load configuration file
    try:
        environment, secret_name, topic_arn = load_config()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error loading configuration')
        }

    audience = get_audience(environment)

    # Generate the authentication header
    try:
        ingest_auth_agent = IngestAuthAgent(secret_name=secret_name, audience=audience)
        auth_headers = ingest_auth_agent.make_auth_header()
        token = auth_headers['Authorization'].split(' ')[1]
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error generating authentication token')
        }

    update_project = False

    # Upload the spreadsheet
    try:
        result = upload_to_ingest(object_key, spreadsheet_name, token, environment, project_uuid, update_project)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error uploading file')
        }

    # Prepare and send notification
    notification_message = prepare_notification(bucket_name, folder_uuid, project_uuid, object_key, file_size, file_last_modified, result, environment)

    sns = boto3.client('sns')
    try:
        send_notification(sns, topic_arn, notification_message, project_uuid)
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error sending notification')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Notification sent successfully')
    }