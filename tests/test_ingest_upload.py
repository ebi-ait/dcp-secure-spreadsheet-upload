import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

from werkzeug.datastructures import FileStorage

from common.utils import get_project_uuid_from_tags
from ingest_upload.upload_to_ingest_lambda import upload_to_ingest


class TestIngestUploadLambdaFunction(unittest.TestCase):

    def setUp(self):
        self.mock_s3 = MagicMock()
        self.bucket_name = 'test-bucket'
        self.folder_uuid = 'example_folder'

    def test_get_project_uuid_from_tags_valid(self):
        # given
        self.mock_s3.get_object_tagging.return_value = {
            'TagSet': [{'Key': 'name', 'Value': '613b5c9f-760f-45cd-8583-4c3fb0dff804'}]
        }

        # when
        project_uuid = get_project_uuid_from_tags(self.mock_s3, self.bucket_name, self.folder_uuid)

        # then
        self.assertEqual(project_uuid, '613b5c9f-760f-45cd-8583-4c3fb0dff804')

    def test_get_project_uuid_from_tags_missing(self):
        # given
        self.mock_s3.get_object_tagging.return_value = {
            'TagSet': []
        }

        # when/ then
        with self.assertRaises(ValueError) as context:
            get_project_uuid_from_tags(self.mock_s3, self.bucket_name, self.folder_uuid)

        self.assertEqual(str(context.exception), 'Project UUID not found in tags.')

    @patch('ingest_upload.upload_to_ingest_lambda.requests.post')
    @patch('ingest_upload.upload_to_ingest_lambda.boto3.resource')
    def test_upload_to_ingest(self, mock_boto_resource, mock_post):
        # given
        mock_s3 = MagicMock()
        mock_boto_resource.return_value = mock_s3
        mock_s3_obj = MagicMock()
        mock_s3.Object.return_value = mock_s3_obj

        mock_stream = MagicMock()
        mock_file = FileStorage(
            stream=mock_stream,
            filename='test-spreadsheet.xlsx',
            content_type='application/octet-stream'
        )

        mock_s3_obj.get.return_value = {
            'Body': mock_stream,
            'ContentType': 'application/octet-stream',
            'ContentLength': 1234
        }

        # Configure the mock response
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.content = b'{"details": {"submission_id": "test-submission-id"}}'
        mock_post.return_value = mock_response

        # when
        result = upload_to_ingest(
            object_key='test-key',
            spreadsheet_name='test-spreadsheet.xlsx',
            token='fake-token',
            environment='staging',
            project_uuid='test-project-uuid'
        )

        # then
        self.assertEqual(result, {"submission_id": "test-submission-id"})
        mock_post.assert_called_once()
        called_args, called_kwargs = mock_post.call_args
        self.assertEqual(called_args[0], 'https://ingest.staging.archive.data.humancellatlas.org/api_upload')
        self.assertEqual(called_kwargs['data'], {'params': '{"projectUuid": "test-project-uuid"}'})
        self.assertEqual(called_kwargs['headers'], {'Authorization': 'Bearer fake-token'})
        self.assertIn('file', called_kwargs['files'])
        self.assertIsInstance(called_kwargs['files']['file'], FileStorage)
        self.assertEqual(called_kwargs['files']['file'].filename, 'test-spreadsheet.xlsx')
        self.assertEqual(called_kwargs['files']['file'].content_type, 'application/octet-stream')


