import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

from common.utils import get_project_uuid_from_tags, get_s3_event_details, prepare_notification


class TestNotifyUploadLambdaFunction(unittest.TestCase):

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

    def test_get_s3_event_details(self):
        # given
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'test-key'}
                }
            }]
        }

        # when
        bucket_name, object_key = get_s3_event_details(event)

        # then
        self.assertEqual(bucket_name, 'test-bucket')
        self.assertEqual(object_key, 'test-key')

    def test_prepare_notification(self):
        # given/ when
        notification_message = prepare_notification(
            'test-bucket', 'test-folder', 'test-key', 'test-project-uuid',
            1024, datetime.strptime('2023-06-21T12:34:56.000Z', "%Y-%m-%dT%H:%M:%S.%fZ")
        )

        # then
        expected_message = {
            'bucket': 'test-bucket',
            'folder': 'test-folder',
            'project_uuid': 'test-project-uuid',
            'file_name': 'test-key',
            'file_size': 1024,
            'last_modified': '2023-06-21 12:34:56',
            'is_update': False,
            'update_project': False,
            'message': "A new spreadsheet named 'test-key' has been uploaded to the folder 'test-folder' in the "
                       "bucket 'test-bucket'."
        }

        self.assertEqual(notification_message, expected_message)
