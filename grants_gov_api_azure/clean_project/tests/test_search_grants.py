import unittest
from unittest.mock import patch, MagicMock
import json
import azure.functions as func
import sys
import os

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src/functions/GrantsGovFunctions/SearchGrants'))

from __init__ import main

class TestSearchGrants(unittest.TestCase):
    @patch('SearchGrants.requests.get')
    @patch('SearchGrants.BlobServiceClient.from_connection_string')
    def test_search_grants_success(self, mock_blob_service, mock_get):
        # Setup mock responses
        mock_response = MagicMock()
        mock_response.json.return_value = {"opportunityList": [{"id": "12345"}]}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Mock blob storage
        mock_container_client = MagicMock()
        mock_blob_client = MagicMock()
        mock_container_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service_client = MagicMock()
        mock_blob_service_client.create_container.return_value = mock_container_client
        mock_blob_service_client.get_container_client.return_value = mock_container_client
        mock_blob_service.return_value = mock_blob_service_client
        
        # Create a mock timer
        timer = func.TimerRequest(body="", timer_status=func.TimerStatusCode.Past)
        
        # Run the function
        main(timer)
        
        # Verify the API was called
        mock_get.assert_called_once()
        self.assertTrue(mock_blob_client.upload_blob.called)

if __name__ == '__main__':
    unittest.main()
