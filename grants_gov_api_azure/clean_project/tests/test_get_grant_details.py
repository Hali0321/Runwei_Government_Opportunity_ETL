import unittest
from unittest.mock import patch, MagicMock
import azure.functions as func
import sys
import os

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src/functions/GetGrantDetails')) 

from __init__ import main

class TestGetGrantDetails(unittest.TestCase):
    @patch('GetGrantDetails.requests.get')
    @patch('GetGrantDetails.CosmosClient')
    def test_get_grant_details_success(self, mock_cosmos, mock_get):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": "12345", "title": "Test Grant"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Mock Cosmos DB
        mock_container = MagicMock()
        mock_db = MagicMock()
        mock_db.get_container_client.return_value = mock_container
        mock_client = MagicMock()
        mock_client.get_database_client.return_value = mock_db
        mock_cosmos.return_value = mock_client
        
        # Create a mock queue message
        msg = func.QueueMessage(body=b'12345')
        
        # Run the function
        main(msg)
        
        # Verify the API was called with correct opportunity ID
        mock_get.assert_called_with("https://www.grants.gov/grantsws/rest/opportunity/12345")
        self.assertTrue(mock_container.upsert_item.called)

if __name__ == '__main__':
    unittest.main()
