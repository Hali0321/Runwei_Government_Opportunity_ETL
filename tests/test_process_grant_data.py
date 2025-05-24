import unittest
from unittest.mock import patch, MagicMock
import azure.functions as func
import sys
import os

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src/functions/GrantsGovFunctions/ProcessGrantData'))

from __init__ import main

class TestProcessGrantData(unittest.TestCase):
    @patch('ProcessGrantData.CosmosClient')
    @patch('ProcessGrantData.pyodbc.connect')
    def test_process_grant_data_success(self, mock_connect, mock_cosmos):
        # Mock Cosmos DB
        mock_container = MagicMock()
        mock_items = [{"id": "12345", "title": "Test Grant", "description": "Description", "agencyName": "Test Agency", "openDate": "2023-01-01", "closeDate": "2023-12-31", "awardCeiling": 10000}]
        mock_container.query_items.return_value = mock_items
        mock_db = MagicMock()
        mock_db.get_container_client.return_value = mock_container
        mock_client = MagicMock()
        mock_client.get_database_client.return_value = mock_db
        mock_cosmos.return_value = mock_client
        
        # Mock SQL connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create a mock queue message
        msg = func.QueueMessage(body=b'12345')
        
        # Run the function
        main(msg)
        
        # Verify the SQL execution was called
        self.assertTrue(mock_cursor.execute.called)
        self.assertTrue(mock_conn.commit.called)

if __name__ == '__main__':
    unittest.main()
