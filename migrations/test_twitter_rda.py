import unittest
from unittest.mock import patch
from your_module import fetch_twitter_data, process_twitter_data  # Replace with your actual functions

class TestTwitterRDA(unittest.TestCase):
    @patch('your_module.fetch_twitter_data')
    def test_fetch_twitter_data(self, mock_fetch):
        # Mock Twitter API response
        mock_fetch.return_value = {'id': 123, 'text': 'Hello Twitter!'}
        result = fetch_twitter_data('some_username')
        self.assertEqual(result['id'], 123)
        self.assertIn('Hello', result['text'])

    def test_process_twitter_data(self):
        sample_data = {'id': 123, 'text': 'Hello Twitter!'}
        processed = process_twitter_data(sample_data)
        self.assertEqual(processed['length'], len(sample_data['text']))

if __name__ == '__main__':
    unittest.main()
