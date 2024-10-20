import unittest
import os
from kafkalite import KafkaLite

class TestKafkaLite(unittest.TestCase):
    
    def setUp(self):
        """Setup test environment (runs before each test)."""
        self.kafka_lite = KafkaLite()  # Instantiate KafkaLite class
        self.test_topic = "test_topic"
    
    def tearDown(self):
        """Cleanup after tests (runs after each test)."""
        # Remove the topic and metadata files after tests
        topic_dir = os.path.join("topics", self.test_topic)
        if os.path.exists(topic_dir):
            for filename in os.listdir(topic_dir):
                file_path = os.path.join(topic_dir, filename)
                os.remove(file_path)
            os.rmdir(topic_dir)

    def test_create_topic(self):
        """Test topic creation."""
        self.kafka_lite.create_topic(self.test_topic)
        self.assertTrue(os.path.exists(f"topics/{self.test_topic}/{self.test_topic}.log"))
        self.assertTrue(os.path.exists(f"topics/{self.test_topic}/{self.test_topic}.bin"))
        self.assertTrue(os.path.exists(f"topics/{self.test_topic}/{self.test_topic}.meta"))

    def test_produce_message(self):
        """Test producing a message to a topic."""
        self.kafka_lite.create_topic(self.test_topic)
        self.kafka_lite.produce(self.test_topic, "Test message")
        log_path = os.path.join("topics", self.test_topic, f"{self.test_topic}.log")
        with open(log_path, 'r') as log_file:
            lines = log_file.readlines()
        self.assertEqual(len(lines), 1)
        self.assertIn("Test message", lines[0])

    def test_message_id_increment(self):
        """Test that message IDs are incrementing correctly."""
        self.kafka_lite.create_topic(self.test_topic)
        self.kafka_lite.produce(self.test_topic, "First message")
        self.kafka_lite.produce(self.test_topic, "Second message")
        last_id = self.kafka_lite.get_last_message_id(self.test_topic)
        self.assertEqual(last_id, 2)

if __name__ == "__main__":
    unittest.main()
