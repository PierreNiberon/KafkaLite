import os

class KafkaLite:
    def __init__(self, topics_dir=None):
        # Set topics directory based on an environment variable or default to 'topics'
        self.topics_dir = topics_dir or os.getenv('KAFKALITE_TOPIC_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), '../topics'))
    
    def create_topic(self, topic_name):
        """Creates a new topic log file and its metadata file if they don't already exist."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        os.makedirs(topic_dir, exist_ok=True)

        topic_path = os.path.join(topic_dir, f"{topic_name}.log")
        metadata_path = os.path.join(topic_dir, f"{topic_name}.meta")

        if not os.path.exists(topic_path):
            open(topic_path, 'a').close()
            with open(metadata_path, 'w') as meta_file:
                meta_file.write("last_message_id=0\n")
            print(f"Topic '{topic_name}' created.")
        else:
            print(f"Topic '{topic_name}' already exists.")

    def produce(self, topic_name, message):
        """Appends a message to the topic's log file and updates the metadata."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        topic_path = os.path.join(topic_dir, f"{topic_name}.log")
        metadata_path = os.path.join(topic_dir, f"{topic_name}.meta")

        if not os.path.exists(topic_path):
            print(f"Topic '{topic_name}' does not exist.")
            return

        # Read and increment last message ID
        with open(metadata_path, 'r+') as meta_file:
            metadata = dict(line.strip().split('=') for line in meta_file.readlines())
            last_message_id = int(metadata.get('last_message_id', 0)) + 1
            metadata['last_message_id'] = str(last_message_id)
            meta_file.seek(0)
            meta_file.write(f"last_message_id={last_message_id}\n")
        
        # Append message to the log file
        with open(topic_path, 'a') as log_file:
            log_file.write(f"{last_message_id}: {message}\n")
        print(f"Message '{message}' written to topic '{topic_name}'.")

    def delete_topic(self, topic_name):
        """Deletes the topic's directory and its files."""
        topic_dir = os.path.join(self.topics_dir, topic_name)

        if os.path.exists(topic_dir):
            for file_name in os.listdir(topic_dir):
                file_path = os.path.join(topic_dir, file_name)
                os.remove(file_path)
            os.rmdir(topic_dir)
            print(f"Topic '{topic_name}' deleted.")
        else:
            print(f"Topic '{topic_name}' does not exist.")
