import os
import struct
from concurrent.futures import ThreadPoolExecutor

class KafkaLite:
    def __init__(self, topics_dir=None):
        self.topics_dir = topics_dir or os.getenv('KAFKALITE_TOPIC_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), '../topics'))
        self.executor = ThreadPoolExecutor(max_workers=1)

    def write_log_async(self, topic_path_log, last_message_id, message):
        """Background task to write the log asynchronously."""
        with open(topic_path_log, 'a') as log_file:
            log_file.write(f"{last_message_id}: {message}\n")
    
    def create_topic(self, topic_name):
        """Creates a new topic log file and its metadata file if they don't already exist."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        os.makedirs(topic_dir, exist_ok=True)

        topic_path_log = os.path.join(topic_dir, f"{topic_name}.log")
        topic_path_bin = os.path.join(topic_dir, f"{topic_name}.bin")
        metadata_path = os.path.join(topic_dir, f"{topic_name}.meta")

        if not os.path.exists(topic_path_bin):
            open(topic_path_bin, 'ab').close()
            open(topic_path_log, 'a').close()
            with open(metadata_path, 'w') as meta_file:
                meta_file.write("last_message_id=0\n")
            print(f"Topic '{topic_name}' created.")
        else:
            print(f"Topic '{topic_name}' already exists.")

    def produce(self, topic_name, message):
        """Appends a message to the topic's log file and updates the metadata."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        topic_path_log = os.path.join(topic_dir, f"{topic_name}.log")
        topic_path_bin = os.path.join(topic_dir, f"{topic_name}.bin")
        metadata_path = os.path.join(topic_dir, f"{topic_name}.meta")

        if not os.path.exists(topic_path_bin):
            print(f"Topic '{topic_name}' does not exist.")
            return

        # Read and increment last message ID
        with open(metadata_path, 'r+') as meta_file:
            metadata = dict(line.strip().split('=') for line in meta_file.readlines())
            last_message_id = int(metadata.get('last_message_id', 0)) + 1
            metadata['last_message_id'] = str(last_message_id)
            meta_file.seek(0)
            meta_file.write(f"last_message_id={last_message_id}\n")
        
        # Append message to the bin file
        with open(topic_path_bin, 'ab') as bin_file:
            encoded_message = message.encode('utf-8')
            bin_file.write(struct.pack('I', last_message_id))  # Write message ID as an integer
            bin_file.write(struct.pack('I', len(encoded_message)))  # Write the length of the message
            bin_file.write(encoded_message)  # Write the message itself

        # Write the log file asynchronously
        self.executor.submit(self.write_log_async, topic_path_log, last_message_id, message)

    def consume(self, topic_name, from_id=0):
        """Reads messages from the topic log file starting from a specific ID."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        topic_path = os.path.join(topic_dir, f"{topic_name}.log")

        if not os.path.exists(topic_path):
            print(f"Topic '{topic_name}' does not exist.")
            return
        
        # Read and print messages from the log file
        with open(topic_path, 'r') as log_file:
            for line in log_file:
                message_id, message = line.strip().split(': ', 1)
                if int(message_id) >= from_id:
                    print(f"{message_id}: {message}")

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

    def get_last_message_id(self, topic_name):
        """Retrieves the last message ID from the topic's metadata file."""
        metadata_path = os.path.join(self.topics_dir, topic_name, f"{topic_name}.meta")
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"Metadata file for topic '{topic_name}' not found.")
        
        with open(metadata_path, 'r') as meta_file:
            metadata = dict(line.strip().split('=') for line in meta_file.readlines())
            last_message_id = int(metadata.get('last_message_id', 0))
        return last_message_id
