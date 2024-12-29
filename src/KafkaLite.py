import os
import struct
import datetime
from concurrent.futures import ThreadPoolExecutor

class KafkaLite:

    def __init__(self, topics_dir=None):
        self.topics_dir = topics_dir or os.getenv('KAFKALITE_TOPIC_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), '../topics'))
        self.executor = ThreadPoolExecutor(max_workers=1)

    
    def create_topic(self, topic_name):
        """Creates a new topic log file and its metadata file if they don't already exist."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        os.makedirs(topic_dir, exist_ok=True)

        topic_path_log = os.path.join(topic_dir, f"{topic_name}.log")
        metadata_path = os.path.join(topic_dir, f"{topic_name}.meta")

        if not os.path.exists(topic_path_log):
            open(topic_path_log, 'ab').close()
            with open(metadata_path, 'w') as meta_file:
                os_user = os.getlogin()
                meta_file.write(f"created_by={os_user}\n")
                creation_time = datetime.datetime.now()
                meta_file.write(f"created_at={creation_time}\n")
                meta_file.write("last_message_id=0\n")
            print(f"Topic '{topic_name}' created.")
        else:
            print(f"Topic '{topic_name}' already exists.")

    def produce(self, topic_name, message):
        """Appends a message to the topic's log file and updates the metadata."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        topic_path_log = os.path.join(topic_dir, f"{topic_name}.log")
        metadata_path = os.path.join(topic_dir, f"{topic_name}.meta")

        if not os.path.exists(topic_path_log):
            print(f"Topic '{topic_name}' does not exist.")
            return

        # Read and increment last message ID
        with open(metadata_path, 'r+') as meta_file:
            # Read all metadata into a dictionary
            metadata = dict(line.strip().split('=') for line in meta_file.readlines())

            # Increment the last message ID
            last_message_id = int(metadata.get('last_message_id', 0)) + 1
            metadata['last_message_id'] = str(last_message_id)

            # Write back the entire metadata (preserve other fields)
            meta_file.seek(0)
            for key, value in metadata.items():
                meta_file.write(f"{key}={value}\n")
            meta_file.truncate()  # Remove any leftover content

        
        # Append message to the bin file
        with open(topic_path_log, 'ab') as bin_file:
            encoded_message = message.encode('utf-8')
            bin_file.write(struct.pack('I', last_message_id))  # Write message ID as an integer
            bin_file.write(struct.pack('I', len(encoded_message)))  # Write the length of the message
            bin_file.write(encoded_message)  # Write the message itself


    def consume(self, topic_name):
        """Reads messages from the topic log file starting from the first message."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        topic_path = os.path.join(topic_dir, f"{topic_name}.log ")

        if not os.path.exists(topic_path):
            print(f"Topic '{topic_name}' does not exist.")
            return
        
        # Read and print messages from the log file
        with open(topic_path, 'rb') as bin_file:
            
            # loop on the binary to read every message.
            while True :

                # Check if there is data where the cursors currently stands
                message_id = bin_file.read(4)
                if not message_id:
                    break
                
                # Read the message ID
                message_id = struct.unpack('I', message_id)[0]
                

                # Read and unpack the message length
                message_length = struct.unpack('I', bin_file.read(4))[0]
                
                # Read the actual message
                message = bin_file.read(message_length).decode('utf-8')

                print(f"ID: {message_id}, Message: {message}")

    def consume_from_id(self, topic_name, target_id):
        """Consume messages starting from a specific message ID in the binary log."""
        topic_dir = os.path.join(self.topics_dir, topic_name)
        topic_path_log = os.path.join(topic_dir, f"{topic_name}.log")

        if not os.path.exists(topic_path_log):
            print(f"Topic '{topic_name}' does not exist.")
            return

        with open(topic_path_log, 'rb') as bin_file:
            while True:
                # Read the message header (ID and length)
                header = bin_file.read(8)  # 4 bytes for ID + 4 bytes for length
                if len(header) < 8:
                    break  # End of file reached

                message_id, message_length = struct.unpack('II', header)

                # Check if this is the message we're looking for and displays it
                if message_id == target_id:
                    message = bin_file.read(message_length)
                    print(f"ID: {message_id}, Message: {message.decode('utf-8')}")
                    
                    # Then we start to unpack the messages afterwards
                    while True :

                    # Check if there is data where the cursors currently stands
                        message_id = bin_file.read(4)
                        if not message_id:
                            break
                        
                        # Read the message ID
                        message_id = struct.unpack('I', message_id)[0]
                        

                        # Read and unpack the message length
                        message_length = struct.unpack('I', bin_file.read(4))[0]
                        
                        # Read the actual message
                        message = bin_file.read(message_length).decode('utf-8')

                        print(f"ID: {message_id}, Message: {message}")

                    return

                # Skip the content if the message ID is smaller
                bin_file.seek(message_length, 1)

        print(f"Message with ID {target_id} not found.")


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
