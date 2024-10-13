import os
import time

# Base directory to store topic files
current_dir = os.path.dirname(os.path.abspath(__file__))  # src/
root_dir = os.path.abspath(os.path.join(current_dir, ".."))  # KafkaLite/
topics_dir = os.path.join(root_dir, 'topics')



def create_topic(topic_name):
    """Creates a new topic log file and its metadata file if they don't already exist."""
    topic_path = os.path.join(topics_dir, topic_name, f"{topic_name}.log")
    metadata_path = os.path.join(topics_dir, topic_name, f"{topic_name}.meta")

    if not os.path.exists(topic_path):
        # Ensure the topics directory exists
        os.makedirs(os.path.join(topics_dir, topic_name), exist_ok=True)
        # Create an empty log file for the topic
        open(topic_path, 'a').close()
        # Create the metadata file and initialize the last message ID to 0
        with open(metadata_path, 'w') as meta_file:
            meta_file.write("last_message_id=0\n")
        print(f"Topic '{topic_name}' created.")
    else:
        print(f"Topic '{topic_name}' already exists.")

def get_last_message_id(topic_name):
    """Reads the last message ID from the metadata file."""
    metadata_path = os.path.join(topics_dir, topic_name, f"{topic_name}.meta")
    
    if not os.path.exists(metadata_path):
        print(f"Error: Metadata file for topic '{topic_name}' not found.")
        return 0
    
    with open(metadata_path, 'r') as meta_file:
        for line in meta_file:
            if line.startswith("last_message_id="):
                last_id = int(line.split('=')[1].strip())
                return last_id
    
    return 0

def update_last_message_id(topic_name, new_id):
    """Updates the last message ID in the metadata file."""
    metadata_path = os.path.join(topics_dir, topic_name, f"{topic_name}.meta")
    
    with open(metadata_path, 'w') as meta_file:
        meta_file.write(f"last_message_id={new_id}\n")

def produce(topic_name, message):
    """Append a new message to the topic's log file with an incremental ID."""
    topic_path = os.path.join(topics_dir, topic_name, f"{topic_name}.log")
    
    if not os.path.exists(topic_path):
        print(f"Error: Topic '{topic_name}' does not exist. Please create it first.")
        return
    
    # Get the last message ID from the metadata file
    last_id = get_last_message_id(topic_name)
    new_id = last_id + 1

    # Write the message to the log file with an incremental ID and a timestamp
    with open(topic_path, 'a', buffering=1) as log_file:  # Line-buffered
        timestamp = int(time.time())  # UNIX timestamp for message
        log_entry = f"{new_id},{timestamp},{message}\n"
        log_file.write(log_entry)

    # Update the metadata file with the new message ID
    update_last_message_id(topic_name, new_id)
    
    print(f"Produced message to topic '{topic_name}': {new_id},{message}")

def consume(topic_name):
    """Consume the messages in the topic and output to the console."""
    topic_path = os.path.join(topics_dir, topic_name, f"{topic_name}.log")

    if not os.path.exists(topic_path):
        print(f"Error: Topic '{topic_name}' does not exist. Please create it first.")
        return

    with open(topic_path, 'r') as log_file:  
        return print([line.strip() for line in log_file]) 

if __name__ == "__main__":
    import argparse

    # Set up command-line arguments for creating topics and producing messages
    parser = argparse.ArgumentParser(description="KafkaLite: A lightweight message streaming service")
    parser.add_argument('action', choices=['create_topic', 'produce', 'consume'], help="Action to perform")
    parser.add_argument('topic', help="Name of the topic")
    parser.add_argument('-m', '--message', help="Message to produce (required for 'produce')", default=None)

    args = parser.parse_args()

    if args.action == 'create_topic':
        create_topic(args.topic)
    elif args.action == 'produce' and not args.message:
        parser.error("The --message argument is required when action is 'produce'.")
    elif args.action == 'produce':
        if args.message:
            produce(args.topic, args.message)
        else:
            print("Error: Message is required to produce.")
    elif args.action == 'consume':
        consume(args.topic)
