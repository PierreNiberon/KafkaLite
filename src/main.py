import argparse
from kafkalite import KafkaLite

def main():
    parser = argparse.ArgumentParser(description="KafkaLite CLI")
    parser.add_argument('action', choices=['create_topic', 'produce', 'consume', 'consume_from_id', 'delete_topic'], help="Action to perform")
    parser.add_argument('topic', help="Name of the topic")
    parser.add_argument('-m', '--message', help="Message to produce (required for 'produce')", default=None)
    parser.add_argument('--from_id', type=int, help="Starting message ID for consuming (optional for 'consume from id')", default=1)

    args = parser.parse_args()

    # Instantiate KafkaLite with the topic directory
    kafkalite = KafkaLite()

    # Handle actions
    if args.action == 'create_topic':
        kafkalite.create_topic(args.topic)
    elif args.action == 'produce':
        if not args.message:
            parser.error("The --message argument is required for 'produce'.")
        kafkalite.produce(args.topic, args.message)
    elif args.action == 'consume':
        kafkalite.consume(args.topic)
    elif args.action == 'consume_from_id':
        kafkalite.consume_from_id(args.topic, args.from_id)
    elif args.action == 'delete_topic':
        kafkalite.delete_topic(args.topic)

if __name__ == '__main__':
    main()
