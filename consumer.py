"""Read fake Spotify events from Kafka and print them."""

from confluent_kafka import Consumer

# Settings that tell our consumer where Kafka is and how to behave.
settings = {
    "bootstrap.servers": "localhost:9092",  # where Kafka is running
    "group.id": "python-consumer-1",        # name of our consumer group
    "auto.offset.reset": "earliest",        # start from the oldest events
}

consumer = Consumer(settings)

# Which topic to read from.
consumer.subscribe(["spotify-events"])

print("Waiting for events... press Ctrl+C to stop.")

try:
    while True:
        # Ask Kafka for the next message (wait up to 1 second).
        msg = consumer.poll(1.0)

        if msg is None:
            # Nothing arrived yet; keep waiting.
            continue

        if msg.error():
            # Something went wrong; print it and keep going.
            print("Error:", msg.error())
            continue

        # The message content is bytes; decode it into text.
        event = msg.value().decode("utf-8")
        print(event)

except KeyboardInterrupt:
    # User pressed Ctrl+C; exit the loop cleanly.
    print("Stopped by user.")

finally:
    # Always close the consumer so it saves its position.
    consumer.close()
