"""Produce fake Spotify events to Kafka at a steady rate.

One event every RATE_SECONDS, until you press Ctrl+C.
The event fields match EventSim's schema, so the whole pipeline sees
the same shape of data no matter which producer sends it.
"""

import json
import random
import time

from confluent_kafka import Producer

# How often to send one event, in seconds. Change this to go faster or slower.
RATE_SECONDS = 1.0

producer = Producer({"bootstrap.servers": "localhost:9092"})

# Sample values in the same style EventSim uses. We keep these lists short
# for learning; EventSim itself pulls from much bigger datasets.
songs = [
    ("Radiohead", "Karma Police"),
    ("The Killers", "Mr Brightside"),
    ("Daft Punk", "Get Lucky"),
    ("Florence + The Machine", "Dog Days Are Over"),
    ("Arctic Monkeys", "Do I Wanna Know?"),
    ("Billie Eilish", "bad guy"),
    ("Alanis Morissette", "Ironic"),
    ("Metallica", "Outlaw Torn"),
]

first_names = ["Santino", "Jayden", "Marisa", "Chloe", "Liam", "Maia", "Jesus", "Morgan"]
last_names = ["Gonzalez", "Graves", "Miller", "Cuevas", "Weber", "Cruz", "Brady", "Bell"]
locations = [
    "New York-Newark-Jersey City, NY-NJ-PA",
    "Dallas-Fort Worth-Arlington, TX",
    "San Francisco-Oakland-Hayward, CA",
    "Reno, NV",
    "Lincoln, NE",
    "Portland-Vancouver-Hillsboro, OR-WA",
]
user_agents = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.77.4 (KHTML, like Gecko) Version/7.0.5 Safari/537.77.4",
]

counter = 0
print(f"Sending one event every {RATE_SECONDS} second(s). Press Ctrl+C to stop.")

try:
    while True:
        artist, song = random.choice(songs)

        event = {
            "ts": int(time.time() * 1000),
            "userId": str(random.randint(1, 50)),
            "sessionId": random.randint(1, 1000),
            "page": "NextSong",
            "auth": "Logged In",
            "method": "PUT",
            "status": 200,
            "level": random.choice(["free", "paid"]),
            "itemInSession": counter % 20,
            "location": random.choice(locations),
            "userAgent": random.choice(user_agents),
            "lastName": random.choice(last_names),
            "firstName": random.choice(first_names),
            "registration": int((time.time() - random.randint(30, 365) * 86400) * 1000),
            "gender": random.choice(["M", "F"]),
            "artist": artist,
            "song": song,
            "length": round(random.uniform(180, 300), 2),
        }

        # Send the event to Kafka as a JSON text message.
        producer.produce("spotify-events", json.dumps(event).encode("utf-8"))
        producer.flush()  # push it out now, don't wait for a buffer to fill
        counter += 1
        time.sleep(RATE_SECONDS)

except KeyboardInterrupt:
    print(f"Stopped. Sent {counter} events.")
