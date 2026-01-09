import os
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, List


def env(name: str, default: str) -> str:
    """Get environment variable with default fallback."""
    val = os.getenv(name)
    return val if val is not None and val != "" else default


# Kafka Configuration
KAFKA_BROKER = env("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", "player-data")

# Data generation configuration
PLAYERS_PER_BATCH = env("PLAYERS_PER_BATCH", "10")
BATCH_INTERVAL_SECONDS = env("BATCH_INTERVAL_SECONDS", "5")

# Sample player data - extracted from the schema
PLAYER_NAMES = [
    "Mohamed Salah", "Achraf Hakimi", "Victor Osimhen", "Youssef En-Nesyri",
    "Brahim Diaz", "Riyad Mahrez", "Sebastien Haller", "Edouard Mendy",
    "Kalidou Koulibaly", "Sadio Mane", "Yves Bissouma", "Amadou Haidara",
    "Nene Dorgeles", "Alex Iwobi", "Ademola Lookman", "Calvin Bassey",
    "Samuel Chukwueze", "Bryan Mbeumo", "Andre Onana", "Serge Aurier",
    "Wilfried Ndidi", "Kylian Mbappé", "Vinícius Júnior", "Rodrygo",
    "Casemiro", "Bruno Fernandes", "Neymar Jr", "Vinicius Jr",
]

COUNTRIES = [
    "Egypt", "Morocco", "Nigeria", "Senegal", "Mali", "Ivory Coast",
    "Cameroon", "Ghana", "Tunisia", "Algeria", "Kenya", "South Africa",
]

POSITIONS = ["Forward", "Midfielder", "Defender", "Goalkeeper"]


def generate_player_record() -> Dict:
    """Generate a random player record matching the data.csv schema."""
    position = random.choice(POSITIONS)
    
    # Adjust stats based on position
    if position == "Goalkeeper":
        goals = 0
        assists = 0
        saves = random.randint(3, 12)
        clean_sheets = random.randint(0, 3)
        tackles = 0
        interceptions = 0
        minutes_played = random.randint(180, 270)
    elif position == "Defender":
        goals = random.randint(0, 2)
        assists = random.randint(0, 3)
        saves = 0
        clean_sheets = 0
        tackles = random.randint(6, 15)
        interceptions = random.randint(4, 12)
        minutes_played = random.randint(200, 270)
    elif position == "Midfielder":
        goals = random.randint(0, 2)
        assists = random.randint(0, 4)
        saves = 0
        clean_sheets = 0
        tackles = random.randint(4, 10)
        interceptions = random.randint(2, 8)
        minutes_played = random.randint(150, 270)
    else:  # Forward
        goals = random.randint(1, 4)
        assists = random.randint(0, 3)
        saves = 0
        clean_sheets = 0
        tackles = random.randint(1, 5)
        interceptions = random.randint(0, 3)
        minutes_played = random.randint(150, 270)
    
    return {
        "Player_Name": random.choice(PLAYER_NAMES),
        "Country": random.choice(COUNTRIES),
        "Age": random.randint(20, 36),
        "Position": position,
        "Goals": goals,
        "Assists": assists,
        "Minutes_Played": minutes_played,
        "Matches_Played": random.randint(1, 3),
        "Pass_Accuracy": round(random.uniform(70.0, 95.0), 1),
        "Distance_Run_km": round(random.uniform(20.0, 38.0), 1),
        "Sprint_Speed_kmh": round(random.uniform(28.0, 36.0), 1),
        "Tackles": tackles,
        "Interceptions": interceptions,
        "Saves": saves,
        "Clean_Sheets": clean_sheets,
        "Yellow_Cards": random.randint(0, 3),
        "Red_Cards": random.randint(0, 1),
        "Performance_Rating": round(random.uniform(7.0, 9.5), 1),
        "Timestamp": datetime.utcnow().isoformat(),
    }


def create_kafka_producer() -> KafkaProducer:
    """Create and return a Kafka producer."""
    print(f"Connecting to Kafka broker: {KAFKA_BROKER}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        retry_backoff_ms=100,
    )
    print(f"Successfully connected to Kafka broker: {KAFKA_BROKER}")
    return producer


def send_batch_to_kafka(producer: KafkaProducer, batch_size: int, topic: str) -> None:
    """Generate and send a batch of player records to Kafka."""
    batch = [generate_player_record() for _ in range(batch_size)]
    
    for record in batch:
        try:
            producer.send(topic, value=record)
        except Exception as e:
            print(f"Error sending record to Kafka: {e}")
    
    producer.flush()
    print(f"[{datetime.now()}] Sent {len(batch)} records to topic '{topic}'")


def run_generator(
    num_batches: int = None,
    batch_size: int = int(PLAYERS_PER_BATCH),
    interval: int = int(BATCH_INTERVAL_SECONDS),
) -> None:
    """
    Run the data generator.
    
    Args:
        num_batches: Number of batches to generate. If None, runs indefinitely.
        batch_size: Number of records per batch.
        interval: Seconds between batches.
    """
    producer = create_kafka_producer()
    batch_count = 0
    
    try:
        print(f"Starting Kafka data generator")
        print(f"  Topic: {KAFKA_TOPIC}")
        print(f"  Batch Size: {batch_size}")
        print(f"  Interval: {interval} seconds")
        print(f"  Target Batches: {num_batches if num_batches else 'infinite'}")
        
        while True:
            send_batch_to_kafka(producer, batch_size, KAFKA_TOPIC)
            batch_count += 1
            
            if num_batches and batch_count >= num_batches:
                print(f"Reached target of {num_batches} batches. Stopping.")
                break
            
            time.sleep(interval)
    
    except KeyboardInterrupt:
        print(f"\nGenerator interrupted. Sent {batch_count} batches.")
    except Exception as e:
        print(f"Error in generator: {e}")
        raise
    finally:
        producer.close()
        print("Kafka producer closed.")


def main():
    """Entry point for the Kafka data generator."""
    import sys
    
    # Parse command line arguments
    num_batches = None
    batch_size = int(PLAYERS_PER_BATCH)
    interval = int(BATCH_INTERVAL_SECONDS)
    
    if len(sys.argv) > 1:
        try:
            num_batches = int(sys.argv[1])
        except ValueError:
            print(f"Invalid batch count: {sys.argv[1]}")
            sys.exit(1)
    
    run_generator(num_batches=num_batches, batch_size=batch_size, interval=interval)


if __name__ == "__main__":
    main()
