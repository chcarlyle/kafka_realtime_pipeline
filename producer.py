import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def generate_synthetic_appointment():
    """Generates synthetic hospital appointment data."""
    departments = ["Cardiology", "Neurology", "Orthopedics", "Pediatrics", "Dermatology", "Oncology", "Radiology"]
    statuses = ["Scheduled", "Completed", "Cancelled", "No-Show"]
    cities = ["New York", "Chicago", "Los Angeles", "Houston", "Phoenix", "Boston", "Seattle"]
    payment_methods = ["Insurance", "Self-Pay", "Medicare", "Medicaid"]
    urgency_levels = ["Low", "Medium", "High", "Critical"]

    department = random.choice(departments)
    status = random.choice(statuses)
    city = random.choice(cities)
    payment_method = random.choice(payment_methods)
    urgency = random.choice(urgency_levels)

    base_cost = fake.pyfloat(min_value=80, max_value=1200, right_digits=2)
    copay = fake.pyfloat(min_value=10, max_value=80, right_digits=2) if payment_method != "Self-Pay" else 0
    total_cost = base_cost + copay

    return {
        "appointment_id": str(uuid.uuid4())[:8],
        "status": status,
        "department": department,
        "urgency": urgency,
        "cost": round(total_cost, 2),
        "timestamp": datetime.now().isoformat(),
        "city": city,
        "payment_method": payment_method,
        "copay": round(copay, 2),
    }


def run_producer():
    """Kafka producer that sends synthetic orders to the 'orders' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            apt = generate_synthetic_appointment()
            print(f"[Producer] Sending appointment #{count}: {apt}")

            future = producer.send("appointments", value=apt)
            record_metadata = future.get(timeout=10)
            print(f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}")

            producer.flush()
            count += 1

            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_producer()
