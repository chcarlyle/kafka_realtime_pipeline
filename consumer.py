import json
import psycopg2
from kafka import KafkaConsumer

def run_consumer():
    """Consumes messages from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "appointments",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="appointments-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS appointments (
                appointment_id VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                department VARCHAR(100),
                urgency VARCHAR(50),
                cost NUMERIC(10, 2),
                timestamp TIMESTAMP,
                city VARCHAR(100),
                payment_method VARCHAR(50),
                copay NUMERIC(10, 2)
            );
            """
        )
        print("[Consumer] ✓ Table 'appointments' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        message_count = 0
        for message in consumer:
            try:
                appointment = message.value

                insert_query = """
                    INSERT INTO appointments (appointment_id, status, department, urgency, cost, timestamp, city, payment_method, copay)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (appointment_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        appointment["appointment_id"],
                        appointment["status"],
                        appointment["department"],
                        appointment["urgency"],
                        appointment["cost"],
                        appointment["timestamp"],
                        appointment["city"],
                        appointment["payment_method"],
                        appointment["copay"],
                    ),
                )
                message_count += 1
                print(
                    f"[Consumer] ✓ #{message_count} Inserted appointment {appointment['appointment_id']} | "
                    f"{appointment['department']} | ${appointment['cost']} | {appointment['city']}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_consumer()
