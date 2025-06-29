import csv
import random
from datetime import datetime, timedelta

# Configuration
NUM_RECORDS = 200
OUTPUT_FILE = "processing/dimensions/instagram.csv"
COLOR_IDS = list(range(1, 43))  # 1 to 42

# Helper to generate random date
start_date = datetime.now() - timedelta(days=30)
def random_date():
    return (start_date + timedelta(seconds=random.randint(0, 30*24*3600))).date()

def generate_instagram_data():
    """
    Generates realistic Instagram post engagement data.
    Each record represents a post featuring a specific nail color.
    """
    records = []
    for i in range(NUM_RECORDS - 10):  # Reserve 10 for dirty data
        post_id = i + 1
        color_id = random.choice(COLOR_IDS)
        likes = random.randint(0, 100)
        comments = random.randint(0, 20)
        post_date = random_date()
        ingestion_date = post_date
        records.append([
            post_id, color_id, likes, comments, post_date, ingestion_date
        ])

    # Add dirty data
    dirty_records = [
        # Nulls in key fields
        [NUM_RECORDS - 9, None, 10, 2, random_date(), random_date()],  # null color_id
        [NUM_RECORDS - 8, 2, None, 3, random_date(), random_date()],   # null likes
        [NUM_RECORDS - 7, 3, 5, None, random_date(), random_date()],   # null comments
        [NUM_RECORDS - 6, 4, 10, 2, None, random_date()],              # null post_date
        [NUM_RECORDS - 5, 5, 10, 2, random_date(), None],              # null ingestion_date
        # Negative/zero likes/comments
        [NUM_RECORDS - 4, 6, -5, 2, random_date(), random_date()],     # negative likes
        [NUM_RECORDS - 3, 7, 10, -1, random_date(), random_date()],    # negative comments
        [NUM_RECORDS - 2, 8, 0, 2, random_date(), random_date()],      # zero likes
        [NUM_RECORDS - 1, 9, 10, 0, random_date(), random_date()],     # zero comments
        # Duplicate
        [NUM_RECORDS - 10, 1, 10, 2, start_date.date(), start_date.date()],
        [NUM_RECORDS - 10, 1, 10, 2, start_date.date(), start_date.date()],  # exact duplicate
    ]
    records.extend(dirty_records)

    # Write to CSV
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "post_id", "color_id", "likes", "comments", "post_date", "ingestion_date"
        ])
        writer.writerows(records)
    print(f"✅ Successfully generated {len(records)} Instagram records in '{OUTPUT_FILE}' (including dirty data)")
    print(f"📊 Data breakdown: Clean records: {NUM_RECORDS-10}, Dirty records: {len(dirty_records)}")

if __name__ == "__main__":
    generate_instagram_data() 