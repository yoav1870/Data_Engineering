import csv
import random
from datetime import datetime, timedelta

# Configuration
NUM_RECORDS = 200
OUTPUT_FILE = "processing/dimensions/instagram.csv"
COLOR_IDS = list(range(1, 43))  # 1 to 42

def generate_instagram_data():
    """
    Generates realistic Instagram post engagement data.
    Each record represents a post featuring a specific nail color.
    """
    records = []
    today = datetime.now()
    
    for i in range(NUM_RECORDS):
        post_id = i + 1
        color_id = random.choice(COLOR_IDS)
        likes = random.randint(50, 5000)
        comments = random.randint(5, 200)
        
        # Posts from the last year
        post_date = today - timedelta(days=random.randint(1, 365))
        # Ingestion date is always today, as this is a batch job
        ingestion_date = today

        records.append([
            post_id,
            color_id,
            likes,
            comments,
            post_date.strftime('%Y-%m-%d'),
            ingestion_date.strftime('%Y-%m-%d')
        ])

    # Write to CSV
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "post_id", "color_id", "likes", 
            "comments", "post_date", "ingestion_date"
        ])
        writer.writerows(records)

    print(f"✅ Successfully generated {NUM_RECORDS} Instagram records in '{OUTPUT_FILE}'")

if __name__ == "__main__":
    generate_instagram_data() 