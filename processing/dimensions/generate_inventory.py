import csv
from datetime import datetime, timedelta
import random

# Configuration
NUM_RECORDS = 1000
OUTPUT_FILE = "processing/dimensions/inventory.csv"
BRANCH_IDS = list(range(1, 6))  # 1 to 5
COLOR_IDS = list(range(1, 43)) # 1 to 42

def generate_inventory_data():
    """
    Generates realistic inventory usage data for nail salon colors.
    Each record represents a color being used, decreasing the inventory.
    """
    records = []
    start_date = datetime.now() - timedelta(days=30)
    
    for i in range(NUM_RECORDS):
        record_id = i + 1
        branch_id = random.choice(BRANCH_IDS)
        color_id = random.choice(COLOR_IDS)
        quantity_used = random.randint(1, 5) # e.g., 1-5 units used per transaction
        timestamp = start_date + timedelta(seconds=random.randint(0, 30*24*3600))

        records.append([
            record_id,
            branch_id,
            color_id,
            quantity_used,
            timestamp.isoformat()
        ])

    # Write to CSV
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "record_id", "branch_id", "color_id", 
            "quantity_used", "timestamp"
        ])
        writer.writerows(records)

    print(f"✅ Successfully generated {NUM_RECORDS} inventory records in '{OUTPUT_FILE}'")

if __name__ == "__main__":
    generate_inventory_data() 