import csv
from datetime import datetime, timedelta
import random

# Configuration
NUM_RECORDS = 1000
OUTPUT_FILE = "inventory.csv"
BRANCH_IDS = list(range(1, 6))  # 1 to 5
COLOR_IDS = list(range(1, 43)) # 1 to 42

def generate_inventory_data():
    """
    Generates realistic inventory usage data for nail salon colors.
    Each record represents a color being used, decreasing the inventory.
    Includes some dirty data for testing cleaning logic.
    """
    records = []
    start_date = datetime.now() - timedelta(days=30)
    
    # Generate clean records
    for i in range(NUM_RECORDS - 50):  # Reserve 50 slots for dirty data
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

    # Add dirty data for testing
    dirty_records = [
        # Records with null values
        [NUM_RECORDS - 49, None, 1, 2, start_date.isoformat()],  # null branch_id
        [NUM_RECORDS - 48, 1, None, 3, start_date.isoformat()],  # null color_id
        [NUM_RECORDS - 47, 2, 5, None, start_date.isoformat()],  # null quantity_used
        [NUM_RECORDS - 46, 3, 10, 4, None],  # null timestamp
        
        # Records with invalid quantities
        [NUM_RECORDS - 45, 1, 15, -2, start_date.isoformat()],  # negative quantity
        [NUM_RECORDS - 44, 2, 20, 0, start_date.isoformat()],   # zero quantity
        [NUM_RECORDS - 43, 3, 25, -5, start_date.isoformat()],  # negative quantity
        
        # Duplicate records (same record_id, branch_id, color_id)
        [NUM_RECORDS - 42, 1, 30, 2, start_date.isoformat()],   # original
        [NUM_RECORDS - 42, 1, 30, 3, (start_date + timedelta(hours=1)).isoformat()],  # duplicate with different quantity
        [NUM_RECORDS - 42, 1, 30, 1, (start_date + timedelta(hours=2)).isoformat()],  # another duplicate
        
        # More duplicates
        [NUM_RECORDS - 41, 4, 35, 2, start_date.isoformat()],   # original
        [NUM_RECORDS - 41, 4, 35, 2, (start_date + timedelta(minutes=30)).isoformat()],  # exact duplicate
    ]
    
    records.extend(dirty_records)

    # Write to CSV
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "record_id", "branch_id", "color_id", 
            "quantity_used", "timestamp"
        ])
        writer.writerows(records)

    print(f"✅ Successfully generated {len(records)} inventory records in '{OUTPUT_FILE}'")
    print(f"📊 Data breakdown:")
    print(f"   - Clean records: {NUM_RECORDS - 50}")
    print(f"   - Dirty records: {len(dirty_records)}")
    print(f"   - Records with nulls: 4")
    print(f"   - Records with invalid quantities: 3")
    print(f"   - Duplicate records: 5")

if __name__ == "__main__":
    generate_inventory_data() 