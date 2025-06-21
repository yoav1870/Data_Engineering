#!/usr/bin/env python3
"""
Dimension Data Generator for Nail Salon
Creates realistic sample data for all dimension tables
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

class NailSalonDimensionGenerator:
    def __init__(self):
        self.colors_data = []
        self.branches_data = []
        self.employees_data = []
        self.customers_data = []
        self.treatments_data = []
        self.date_dim_data = []
        
    def generate_colors(self):
        """Generate nail polish colors data"""
        print("🎨 Generating COLORS dimension data...")
        
        color_categories = {
            'Red': ['Crimson Red', 'Ruby Red', 'Cherry Red', 'Burgundy', 'Coral Red'],
            'Pink': ['Baby Pink', 'Hot Pink', 'Rose Pink', 'Blush Pink', 'Magenta'],
            'Purple': ['Lavender', 'Deep Purple', 'Violet', 'Plum', 'Mauve'],
            'Blue': ['Navy Blue', 'Sky Blue', 'Turquoise', 'Teal', 'Royal Blue'],
            'Green': ['Emerald Green', 'Mint Green', 'Forest Green', 'Sage', 'Olive'],
            'Yellow': ['Sunshine Yellow', 'Golden Yellow', 'Lemon Yellow', 'Cream', 'Beige'],
            'Orange': ['Peach', 'Coral Orange', 'Tangerine', 'Apricot', 'Terracotta'],
            'Neutral': ['Nude', 'Champagne', 'Silver', 'Gold', 'Black', 'White', 'Gray']
        }
        
        color_id = 1
        for category, colors in color_categories.items():
            for color_name in colors:
                # Generate realistic HEX codes
                if category == 'Red':
                    hex_code = f"#{random.randint(150, 255):02x}{random.randint(0, 100):02x}{random.randint(0, 100):02x}"
                elif category == 'Pink':
                    hex_code = f"#{random.randint(200, 255):02x}{random.randint(100, 200):02x}{random.randint(150, 255):02x}"
                elif category == 'Purple':
                    hex_code = f"#{random.randint(100, 200):02x}{random.randint(0, 100):02x}{random.randint(150, 255):02x}"
                elif category == 'Blue':
                    hex_code = f"#{random.randint(0, 100):02x}{random.randint(100, 200):02x}{random.randint(150, 255):02x}"
                elif category == 'Green':
                    hex_code = f"#{random.randint(0, 150):02x}{random.randint(150, 255):02x}{random.randint(0, 150):02x}"
                elif category == 'Yellow':
                    hex_code = f"#{random.randint(200, 255):02x}{random.randint(200, 255):02x}{random.randint(0, 150):02x}"
                elif category == 'Orange':
                    hex_code = f"#{random.randint(200, 255):02x}{random.randint(100, 200):02x}{random.randint(0, 100):02x}"
                else:  # Neutral
                    gray = random.randint(100, 200)
                    hex_code = f"#{gray:02x}{gray:02x}{gray:02x}"
                
                self.colors_data.append({
                    'color_id': color_id,
                    'name': color_name,
                    'hex_code': hex_code,
                    'category': category,
                    'active': True
                })
                color_id += 1
        
        print(f"✅ Generated {len(self.colors_data)} colors")
        return self.colors_data
    
    def generate_branches(self):
        """Generate salon branches data"""
        print("🏢 Generating BRANCHES dimension data...")
        
        branches = [
            {'name': 'Downtown Salon', 'city': 'New York', 'address': '123 Main St'},
            {'name': 'Uptown Beauty', 'city': 'New York', 'address': '456 Park Ave'},
            {'name': 'Brooklyn Nails', 'city': 'Brooklyn', 'address': '789 Bedford Ave'},
            {'name': 'Queens Elegance', 'city': 'Queens', 'address': '321 Queens Blvd'},
            {'name': 'Manhattan Luxury', 'city': 'New York', 'address': '654 5th Ave'}
        ]
        
        for i, branch in enumerate(branches, 1):
            self.branches_data.append({
                'branch_id': i,
                'name': branch['name'],
                'city': branch['city'],
                'address': branch['address'],
                'active': True,
                'opening_date': datetime(2020, 1, 1) + timedelta(days=random.randint(0, 365))
            })
        
        print(f"✅ Generated {len(self.branches_data)} branches")
        return self.branches_data
    
    def generate_employees(self):
        """Generate salon employees data"""
        print("👩‍💼 Generating EMPLOYEES dimension data...")
        
        first_names = ['Emma', 'Olivia', 'Ava', 'Isabella', 'Sophia', 'Charlotte', 'Mia', 'Amelia', 'Harper', 'Evelyn']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
        roles = ['Senior Nail Technician', 'Nail Technician', 'Junior Technician', 'Apprentice', 'Manager']
        
        for i in range(1, 11):  # 10 employees
            role = random.choice(roles)
            experience_years = random.randint(1, 8)
            
            self.employees_data.append({
                'employee_id': i,
                'first_name': random.choice(first_names),
                'last_name': random.choice(last_names),
                'role': role,
                'experience_years': experience_years,
                'active': True,
                'hire_date': datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))
            })
        
        print(f"✅ Generated {len(self.employees_data)} employees")
        return self.employees_data
    
    def generate_customers(self):
        """Generate customer data (SCD Type 2 ready)"""
        print("👥 Generating CUSTOMERS dimension data...")
        
        first_names = ['Alice', 'Bob', 'Carol', 'David', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack',
                      'Kate', 'Liam', 'Maya', 'Noah', 'Olivia', 'Paul', 'Quinn', 'Ruby', 'Sam', 'Tina']
        last_names = ['Anderson', 'Baker', 'Clark', 'Davis', 'Evans', 'Fisher', 'Garcia', 'Harris', 'Ivanov', 'Johnson']
        
        for i in range(1, 21):  # 20 customers
            # Generate multiple versions for SCD Type 2
            versions = random.randint(1, 3)  # 1-3 versions per customer
            
            for version in range(versions):
                start_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))
                end_date = None if version == versions - 1 else start_date + timedelta(days=random.randint(100, 500))
                
                self.customers_data.append({
                    'customer_id': i,
                    'version_id': version + 1,
                    'first_name': random.choice(first_names),
                    'last_name': random.choice(last_names),
                    'email': f"customer{i}@example.com",
                    'phone': f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                    'address': f"{random.randint(100, 999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm'])} St",
                    'city': random.choice(['New York', 'Brooklyn', 'Queens', 'Bronx']),
                    'start_date': start_date,
                    'end_date': end_date,
                    'is_current': version == versions - 1
                })
        
        print(f"✅ Generated {len(self.customers_data)} customer records (SCD Type 2)")
        return self.customers_data
    
    def generate_treatments(self):
        """Generate nail treatment services data"""
        print("💅 Generating TREATMENTS dimension data...")
        
        treatments = [
            {'name': 'Basic Manicure', 'price': 25.00, 'duration_minutes': 30},
            {'name': 'Gel Manicure', 'price': 35.00, 'duration_minutes': 45},
            {'name': 'Acrylic Full Set', 'price': 45.00, 'duration_minutes': 90},
            {'name': 'Acrylic Fill', 'price': 35.00, 'duration_minutes': 60},
            {'name': 'Pedicure', 'price': 40.00, 'duration_minutes': 60},
            {'name': 'Gel Pedicure', 'price': 50.00, 'duration_minutes': 75},
            {'name': 'Nail Art Design', 'price': 15.00, 'duration_minutes': 20},
            {'name': 'Nail Repair', 'price': 10.00, 'duration_minutes': 15},
            {'name': 'Hand Massage', 'price': 20.00, 'duration_minutes': 20},
            {'name': 'Foot Massage', 'price': 25.00, 'duration_minutes': 25},
            {'name': 'Luxury Manicure', 'price': 50.00, 'duration_minutes': 60},
            {'name': 'Luxury Pedicure', 'price': 65.00, 'duration_minutes': 90},
            {'name': 'French Manicure', 'price': 30.00, 'duration_minutes': 40},
            {'name': 'French Pedicure', 'price': 45.00, 'duration_minutes': 55},
            {'name': 'Nail Extension', 'price': 55.00, 'duration_minutes': 120}
        ]
        
        for i, treatment in enumerate(treatments, 1):
            self.treatments_data.append({
                'treatment_id': i,
                'name': treatment['name'],
                'price': treatment['price'],
                'duration_minutes': treatment['duration_minutes'],
                'active': True
            })
        
        print(f"✅ Generated {len(self.treatments_data)} treatments")
        return self.treatments_data
    
    def generate_date_dimension(self, start_date='2020-01-01', end_date='2025-12-31'):
        """Generate date dimension table"""
        print("📅 Generating DATE_DIM dimension data...")
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_date = start
        while current_date <= end:
            # Determine season
            month = current_date.month
            if month in [12, 1, 2]:
                season = 'Winter'
            elif month in [3, 4, 5]:
                season = 'Spring'
            elif month in [6, 7, 8]:
                season = 'Summer'
            else:
                season = 'Fall'
            
            # Determine if weekend
            is_weekend = current_date.weekday() >= 5
            
            # Determine if holiday (simplified)
            is_holiday = current_date.month == 12 and current_date.day == 25  # Christmas
            
            self.date_dim_data.append({
                'date_id': current_date.strftime('%Y%m%d'),
                'date': current_date.date(),
                'day_of_week': current_date.strftime('%A'),
                'day_of_month': current_date.day,
                'month': current_date.strftime('%B'),
                'month_number': current_date.month,
                'quarter': (current_date.month - 1) // 3 + 1,
                'year': current_date.year,
                'season': season,
                'is_weekend': is_weekend,
                'is_holiday': is_holiday
            })
            
            current_date += timedelta(days=1)
        
        print(f"✅ Generated {len(self.date_dim_data)} date records")
        return self.date_dim_data
    
    def generate_all_dimensions(self):
        """Generate all dimension data"""
        print("🚀 Generating all dimension data...")
        
        self.generate_colors()
        self.generate_branches()
        self.generate_employees()
        self.generate_customers()
        self.generate_treatments()
        self.generate_date_dimension()
        
        print("✅ All dimension data generated successfully!")
        
        return {
            'colors': self.colors_data,
            'branches': self.branches_data,
            'employees': self.employees_data,
            'customers': self.customers_data,
            'treatments': self.treatments_data,
            'date_dim': self.date_dim_data
        }
    
    def save_to_csv(self, output_dir='./'):
        """Save all dimension data to CSV files"""
        print("💾 Saving dimension data to CSV files...")
        
        data_dict = self.generate_all_dimensions()
        
        for table_name, data in data_dict.items():
            df = pd.DataFrame(data)
            filename = f"{table_name}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Saved {len(data)} records to {filename}")
        
        print("✅ All dimension CSV files created successfully!")

if __name__ == "__main__":
    generator = NailSalonDimensionGenerator()
    generator.save_to_csv() 