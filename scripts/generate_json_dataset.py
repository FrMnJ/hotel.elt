import datetime
import pandas as pd
import random
import numpy as np
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)
MONTHS = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
    "INVALID_MONTH": 1
}

def generate_dirty_data(n=100):
    data = []
    
    for _ in range(n):
        dirty = random.random() < 0.1  # 10% dirty rows
        arrivalDayMonth = random.randint(1, 31)
        arrivalYear = random.choice([2022, 2023, 2024])
        arrivalMonth =  random.choice(["January", "February", "INVALID_MONTH"] if dirty else 
                                               ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
        if arrivalMonth == "February" and arrivalDayMonth > 28:
            arrivalDayMonth = 28
        if arrivalMonth in ["April", "June", "September", "November"] and arrivalDayMonth > 30:
            arrivalDayMonth = 30
        row = {
            "adr": round(random.uniform(50, 500), 2) if not dirty else np.nan, 
            "adults": random.randint(1, 5),
            "agent": random.randint(1, 500),
            "arrival_date_day_of_month": arrivalDayMonth,
            "arrival_date_month": arrivalMonth,
            "arrival_date_week_number": random.randint(1, 52),
            "arrival_date_year": arrivalYear, 
            "assigned_room_type": random.choice(["A", "B", "C", "X"] if not dirty else ["XX"]),
            "babies": random.randint(0, 2),
            "booking_changes": random.randint(0, 5),
            "children": random.randint(0, 3),
            "company": random.randint(1, 100) if not dirty else np.nan,
            "country": random.choice(["USA", "GBR", "FRA", "ESP", "INVALID_COUNTRY"] if dirty else ["USA", "GBR", "FRA"]),
            "customer_type": random.choice(["Transient", "Contract", "Group", "Transient-party"]),
            "days_in_waiting_list": random.randint(0, 50),
            "deposit_type": random.choice(["No Deposit", "Non Refund", "Refundable"]) if not dirty else "UNKNOWN",
            "distribution_channel": random.choice(["TA", "TO", "Direct"]),
            "hotel": random.choice(["City Hotel", "Resort Hotel"]),
            "is_canceled": random.choice([0, 1]),
            "is_repeated_guest": random.choice([0, 1]),
            "lead_time": random.randint(0, 365),
            "market_segment": random.choice(["Online", "Offline", "Corporate"]),
            "meal": random.choice(["BB", "HB", "FB", "SC"]),
            "previous_bookings_not_canceled": random.randint(0, 10),
            "previous_cancellations": random.randint(0, 5),
            "required_card_parking_spaces": random.randint(0, 2),
            "reservation_status": random.choice(["Canceled", "Check-Out", "No-Show"]),
            "reservation_status_date": fake.date_between(datetime.date(int(arrivalYear), MONTHS[arrivalMonth], arrivalDayMonth), datetime.date(2024, 12, 31)),
            "reserved_room_type": random.choice(["A", "B", "C"]),
            "stays_in_weekend_nights": random.randint(0, 3),
            "stays_in_week_nights": random.randint(0, 5),
            "total_of_special_requests": random.randint(0, 3)
        }
        
        data.append(row)

    return pd.DataFrame(data)

if __name__ == "__main__":
    df = generate_dirty_data(25000)
    print(df.head())

    df.to_json("./datasets/hotel_bookings_data.json", orient="records", indent=4)
