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
            "ADR": round(random.uniform(50, 500), 2) if not dirty else np.nan, 
            "Adults": random.randint(1, 5),
            "Agent": random.choice(["TA123", "TO456", "AG789", np.nan]) if not dirty else "INVALID_AGENT",
            "ArrivalDateDayOfMonth": arrivalDayMonth,
            "ArrivalDateMonth": arrivalMonth,
            "ArrivalDateYear": arrivalYear, 
            "AssignedRoomType": random.choice(["A", "B", "C", "X"] if not dirty else ["XX"]),
            "Babies": random.randint(0, 2),
            "BookingChanges": random.randint(0, 5),
            "Children": random.randint(0, 3),
            "Company": random.choice(["CompA", "CompB", "CompC", np.nan]),
            "Country": random.choice(["USA", "GBR", "FRA", "ESP", "INVALID_COUNTRY"] if dirty else ["USA", "GBR", "FRA"]),
            "CustomerType": random.choice(["Transient", "Contract", "Group", "Transient-party"]),
            "DaysInWaitingList": random.randint(0, 50),
            "DepositType": random.choice(["No Deposit", "Non Refund", "Refundable"]) if not dirty else "UNKNOWN",
            "DistributionChannel": random.choice(["TA", "TO", "Direct"]),
            "IsCanceled": random.choice([0, 1]),
            "IsRepeatedGuest": random.choice([0, 1]),
            "LeadTime": random.randint(0, 365),
            "MarketSegment": random.choice(["Online", "Offline", "Corporate"]),
            "Meal": random.choice(["BB", "HB", "FB", "SC"]),
            "PreviousBookingsNotCanceled": random.randint(0, 10),
            "PreviousCancellations": random.randint(0, 5),
            "RequiredCardParkingSpaces": random.randint(0, 2),
            "ReservationStatus": random.choice(["Canceled", "Check-Out", "No-Show"]),
            "ReservationStatusDate": fake.date_between(datetime.date(int(arrivalYear), MONTHS[arrivalMonth], arrivalDayMonth), datetime.date(2024, 12, 31)),
            "ReservedRoomType": random.choice(["A", "B", "C"]),
            "StaysInWeekendNights": random.randint(0, 3),
            "StaysInWeekNights": random.randint(0, 5),
            "TotalOfSpecialRequests": random.randint(0, 3)
        }
        
        data.append(row)

    return pd.DataFrame(data)

if __name__ == "__main__":
    df = generate_dirty_data(14000)
    print(df.head())

    df.to_json("./datasets/hotel_bookings_data.json", orient="records", indent=4)
