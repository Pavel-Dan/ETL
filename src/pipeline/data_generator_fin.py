import sys
import rapidjson as json
import optional_faker as _
import uuid
import random
import numpy as np
import pandas as pd

from dotenv import load_dotenv
from faker import Faker
from datetime import datetime

load_dotenv()
fake = Faker()

inventory =[
    {"sku": "TSHIRT_BASIC", "name": "Uniclothe Basic T-Shirt", "price": 19.99},
    {"sku": "TSHIRT_GRAPHIC", "name": "Uniclothe Graphic T-Shirt", "price": 24.99},
    {"sku": "POLO_CLASSIC", "name": "Uniclothe Classic Polo", "price": 29.99},
    {"sku": "HOODIE_CLASSIC", "name": "Uniclothe Classic Hoodie", "price": 39.99},
    {"sku": "SWEATER_WOOL", "name": "Uniclothe Wool Sweater", "price": 59.99},
    {"sku": "JEANS_SLIM", "name": "Uniclothe Slim Jeans", "price": 49.99},
    {"sku": "JEANS_RELAXED", "name": "Uniclothe Relaxed Jeans", "price": 54.99},
    {"sku": "CHINOS_BEIGE", "name": "Uniclothe Beige Chinos", "price": 44.99},
    {"sku": "SHORTS_SUMMER", "name": "Uniclothe Summer Shorts", "price": 29.99},
    {"sku": "JACKET_LIGHT", "name": "Uniclothe Light Jacket", "price": 79.99},
    {"sku": "JACKET_DENIM", "name": "Uniclothe Denim Jacket", "price": 69.99},
    {"sku": "COAT_WINTER", "name": "Uniclothe Winter Coat", "price": 129.99},
    {"sku": "SNEAKERS_WHITE", "name": "Uniclothe White Sneakers", "price": 59.99},
    {"sku": "SNEAKERS_BLACK", "name": "Uniclothe Black Sneakers", "price": 64.99},
    {"sku": "BOOTS_LEATHER", "name": "Uniclothe Leather Boots", "price": 99.99},
    {"sku": "CAP_LOGO", "name": "Uniclothe Logo Cap", "price": 14.99},
    {"sku": "HAT_WOOL", "name": "Uniclothe Wool Hat", "price": 19.99},
    {"sku": "BAG_TOTE", "name": "Uniclothe Tote Bag", "price": 24.99},
    {"sku": "BACKPACK_URBAN", "name": "Uniclothe Urban Backpack", "price": 49.99},
    {"sku": "BELT_LEATHER", "name": "Uniclothe Leather Belt", "price": 29.99},
    {"sku": "SOCKS_PACK3", "name": "Uniclothe 3-Pack Socks", "price": 12.99},
    {"sku": "SCARF_COTTON", "name": "Uniclothe Cotton Scarf", "price": 19.99},
    {"sku": "GLOVES_WOOL", "name": "Uniclothe Wool Gloves", "price": 24.99}
]


channels = ["e-commerce", "store"]



def print_client_support():
    product = random.choice(inventory)
    channel = random.choice(channels)

        # Acheteur en ligne = adresse générée
    customer_address = {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "postalcode": fake.zipcode()
    }

    client_support = {
        "txid": str(uuid.uuid4()),
        "channel": channel,
        "sku": product["sku"],
        "item": product["name"],
        "price": product["price"],
        "quantity": fake.random_int(min=1, max=5),
        "purchase_time": datetime.utcnow().isoformat(),
        "customer": {
            "name": fake.name(),
            "email": fake.email(),
            "address": customer_address
        }
    }
    channel = random.choice(["Online", "In-Store", "Mobile"])
    if channel == "In-Store":
        store = "8fca494d-22c9-421a-bd22-c1f0f8035a25"
    else:
        store = "f94efd4c-8fbd-42e6-9ec2-f609a8186110"

    order_id = str(uuid.uuid4())
    order_datetime = pd.to_datetime(fake.date_time_between(start_date="-3y", end_date="now"))
    total_amount = np.random.randint(10, 2000)
    payment_status = random.choice(["Paid", "Unpaid", "Refunded"])
    order = {
    "order_id": order_id,
    "customer_id": str(uuid.uuid4()),
    "order_datetime": order_datetime.isoformat(),
    "channel": channel ,
    "store_id": store,
    "order_status": random.choice(["Pending","Completed","Cancelled","Returned"]),
    "total_amount": total_amount,
    "currency": "USD",
    "payment_status": payment_status
    
    }

    product_df = pd.read_csv("./product.csv")

    pid = random.choice(product_df['PRODUCT_ID'])
    qty = np.random.randint(1, 4)
    price = float(product_df.loc[product_df["PRODUCT_ID"] == pid, "PRICE"].values[0])
    discount = round(random.choice([0, 0.05, 0.1, 0.2]), 2)
    line_total = round(qty * price * (1 - discount), 2)
    
    order_item = {
        "order_item_id": str(uuid.uuid4()),
        "order_id": order_id,
        "product_id": pid,
        "qty": qty,
        "unit_price": price,
        "discount_pct": discount,
        "line_total": line_total
    }
    
    payments = {
    "payment_id": str(uuid.uuid4()),
    "order_id": order_id,
    "payment_datetime": (order_datetime + pd.to_timedelta(np.random.randint(0, 5), unit="h")).isoformat(),
    "payment_method": random.choice(["Credit Card", "PayPal", "Bank Transfer", "Gift Card"]),
    "amount": total_amount,
    "payment_status": payment_status

    }

    client_support = {"order" : order,
                      "order_item" : order_item,
                      "payments" : payments}

    d = json.dumps(client_support) + "\n"
    sys.stdout.write(d)


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_client_support()
    print("")
