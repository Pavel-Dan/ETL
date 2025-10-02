import sys
import rapidjson as json
import optional_faker as _
import uuid
import random

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

    d = json.dumps(client_support) + "\n"
    sys.stdout.write(d)


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_client_support()
    print("")
