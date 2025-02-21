#Steam data into products table in MYSQL

from utils import *
import random, string
import sys
from time import sleep

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Beauty",
    "Toys",
    "Sports & Fitness",
    "Books",
    "Grocery",
    "Pet Supplies",
    "Office Supplies",
]
PRODUCTS = {
    "Electronics": [
        "TV",
        "Laptop",
        "Smartphone",
        "Tablet",
        "Headphones",
        "Speaker",
        "Camera",
        "Printer",
        "Router",
        "Smartwatch",
    ],
    "Clothing": [
        "Shirt",
        "Dress",
        "Pants",
        "Shorts",
        "Skirt",
        "Jacket",
        "Coat",
        "Hat",
        "Shoes",
        "Socks",
        "Underwear",
        "Jewelry",
    ],
    "Home & Garden": [
        "Furniture (Sofa, Bed, Chair, Table)",
        "Appliance (Refrigerator, Dishwasher, Oven, Washing Machine)",
        "Tool (Hammer, Saw, Drill, Screwdriver)",
        "Bedding (Sheets, Pillowcases, Comforter)",
        "Decor (Rug, Lamp, Artwork, Mirror)",
        "Plant (Indoor, Outdoor)",
    ],
    "Beauty": [
        "Makeup (Foundation, Concealer, Mascara, Eyeliner, Lipstick)",
        "Skincare (Moisturizer, Cleanser, Serum, Sunscreen)",
        "Haircare (Shampoo, Conditioner, Styling Products)",
        "Fragrance (Perfume, Cologne)",
        "Bath & Body (Soap, Lotion, Shower Gel)",
    ],
    "Toys": [
        "Doll",
        "Action Figure",
        "Building Set",
        "Board Game",
        "Puzzle",
        "Stuffed Animal",
        "Ride-On Toy",
        "Educational Toy",
        "Art Supplies",
        "Sports Equipment",
    ],
    "Sports & Fitness": [
        "Apparel (Shirt, Pants, Shoes)",
        "Equipment (Basketball, Football, Baseball Bat, Yoga Mat)",
        "Fitness Machine (Treadmill, Elliptical, Bike)",
        "Accessory (Wristband, Water Bottle, Gym Bag)",
    ],
    "Books": [
        "Fiction (Novel, Thriller, Romance, Fantasy)",
        "Non-fiction (Biography, History, Self-Help, Cookbook)",
        "Children's Book (Picture Book, Chapter Book, Young Adult)",
        "Audiobook",
        "E-book",
    ],
    "Grocery": [
        "Fresh Produce (Fruits, Vegetables)",
        "Meat & Seafood",
        "Dairy Products (Milk, Cheese, Yogurt)",
        "Bread & Bakery",
        "Packaged Food (Snacks, Cereal, Pasta)",
        "Beverage (Soda, Juice, Coffee, Tea)",
    ],
    "Pet Supplies": [
        "Food",
        "Treats",
        "Toys",
        "Bedding",
        "Leash & Collar",
        "Grooming Supplies",
        "Carrier",
        "Medication",
        "Waste Disposal Bags",
    ],
    "Office Supplies": [
        "Paper (Printer Paper, Notebook)",
        "Pen & Pencil",
        "Desk Organizer",
        "Sticky Notes",
        "Highlighter",
        "Stapler",
        "Paper Clip",
        "Binder",
        "Label",
    ],
}


class Product:
    def __init__(self, name, category, price) -> None:
        self.name = name
        self.category = category
        self.price = price


class productGenerator:
    # static/class variable

    @staticmethod
    def generate():
        category = random.choice(CATEGORIES)
        if category in PRODUCTS:
            product = random.choice(PRODUCTS[category])
        else:
            # Handle case where category doesn't have a specific product list
            product = f"{category} - {random.randint(100, 999)}"  # Fallback
        price = round(random.uniform(10.00, 1000.00), 2)
        return Product(product, category, price)


def main():
    i = 0
    try:
        db_connection = dbcon()
        db_connection.connect()
        while True:
            product = productGenerator.generate()
            insert_query = """insert into product (name , category , price , last_updated)
            values('{}' , '{}' , '{}' , current_timestamp);""".format(
                product.name, product.category, product.price
            )
            db_connection.execute_query(insert_query)
            i += 1
            print("record inserted")
            sleep(random.randint(1, 10))
    except KeyboardInterrupt as err:
        print("KeyboardInterrupt")
        sys.exit(0)
    except Exception as err:
        print(f"An unexpected error occurred: {err}")

    finally:
        # Ensure connection is closed regardless of exceptions
        if db_connection:
            db_connection.close()
        print("Successfully inserted {} dummy rows (or connection closed).".format(i))


if __name__ == "__main__":
    main()
