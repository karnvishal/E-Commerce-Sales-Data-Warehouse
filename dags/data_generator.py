from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import numpy as np
import os

fake = Faker()

NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
MAX_ORDERS_PER_DAY = 20
CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Books', 'Beauty']
DAYS_TO_GENERATE = 30

def generate_customers(num_customers):
    customers = []
    for _ in range(num_customers):
        join_date = fake.date_between(start_date='-2y', end_date='today')
        customers.append({
            'customer_id': fake.uuid4(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': str(fake.zipcode()),
            'join_date': join_date,
            'loyalty_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'segment': random.choice(['Frequent', 'Occasional', 'One-time']),
            'credit_score': random.randint(300, 850)
        })
    return pd.DataFrame(customers)

def generate_products(num_products):
    products = []
    for _ in range(num_products):
        cost = round(random.uniform(5, 500), 2)
        price = round(cost * random.uniform(1.2, 3.0), 2)
        products.append({
            'product_id': fake.uuid4(),
            'sku': fake.bothify(text='SKU-####-%%%%'),
            'name': fake.catch_phrase(),
            'category': random.choice(CATEGORIES),
            'subcategory': fake.word(),
            'brand': fake.company(),
            'cost_price': cost,
            'selling_price': price,
            'weight': round(random.uniform(0.1, 20), 2),
            'created_at': fake.date_between(start_date='-3y', end_date='-6m'),
            'is_active': random.choices([True, False], weights=[0.9, 0.1])[0],
            'inventory_qty': random.randint(0, 1000)
        })
    return pd.DataFrame(products)

def generate_orders(customers, products, start_date, end_date):
    orders = []
    order_items = []
    
    current_date = start_date
    while current_date <= end_date:
        # Vary order volume by day of week
        weekday_factor = 1.5 if current_date.weekday() in [5, 6] else 1.0
        daily_orders = random.randint(int(MAX_ORDERS_PER_DAY * 0.3), 
                                    int(MAX_ORDERS_PER_DAY * weekday_factor))
        
        for _ in range(daily_orders):
            customer = customers.sample(1).iloc[0]
            order_date = current_date.replace(hour=random.randint(8, 22), 
                                            minute=random.randint(0, 59))
            order_id = fake.uuid4()
            
            # 5% chance of being a guest order
            if random.random() < 0.05:
                customer_id = None
                is_guest = True
            else:
                customer_id = customer['customer_id']
                is_guest = False
            
            order_status = random.choices(
                ['completed', 'processing', 'cancelled', 'returned'],
                weights=[0.85, 0.05, 0.05, 0.05]
            )[0]
            
            orders.append({
                'order_id': order_id,
                'customer_id': customer_id,
                'is_guest': is_guest,
                'order_date': order_date,
                'status': order_status,
                'total_amount': 0, 
                'shipping_address': customer['address'],
                'shipping_city': customer['city'],
                'shipping_state': customer['state'],
                'shipping_zip': str(customer['zip_code']),
                'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer']),
                'payment_status': 'paid' if order_status == 'completed' else random.choice(['pending', 'failed']),
                'shipping_cost': round(random.uniform(0, 15), 2)
            })
            
            # Generate 1-10 items per order
            num_items = random.randint(1, 10)
            order_total = 0
            for _ in range(num_items):
                product = products.sample(1).iloc[0]
                quantity = random.randint(1, 5)
                price = product['selling_price']
                discount = round(random.uniform(0, 0.3), 2) if random.random() < 0.3 else 0
                item_total = quantity * price * (1 - discount)
                order_total += item_total
                
                order_items.append({
                    'order_item_id': fake.uuid4(),
                    'order_id': order_id,
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': price,
                    'discount_pct': discount,
                    'total_price': item_total,
                    'return_status': 'not_returned' if order_status == 'completed' else random.choices(
                        ['not_returned', 'returned', 'refunded'],
                        weights=[0.9, 0.07, 0.03]
                    )[0],
                    'return_reason': None 
                })
            
            # Update order total
            if orders[-1]['status'] == 'completed':
                orders[-1]['total_amount'] = round(order_total + orders[-1]['shipping_cost'], 2)
            else:
                orders[-1]['total_amount'] = 0
        
        current_date += timedelta(days=1)
    
    # Add return reasons for returned items
    return_reasons = [
        'Wrong item', 'Defective', 'No longer needed', 
        'Better price elsewhere', 'Wrong size'
    ]
    for item in order_items:
        if item['return_status'] != 'not_returned':
            item['return_reason'] = random.choice(return_reasons)
    
    return pd.DataFrame(orders), pd.DataFrame(order_items)

def generate_inventory_movements(products, start_date, end_date):
    movements = []
    current_date = start_date
    while current_date <= end_date:
        for _, product in products.iterrows():
            # Generate random movement (positive = incoming, negative = outgoing)
            movement_type = random.choice(['purchase', 'sale', 'adjustment', 'return'])
            if movement_type in ['purchase', 'return']:
                qty = random.randint(1, 100)
            elif movement_type == 'sale':
                qty = -random.randint(1, 50)
            else: 
                qty = random.randint(-20, 20)
            
            if qty != 0:
                movements.append({
                    'movement_id': fake.uuid4(),
                    'product_id': product['product_id'],
                    'movement_date': current_date.replace(hour=random.randint(0, 23)),
                    'quantity': qty,
                    'movement_type': movement_type,
                    'reference_id': fake.uuid4(),
                    'notes': fake.sentence()
                })
        current_date += timedelta(days=1)
    return pd.DataFrame(movements)

def generate_daily_data(date):
    """Generate data for a specific date"""
    print(f"Generating data for {date.strftime('%Y-%m-%d')}")
    
    # Load existing customers and products to maintain consistency
    try:
        customers = pd.read_csv('data/customers.csv')
        products = pd.read_csv('data/products.csv')
    except FileNotFoundError:
        customers = generate_customers(NUM_CUSTOMERS)
        products = generate_products(NUM_PRODUCTS)
        os.makedirs('data', exist_ok=True)
        customers.to_csv('data/customers.csv', index=False)
        products.to_csv('data/products.csv', index=False)
    
    # Generate daily data
    orders, order_items = generate_orders(
        customers, products, 
        datetime.combine(date, datetime.min.time()),
        datetime.combine(date, datetime.max.time())
    )
    
    inventory_movements = generate_inventory_movements(
        products,
        datetime.combine(date, datetime.min.time()),
        datetime.combine(date, datetime.max.time())
    )
    
    # Save with date partitioning
    date_str = date.strftime('%Y-%m-%d')
    os.makedirs(f'data/orders/{date_str}', exist_ok=True)
    os.makedirs(f'data/order_items/{date_str}', exist_ok=True)
    os.makedirs(f'data/inventory/{date_str}', exist_ok=True)
    
    orders.to_csv(f'data/orders/{date_str}/orders.csv', index=False)
    order_items.to_csv(f'data/order_items/{date_str}/order_items.csv', index=False)
    print(f"File created: {os.path.abspath(f'data/orders/{date_str}/orders.csv')}")
    inventory_movements.to_csv(f'data/inventory/{date_str}/inventory_movements.csv', index=False)
    
    print(f"Generated data for {date_str}: {len(orders)} orders, {len(order_items)} items")

if __name__ == "__main__":
    # Generate data for yesterday by default (for daily runs)
    target_date = datetime.now() - timedelta(days=1)
    generate_daily_data(target_date.date())