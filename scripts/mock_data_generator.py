#!/usr/bin/env python3
"""
Mock Data Generator for eCommerce/POS Transactions
Generates realistic customer, product, and transaction data
"""

import random
import csv
from datetime import datetime, timedelta
import argparse
from pathlib import Path

CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Sports', 'Food', 'Beauty', 'Books']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'online_wallet', 'cash', 'bank_transfer']
STATUSES = ['completed', 'pending', 'refunded', 'cancelled']


class MockDataGenerator:
    def __init__(self, num_records=10000, seed=42):
        random.seed(seed)
        self.num_records = num_records
        self.num_customers = max(100, num_records // 50)
        self.num_products = max(50, num_records // 200)

    def generate_customers(self, output_path='data/raw/customers.csv'):
        """Generate customer data with PII"""
        print(f"Generating {self.num_customers} customers...")

        first_names = [
            'John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert',
            'Jessica', 'James', 'Maria', 'William', 'Linda', 'Richard',
            'Barbara', 'Thomas'
        ]
        last_names = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
            'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez',
            'Gonzalez', 'Wilson', 'Anderson'
        ]

        domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'company.com', 'mail.com']
        cities = [
            'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
            'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'
        ]
        states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']

        customers = []
        for i in range(self.num_customers):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)

            customer = {
                'customer_id': f'CUST_{i+1:06d}',
                'first_name': first_name,
                'last_name': last_name,
                'email': (
                    f'{first_name.lower()}.{last_name.lower()}{i}'
                    f'@{random.choice(domains)}'
                ),
                'phone': (
                    f'+1{random.randint(2, 9)}'
                    f'{random.randint(10, 99)}'
                    f'{random.randint(100, 999)}'
                    f'{random.randint(1000, 9999)}'
                ),
                'city': random.choice(cities),
                'state': random.choice(states),
                'zip_code': f'{random.randint(10000, 99999)}',
                'created_date': (
                    datetime.now() - timedelta(days=random.randint(30, 365))
                ).date(),
                'is_active': random.choice([True, True, True, False]),
            }
            customers.append(customer)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=customers[0].keys())
            writer.writeheader()
            writer.writerows(customers)

        print(f"[OK] Generated {len(customers)} customers -> {output_path}")
        return customers

    def generate_products(self, output_path='data/raw/products.csv'):
        """Generate product catalog"""
        print(f"Generating {self.num_products} products...")

        products = []
        for i in range(self.num_products):
            category = random.choice(CATEGORIES)
            product = {
                'product_id': f'PROD_{i+1:06d}',
                'product_name': f'{category} Item #{i+1}',
                'category': category,
                'price': round(random.uniform(5, 500), 2),
                'stock_quantity': random.randint(0, 1000),
                'is_active': random.choice([True, True, True, False]),
            }
            products.append(product)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=products[0].keys())
            writer.writeheader()
            writer.writerows(products)

        print(f"[OK] Generated {len(products)} products -> {output_path}")
        return products

    def generate_transactions(
        self, customers, products,
        output_path='data/raw/transactions.csv'
    ):
        """Generate transaction records"""
        print(f"Generating {self.num_records} transactions...")

        transactions = []
        start_date = datetime.now() - timedelta(days=90)

        for i in range(self.num_records):
            customer = random.choice(customers)
            product = random.choice(products)

            transaction_date = start_date + timedelta(
                days=random.randint(0, 89),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            quantity = random.randint(1, 5)
            amount = round(product['price'] * quantity, 2)

            if random.random() < 0.1:
                amount = round(amount * 0.9, 2)

            transaction = {
                'transaction_id': f'TXN_{i+1:09d}',
                'transaction_date': transaction_date,
                'customer_id': customer['customer_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': product['price'],
                'amount': amount,
                'payment_method': random.choice(PAYMENT_METHODS),
                'status': random.choices(STATUSES, weights=[85, 5, 5, 5])[0],
                'store_location': random.choice(
                    ['Online', 'Store-NYC', 'Store-LA', 'Store-CHI']
                ),
            }
            transactions.append(transaction)

        transactions.sort(key=lambda x: x['transaction_date'])

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=transactions[0].keys())
            writer.writeheader()
            writer.writerows(transactions)

        print(f"[OK] Generated {len(transactions)} transactions -> {output_path}")
        return transactions


def main():
    parser = argparse.ArgumentParser(description='Generate mock eCommerce data')
    parser.add_argument(
        '--records', type=int, default=10000,
        help='Number of transactions to generate (default: 10000)'
    )
    parser.add_argument(
        '--output-dir', type=str, default='data/raw',
        help='Output directory for CSV files (default: data/raw)'
    )
    parser.add_argument(
        '--seed', type=int, default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("eCommerce Mock Data Generator")
    print("=" * 60)

    generator = MockDataGenerator(num_records=args.records, seed=args.seed)

    customers = generator.generate_customers(f'{args.output_dir}/customers.csv')
    products = generator.generate_products(f'{args.output_dir}/products.csv')
    transactions = generator.generate_transactions(
        customers, products, f'{args.output_dir}/transactions.csv'
    )

    print()
    print("=" * 60)
    print("Data Generation Complete!")
    print("=" * 60)
    print()
    print("Summary:")
    print(f"  * Customers:      {len(customers):,}")
    print(f"  * Products:       {len(products):,}")
    print(f"  * Transactions:   {len(transactions):,}")
    print()
    print(f"Output Directory: {args.output_dir}/")
    print()
    print("[OK] Ready to start the pipeline!")
    print()
    print("Next steps:")
    print("  1. docker-compose up -d")
    print("  2. Go to http://localhost:8080 (Airflow)")
    print("  3. Enable and trigger a DAG")


if __name__ == '__main__':
    main()
