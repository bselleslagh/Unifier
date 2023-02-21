'''
This script creates a dataframe with dummy customer data.
It uses a streetnames dataset to generate realistic addresses.
You can also update the local in the main_async function to generate data for other countries.
'''

import asyncio, tqdm
import faker
import pandas as pd
from dataclasses import dataclass

# Define the customer class
@dataclass
class Customer:
    firstname: str
    lastname: str
    street: str
    housenumber: int
    box: str
    postalcode: str
    city: str
    country: str
    birthdate: str
    email: str

# Create customer asynchronously
async def create_customer_async(streetnames, fake):
    record = streetnames.sample(1).to_dict('records')[0]
    customer = Customer(
        firstname=fake.first_name(),
        lastname=fake.last_name(),
        street=record['streetname_nl'],
        housenumber=record['house_number'],
        box=record['box_number'],
        postalcode=record['postcode'],
        city=record['municipality_name_nl'],
        country="Belgium",
        birthdate=fake.date_of_birth(),
        email=fake.email()
    )
    return customer

async def main_async(locale='nl_BE', amount=10000, max_concurrency=10):
    '''
    This function creates a dataframe with dummy customer data.
    '''
    streetnames = pd.read_pickle('../data/streetnames.pkl')
    fake = faker.Faker(locale=locale)
    customers = []
    tasks = []
    pbar = tqdm.tqdm(total=amount)
    for i in range(amount):
        task = asyncio.create_task(create_customer_async(streetnames, fake))
        tasks.append(task)
        if len(tasks) >= max_concurrency:
            # Wait for the tasks to complete before starting new ones
            completed, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in completed:
                customers.append(await task)
                pbar.update(1)
            tasks = list(pending)
    if tasks:
        # Wait for the remaining tasks to complete
        completed, pending = await asyncio.wait(tasks)
        for task in completed:
            customers.append(await task)
            pbar.update(1)
    df = pd.DataFrame(customers)
    df.to_parquet('../data/customers.parquet')

if __name__ == '__main__':
    asyncio.run(main_async())