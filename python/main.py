from faker import Faker
import random

Faker.seed(0)
fake = Faker(['it_IT', 'en_US', 'ja_JP', 'fr_FR'])

def poll(offsets):
    nb_results = random.randint(1, 10) # from a thousands to 10,000 results will be returned
    results = []
    for _ in range(nb_results):
        results.append({
            'trap_id': fake.sbn9(),
            'speed': random.randint(500, 1500)/10,
            'license_plate': fake.license_plate(),
            'date_time': fake.iso8601(),
            'driver_name': fake.name(),
            'driver_address': fake.address(),
        })

    return {'value': results}

if __name__ == '__main__':
    print(poll(None))
