import os
import shutil
import random
import polars as pl
from faker import Faker
from datetime import datetime, timedelta
# Create a Faker instance for generating fake data
fake = Faker()

class Person:
    categories = ['A', 'B', 'C']
    next_id = None
    next_date = None
    def get_id(self):
        if Person.next_id is None: 
            Person.next_id = 0
        else: 
            Person.next_id += 1
        return Person.next_id

    def get_date(self):
        if Person.next_date is None: 
            Person.next_date = datetime(2023,1,1)
        else: 
            Person.next_date += timedelta(days=random.randint(0,3))
        return Person.next_date
    def __init__(self, start_date:datetime=None, **kwargs):
        if kwargs:
            for kw in kwargs:
                setattr(self, kw, kwargs.get(kw))
            return 
        self.id = self.get_id()
        self.first_name = fake.first_name()
        self.last_name = fake.last_name()
        self.age = random.randint(18, 80)
        self.address = fake.address()
        self.email = fake.email()
        self.category = fake.random_element(self.categories)
        self.start_date = start_date if start_date else self.get_date()

    def __str__(self):
        return f"id: {self.id} Name: {self.first_name} {self.last_name}, Age: {self.age}, Address: {self.address}, Email: {self.email}, Category: {self.category}"
    def __repr__(self):
        return self.__str__()
    
    def get_record(self):
        return pl.from_dicts({'id': self.id, 'first_name': self.first_name, 'last_name': self.last_name, 'age': self.age, 'address': self.address, 'email': self.email, 'category': self.category, 'start_date':self.start_date})

    def transactions(self):
        pass

    def generate_account_balance_history(self):
        account_balance = round(random.uniform(1000, 10000), 2)
        history = [{"person_id": self.id, "date": self.start_date, "balance": account_balance}]  # Initial balance
        return pl.from_dicts(history)
    
    def new_account_balance_record(self, last_date:datetime, last_balance:float):
        
        new_date = last_date + timedelta(days=random.randint(1,7))  # Simulate monthly balance
        new_balance = last_balance  + round(random.uniform(-500, 500), 2)
        record = [{"person_id":self.id, "date": new_date, "balance": new_balance}]
        return pl.from_dicts(record)

class AccountBalanceModel:
    people_path = "./delta_tables/people"
    account_balance_path = "./delta_tables/account_balance"

    people:pl.DataFrame
    account_balance:pl.DataFrame
    def __init__(self) -> None:
        if not os.path.isdir('./delta_tables'): os.mkdir('./delta_tables')
        Person.next_id = None
        Person.next_date = None
    
    def __str__(self):
        if hasattr(self, 'people'): return f"People ({len(self.people)}), Balance records ({len(self.account_balance)})"
        else: return 'AccountBalanceModel with no data'
    
    def __repr__(self):
        return self.__str__()

    def load(self):
        try:
            self.people = pl.read_delta(self.people_path)
            self.account_balance = pl.read_delta(self.account_balance_path)
            print(Person.next_id, Person.next_date)
            Person.next_id = self.people.select(pl.max('id')).to_series().to_list()[-1]
            Person.next_date = self.people.select(pl.max('start_date')).to_series().to_list()[-1]
            print(Person.next_id, Person.next_date)
        except:
            print("Loading failed...")
        return self
    
    def new_people(self, quantity:int=10)-> tuple[pl.DataFrame]:
        people = [Person() for i in range(quantity)]
        new_people = pl.concat([i.get_record() for i in people])
        account_balances = pl.concat([i.generate_account_balance_history() for i in people])
        return new_people, account_balances
    
    def new_model(self, new_people:int=10):
        Person.next_id = None
        self.people, self.account_balance = self.new_people(new_people)
        for i in ['people', 'account_balance']:
            path = getattr(self, f'{i}_path')
            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                pass
            getattr(self, i).write_delta(path)
        return self
    
    def last_balance_date(self, id):
        return (self.account_balance
                    .filter(pl.col('person_id') == id)
                    .select(pl.max('date'))
                    .to_series()
                    .to_list()[-1]
                    )
    
    def last_balance(self, id):
        df =  (self.account_balance
                    .filter(pl.col('person_id') == id)
                    .filter(pl.col('date') == self.last_balance_date(id))
                    .select(pl.col('balance'))
                    .to_series()
                    .to_list()[-1]
                    )
        return df
    
    def tables_must_exist(func):
        def wrapper(self, *args, **kwargs):
            if not hasattr(self, 'people') or not hasattr(self, 'account_balance'):
                raise ValueError('Load model or Create new model before adding records')
            return func(self, *args, **kwargs)
        return wrapper
    
    def new_balances(self, n_new=5):
        update_ids = random.sample(self.people.select('id').unique().to_series().to_list(), k=n_new)
        selected = self.people.filter(pl.col('id').is_in(update_ids)).to_dicts()
        last_date = self.account_balance.select(pl.max('date')).to_series().to_list()[-1]
        last_balances = {i['id']:self.last_balance(i['id']) for i in selected}
        return pl.concat([Person(**i).new_account_balance_record(last_date=last_date, last_balance=last_balances[i['id']]) for i in selected])
    
    @tables_must_exist
    def iterate(self, n_new:int=2, new_balances:int=5):
        new_records, first_balances = self.new_people(n_new) 
        self.people = pl.concat([self.people, new_records])
        self.account_balance = pl.concat([self.account_balance, first_balances])
        self.account_balance = pl.concat([self.account_balance, self.new_balances(new_balances)])
        return self
        
    def add_records(self, iterations=1):
        for i in range(iterations):
            self = self.iterate()
        return self
    
    @tables_must_exist
    def save(self):
        for i in ['people', 'account_balance']:
            path = getattr(self, f'{i}_path')
            getattr(self, i).write_delta(path, mode='append')
            # getattr(self, i).write_delta(path, mode='overwrite')
        return self

