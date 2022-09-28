import random
from faker import Faker

import re
import pandas as pd
from model import model_client_db
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, insert
from sqlalchemy.types import Integer, Enum, Date


class Hash:
    def __init__(self, s: str):
        self.p, self.m, self.hash = 31, 10**9 + 7, 0
        self.compute_hashes(s)
 
    def compute_hashes(self, s: str):
        hash = 0
        for ch in s:
            value = ord(ch)
            hash = (hash * self.p + value) % self.m
        self.hash = hash
 
    def __eq__(self, other):
        return self.hash == other.hash
 
    def __str__(self):
        return f'({self.hash})'
 

def get_hash_value(s: str):
    return Hash(s).hash

def fix_cpf(cpf):
    cpf = re.sub('[.-]','', cpf)
    return cpf

def fix_rg(rg):
    rg = str(rg)
    return rg.replace('X', '0')

def insert_data(engine_db, table_name, classe_db, num_of_rows):
    Faker.seed(123)
    faker = Faker(['pt_BR'])

    id = 0
    
    session_db = Session(engine_db)

    for row in range(num_of_rows):

        nome = faker.name()
        rg = fix_rg(faker.rg())
        cpf = fix_cpf(faker.cpf())
        idade = random.randint(0, 100)
        data_de_nascimento = faker.date()
        endereco = faker.address()
        email = faker.ascii_email()
        telefone = faker.cellphone_number()
        profissao = faker.job()

        stmt = insert(classe_db[table_name]).values(
            id = id,
            nome = nome,
            rg = rg,
            cpf = cpf,
            idade = idade,
            data_de_nascimento = data_de_nascimento,
            endereco = endereco,
            email = email,
            telefone = telefone,
            profissao = profissao
        )

        id += 1

        session_db.execute(stmt)
    
    session_db.commit()

if __name__ == '__main__':

    USER = 'root'
    DB_PW = 'Dd16012018'
    HOST = 'localhost'
    DB = 'ficticio_database'

    engine_db = create_engine('mysql://{}:{}@{}:3306/{}'.format(USER, DB_PW, HOST, DB))
    
    create_table = "\
        create table nivel1(\
        id INT NOT NULL, \
        nome VARCHAR(100) NOT NULL,\
        rg VARCHAR(200) NOT NULL,\
        cpf VARCHAR(200) NOT NULL,\
        idade VARCHAR(200) NOT NULL,\
        data_de_nascimento DATE,\
        endereco VARCHAR(200),\
        email VARCHAR(100),\
        telefone VARCHAR(50),\
        profissao VARCHAR(50),\
        PRIMARY KEY (id)\
        );"

    engine_db.execute(create_table)

    classes_db = {}
    for table_name in engine_db.table_names():
        classes_db[f"{table_name}"] = eval(f"model_client_db.{table_name.capitalize()}")

    insert_data(engine_db=engine_db, table_name='nivel1', classe_db=classes_db, num_of_rows=10000)

    print('FIM')