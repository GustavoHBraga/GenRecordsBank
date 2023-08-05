import random
import string
import os
import asyncio

async def generate_cpf():
    return ''.join(random.choices(string.digits, k=11))

async def generate_name():
    first_names = ['Alice', 'Bob', 'Carol', 'David', 'Eva', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack']
    last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Taylor', 'Anderson']
    return f'{random.choice(first_names)} {random.choice(last_names)}'

async def generate_address():
    streets = ['Main St', 'Oak Ave', 'Elm St', 'Cedar Rd', 'Maple Ln', 'Pine Dr', 'Willow Ave', 'Birch Rd', 'Ash St', 'Cypress Ln']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
    return f'{random.choice(streets)}, {random.randint(1, 100)}, {random.choice(cities)}'

async def generate_pan():
    return ''.join(random.choices(string.digits, k=16))

async def generate_record(line_number, dados):
    print("Generating record...")
    for line in range(line_number):
        cpf = await generate_cpf()
        name = await generate_name()
        address = await generate_address()
        pan = await generate_pan()
        await dados.put(f'{line:06d};{cpf};{name};{address};{pan}#END#!\n')

async def generate_file(dados, num_records, file_path):
    print("Generating file...")
    processados = 0
    file = open(file_path, 'w')
    while processados < num_records:
        record = await dados.get()
        file.write(record)
        processados += 1
    
    file.close()

if __name__ == '__main__':
    dados = asyncio.Queue()
    el = asyncio.get_event_loop()

    num_records = 500_000
    file_path = 'registros.txt'

    task1 = generate_record(num_records, dados)  # Awaiting the function here
    task2 = generate_file(dados, num_records, file_path)
    tasks = asyncio.gather(task1, task2)
    el.run_until_complete(tasks)

    print(f'{num_records} registros foram gerados e salvos em {file_path}.')
