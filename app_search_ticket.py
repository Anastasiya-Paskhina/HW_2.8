import csv
import re
from pymongo import MongoClient
import pymongo
from pprint import pprint
from datetime import datetime


client = MongoClient()
db_tickets = client.tickets


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        event_data = list()
        for row in reader:
            row['Цена'] = int(row['Цена'])
            date = row['Дата'] + '.{}'.format(datetime.now().year)
            row['Дата'] = datetime.strptime(date, '%d.%m.%Y').date()
            event_data.append(row)
        # создаем коллекцию билетов
        ticket_list = db.tickets_list
        ticket_list.insert_many(event_data)


def find_cheapest(db):
    """
    Найти самые дешевые билеты
    Документация: https://docs.mongodb.com/manual/reference/operator/aggregation/sort/
    """
    selected_tickets = list(db.ticket_list.find().sort('Цена', pymongo.ASCENDING))
    print('Самый дешевый билет на концерт {} стоит {}'.format(selected_tickets[0]['Исполнитель'],
                                                              selected_tickets[0]['Цена']))


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и выведите их по возрастанию цены
    """
    regex = re.compile(r'\w*{}\w*'.format(name))
    selected_tickets = list(db.ticket_list.find({'Исполнитель': regex}).sort('Цена', pymongo.ASCENDING))
    for ticket in selected_tickets:
        print('{} {} {}'.format(ticket['Дата'], ticket['Исполнитель'], ticket['Цена']))


if __name__ == '__main__':
    read_data('artists.csv', db_tickets)
    find_cheapest(db_tickets)
    find_by_name('T', db_tickets)