import requests
import json
import os
import re

from copy import deepcopy
from datetime import datetime
from time import sleep
from bs4 import BeautifulSoup


class Calendar:
    @staticmethod
    def download():
        r = requests.get(Calendar.url())
        r.encoding = 'UTF-8'

        page_content = BeautifulSoup(r.text, 'html5lib')
        tables = page_content.find('div', id='sLewaDol').find_all('table', class_='prog')

        calendar = []
        for table in tables:
            calendar = calendar + Calendar._parse_table(table)

        return calendar

    @staticmethod
    def _parse_table(tag):
        rows = tag.find_all('tr')
        date = rows[0].text
        del rows[0]

        data = []
        for row in rows:
            row_data = Calendar._parse_row(row)
            if True:
                row_data['date'] = date[:(date.find('(')-1)]
                data.append(row_data)

        return data

    @staticmethod
    def _parse_row(row_tag):
        dic = {}

        dic['date'] = ''
        dic['hour'] = row_tag.find('td', class_='prog_godz').text
        dic['name'] = row_tag.find_all('td', class_='prog_event1')[0].text
        dic['type'] = row_tag.find_all('td', class_='prog_event1')[1].text

        pattern = r'[\n\t]+'
        for entry in dic:
            dic[entry] = re.sub(pattern, '', dic[entry])

        return dic

    @staticmethod
    def url():
        return Contest.URLS['calendar']


class Contest:
    TYPES = {'seria próbna': 'trial',
             'konkurs': 'contest',
             'oficjalny trening': 'trening',
             'kwalifikacje': 'qualifications',
             'konkurs druż.': 'team competition'}

    URLS = {'contestants_list': 'https://www.skokinarciarskie.pl/m/cached/live_static_lista.html',
            'jump_details': 'https://www.skokinarciarskie.pl/m/cached/live_gora_static.html',
            'calendar': 'https://www.skokinarciarskie.pl/program-zawodow-w-skokach-narciarskich'}

    def __init__(self, **kwargs):
        self.__hill = {'name': kwargs['hill'], 'hs': kwargs['hs']}
        self.__date = kwargs['date']
        self.__hour = kwargs['hour']
        self.__type = kwargs['type']
        self.__source = kwargs['soup']

        self.__contestants = []
        self.__left = -1
        self.__total = -1
        self.__status = None

    @property
    def hill(self):
        return self.__hill

    @property
    def date(self):
        return self.__date

    @property
    def hour(self):
        return self.__hour

    @property
    def type(self):
        return self.__type

    def validate(self):
        raise NotImplementedError

    def get_data(self):
        raise NotImplementedError

        header = self.__source.find('div', class_='live_naglowek2')
        hill_date = header.find('div', class_='live_naglowek_a').text.split(' - ')

        (self.__hill['name'], self.__hill['hs']) = hill_date[0].split('-')
        self.__date = hill_date[1]

        type = header.find('div', class_='live_naglowek_b')
        contests_in_day = [x for x in type.children][1:]

        self.__type = Contest.TYPES[contests_in_day[0].text.lower()[:-1]]
        self.__hour = contests_in_day[1].replace(',', '').strip()

    def monitor(self, delay=10):
        last_jumper = ''  # initalize last jumper as empty string

        # TODO: Pause loop when no contestants left in a series
        # TODO: Exit function if two series passed
        while True:
            log('Checking for changes...')
            try:
                r = requests.get(Contest.URLS['jump_details'])
                r.encoding = 'UTF-8'
            except requests.exceptions.ConnectionError as e:
                log('Can not request url...\n' + e)
                sleep(delay*0.5)
                continue
            except e:
                log('Unhandled error occurred:\n' + e)
                continue

            try:
                jump = Jump(BeautifulSoup(r.text, 'html.parser'))
                jump.parse()
            except e:
                log('Unhandled error occured.\n' + str(e))

            if jump.jumper != last_jumper:
                log('New jump data available..')
                last_jumper = jump.jumper
                jump.save(r'')
                log('Jump data:\n{}'.format(jump.print()))
            else:
                log('No new data available...')

            sleep(delay)

    def save(self, path):
        t = timestamp().replace(':', '').replace(' ', '')[:8]
        file = os.path.join(path, r'results\contest_{}.txt'.format(t))

        d = deepcopy(self.__dict__)
        del d['_Contest__source']

        with open(file, 'a') as f:
            f.write(json.dumps(d) + '\n')

    def save_source(self):
        ts = timestamp().replace(' ', '').replace(':', ' ')[:8]
        with open(r'raw\contest_source_{}.txt'.format(ts), 'a') as f:
            f.write(self.__source)
            try:
                r = requests.get(self.URLS['contestants_list'])
                r.encoding = 'UTF-8'
            except requests.exceptions.ConnectionError as e:
                print('Can not request contestants_list')
                return
            f.write(r.text)

    @staticmethod
    def contenstants_list():
        r = requests.get(Contest.URLS['contestants_list'])
        r.encoding = 'UTF-8'

        soup = BeautifulSoup(r.text, 'html.parser')
        rows = soup.find('tbody').find_all('tr')[1:]

        if len(rows) == 1 and rows[0].text == '-':
            raise ValueError

        contestants = {}
        for row in rows:
            position = row.find('td', class_='poz').text
            contestant = row.find('td', class_='zaw').text
            contestants[position] = contestant

        return contestant


class Jump:
    def __init__(self, soup):
        self.__points = {}
        self.__jumper = {}
        self.__length = ''
        self.__wind = ''
        self.__series = ''
        self.__source = soup

    @property
    def jumper(self):
        return self.__jumper

    @property
    def length(self):
        return self.__length

    @property
    def wind(self):
        return self.__wind

    @property
    def bar(self):
        return self.__bar

    @property
    def speed(self):
        return self.__speed

    @property
    def points(self):
        return self.__points

    @property
    def series(self):
        return self.__series

    def parse(self):
        tables = self.__source.find_all('tbody')

        if len(tables) == 1:
            return  # TODO

        self._parse_summary(tables[0])
        self._parse_details(tables[1])

    def _parse_summary(self, tag):
        rows = [x for x in tag.children]
        rows = rows[1:]

        row = [x for x in rows[0].children]
        self.__jumper['name'] = row[0].text
        self.__jumper['country'] = row[1].find('img').attrs['title']

        row = row if len(rows) < 3 else [x for x in rows[2].children]
        self.__series = 1 if len(rows) < 3 else 2

        self.__length = row[2].text
        self.__points['total'] = row[3].text

    def _parse_details(self, tag):
        def parse_row(row):
            cols = [x for x in row.children]
            return cols[1].text, cols[2].text

        rows = [x for x in tag.find('tbody').children]

        if len(rows) == 4:
            notes_row = rows[0].find_all('td', class_='pkt')
            self.__points['notes'] = [x.text for x in notes_row][:-1]
            del rows[0]

        (self.__wind, self.__points['wind']) = parse_row(rows[0])
        (self.__bar, self.__points['bar']) = parse_row(rows[1])
        (self.__speed, __) = parse_row(rows[2])

    def print(self):
        jumper = 'Jumper:\t{}'.format(self.jumper['name'])
        jump = 'Lenght:\t{}'.format(self.length)
        wind = 'Wind:\t{}'.format(self.wind)
        bar = 'Bar:\t{}'.format(self.bar)
        speed = 'Speed:\t{}'.format(self.speed)
        points = 'Points:\n'
        points += '\n'.join(['\t{}:\t{}'.format(key, value) for (key, value) in self.points.items()])

        print('\n'.join([jumper, jump, wind, bar, speed, points]))

    def save(self, path=''):
        d = deepcopy(self.__dict__)
        del d['_Jump__source']

        t = timestamp().replace(' ', '')[:8]
        file = os.path.join(path, r'results\jumps_{}.txt'.format(t))

        with open(file, 'a', encoding='UTF-8') as f:
            f.write(json.dumps(d) + '\n')

    def save_source(self, path=''):
        t = timestamp().replace(' ', '')[:8]
        file = os.path.join(path, r'raw\jumps_{}.txt'.format(t))

        with open(file, 'a') as f:
            f.write(self.__source + '\n')


def timestamp():
    dt = datetime.now()
    return dt.strftime("%Y %m %d %H:%M:%S")


def log(msg):
    print(timestamp() + '\t' + msg)
