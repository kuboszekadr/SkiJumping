import requests
import json
import os

from datetime import datetime
from random import random
from time import sleep
from bs4 import BeautifulSoup

class Contest:
    pass


class Jumper:
    pass


class Jump:
    def __init__(self, soup):
        self.__points = {}
        self.__jumper = {}
        self.__length = ''
        self.__wind = ''
        self.__series = ''

        jump_tag = soup.find('tr', class_='odd')
        details_tag = soup.find('tbody', class_='szczegoly_ukryj').find('table')

        self._parse_jump_tag(jump_tag)
        self._parse_details(details_tag)

    def _parse_jump_tag(self, tag):
        children = [ch for ch in tag.children]  # change list_iterator to list for easier data access
        self.__jumper['name'] = children[0].text
        self.__jumper['country'] = ''

        self.__length = children[2].text
        self.__points['total'] = children[3].text

    def _parse_details(self, tag):
        wind = [w for w in tag.find('tr', 'odd').children]
        self.__wind = wind[1].text
        self.__points['wind'] = wind[2].text

        points = tag.find_all('tr', 'even')

        # For trainings there are no judge notes that's why it is
        # important always have three variables
        marks = None

        if len(points) == 3:
            (marks, bar, speed) = points
        else:
            (bar, speed) = points

        bar = [b for b in bar.children]
        self.__bar = bar[1].text
        self.__points['bar'] = bar[2].text

        speed = [s for s in speed.children]
        self.__speed = speed[1].text

        mark = []
        if marks:
            for m in marks:
                if m['class'] == ['pkt']:
                    mark.append(m.text)
        self.__points['marks'] = mark

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

    def print(self):
        jumper = 'Jumper:\t{}'.format(self.jumper['name'])
        jump = 'Lenght:\t{}'.format(self.length)
        wind = 'Wind:\t{}'.format(self.wind)
        bar = 'Bar:\t{}'.format(self.bar)
        speed = 'Speed:\t{}'.format(self.speed)
        points = 'Points:\n'
        points += '\n'.join(['\t{}:\t{}'.format(key, value) for (key, value) in self.points.items()])

        print('\n'.join([jumper, jump, wind, bar, speed, points]))

    def save(self, path):
        data = {}
        data['jumper'] = self.jumper['name']
        data['country'] = ''  # self.jumper['country']
        data['wind'] = self.wind
        data['bar'] = self.bar
        data['speed'] = self.speed
        data['points'] = self.points

        with open(os.path.join(path, 'results_2_20190301.txt'), 'a') as f:
            f.write(json.dumps(data) + '\n')


def timestamp():
    dt = datetime.now()
    return dt.strftime("%Y%m%d %H:%M:%S")


url = 'https://www.skokinarciarskie.pl/m/cached/live_gora_static.html'
delay = 10

jumper = ''

while True:
    print('{}\tChecking for changes...'.format(timestamp()))

    try:
        r = requests.get(url)
        r.encoding = 'UTF-8'
    except requests.exceptions.ConnectionError as e:
        print("{}\tCan not request url".format(timestamp()))
        print(e)
        continue
    except e:
        print('{}\tUnhandled error occured:\n'.format(timestamp()))
        print(e)
        continue

    jump = Jump(BeautifulSoup(r.text, 'html.parser'))
    if jump.jumper != jumper:
        print('{}\tNew jump data available..'.format(timestamp()))
        jumper = jump.jumper
        jump.save(r'C:\Users\kubos\Desktop\D2-1.14b-Installer-plPL') # TO DO
        print('{}\tJump data:\n{}'.format(timestamp(), jump.print()))
    else:
        print('{}\tNo new data available...'.format(timestamp()))

    sleep(delay)

# soup = BeautifulSoup(r.text, 'html.parser')
#
# tables = soup.find_all('tbody')
# jump = tables[0]
# details = tables[1]
#
# soup.prettify()
#
# jump_data = jump.find_all('tr', 'even')
# jump_data = jump_data[len(jump_data)-1]
# length = jump_data.find('td', class_='odl').text
# total_points = jump_data.find('td', class_='pkt').text