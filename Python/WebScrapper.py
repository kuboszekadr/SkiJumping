import requests
import json
import os
import re
import sqlalchemy

from copy import deepcopy
from datetime import datetime
from time import sleep
from bs4 import BeautifulSoup

class Meta:
    URLS = {'contestants_list': 'https://www.skokinarciarskie.pl/m/cached/live_static_lista.html',
            'jump_details': 'https://www.skokinarciarskie.pl/m/cached/live_gora_static.html',
            'calendar': 'https://www.skokinarciarskie.pl/program-zawodow-w-skokach-narciarskich',
            'live': 'https://www.skokinarciarskie.pl/skoki-na-zywo-live/'}

    CONTEST_TYPES = {'seria próbna': 'trial series',
                     'konkurs ind.': 'contest',
                     'oficjalny trening': 'trening',
                     'kwalifikacje': 'qualifications',
                     'konkurs druż.': 'team competition',
                     'odprawa techniczna': 'briefing',
                     'test skoczni': 'hill test'}

    class DB:
        SCHEMA = 'skijumping'

        @staticmethod
        def connection():
            # return sqlalchemy.create_engine('postgresql://postgres:admin@localhost:5432/postgres')
            return sqlalchemy.create_engine('postgresql://gops:gops@localhost/gops')

        @staticmethod
        def to_sql(rows, table_name):
            """
            Creates insert into query based on list of dictionary
            :param rows: List of dictionary with keys corresponding to table columns (order not important)
            :param table_name: table to which data shall be exported
            :return: sql smt
            """
            sql_stmt_pattern = "insert into {}.{} ({}) values ".format(Meta.DB.SCHEMA,
                                                                       table_name,
                                                                       ','.join(['"{}"'.format(x) for x in rows[0]])
                                                                       )
            sql_stmts = []
            for row in rows:
                sql_stmt = '({})'.format(','.join("'{}'".format(x) for x in row.values()))
                sql_stmts.append(sql_stmt)

            Meta.DB.connection().execute(sql_stmt_pattern + ',\n'.join(sql_stmts))


class Calendar:
    def __init__(self):
        self.__rows = []
        self.__source = ''

    @property
    def source(self):
        return self.__source

    def download(self):
        try:
            r = requests.get(Calendar.url())
            r.encoding = 'ISO 8859-2'
            self.__source = r.text
        except ConnectionAbortedError as e:
            raise e

        page_content = BeautifulSoup(self.source, 'html5lib')
        tables = page_content.find('div', id='sLewaDol').find_all('table', class_='prog')

        self.__rows = []
        for table in tables:
            self.__rows = self.__rows + Calendar._parse_table(table)

        return self.__rows

    @staticmethod
    def _parse_row(row_tag):
        dic = {}

        dic['date'] = ''
        dic['hour'] = row_tag.find('td', class_='prog_godz').text
        dic['name'] = row_tag.find_all('td', class_='prog_event1')[0].text
        dic['type'] = row_tag.find_all('td', class_='prog_event1')[1].text

        pattern = r'[\n\t]+'
        for entry in dic:  # removing redundant patterns in data
            dic[entry] = re.sub(pattern, '', dic[entry])
            dic[entry] = re.sub(' ✔', '', dic[entry])

        try:
            dic['type'] = Meta.CONTEST_TYPES[dic['type']]
        except KeyError:  # if new contest type occurs do not map it
            log('Can not map calendar entry:\t{}\nEntry will not be mapped'.format(dic['type']),
                'warn')
            pass

        return dic

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
    def url():
        return Meta.URLS['calendar']

    def to_sql(self):
        Meta.DB.to_sql(self.__rows, 'calendar')

    def save(self, path=''):
        file = os.path.join(path, r'raw\calendar_{}.txt'.format(timestamp('YYYYMMDD')))
        with open(file, 'a') as f:
            f.write(self.source)


class Hill:
    def __init__(self, tag):
        self.__id = -1
        self.__source = tag

        soup = BeautifulSoup(tag, 'html.parser')
        header = soup.find('div', class_='live_naglowek2')
        hill_date = header.find('div', class_='live_naglowek_a').text.split(' - ')

        (self.__name, self.__hs) = hill_date[0].split('-')

    @property
    def name(self):
        name = self.__name.replace('\n', '')
        return name[:name.find(' HS')]

    @property
    def hs(self):
        return self.__hs

    @property
    def id(self):
        return self.__id

    def get_id(self):
        stmt = "select id from {}.hill where name = '{}' and hs = '{}'".format(Meta.DB.SCHEMA,
                                                                               self.name,
                                                                               self.hs)
        con = Meta.DB.connection()
        id = con.execute(stmt)

        if id.rowcount == 0:
            _id = self._new(con)
        elif id.rowcount == 1:
            _id = id.fetchone()['id']
        else:
            raise KeyError

        self.__id = _id
        del con

    def _new(self, con):
        stmt = "insert into {}.hill (name, hs) values ('{}', '{}') returning id"
        stmt = stmt.format(Meta.DB.SCHEMA, self.name, self.hs)

        return con.execute(stmt).fetchone()['id']


class Contest:
    TABLE = 'contest'

    class ContestantList:
        def __init__(self, contest_id):
            self.__contest_id = contest_id
            self.__contestants = []

        @property
        def contestants(self):
            return self.__contestants

        def download(self):
            try:
                r = requests.get(Meta.URLS['contestants_list'])
                r.encoding = 'UTF-8'
            except requests.exceptions.ConnectionError as e:
                log('Can not request URL...\n' + str(e))
                raise

            soup = BeautifulSoup(r.text, 'html.parser')

            # Check if page is not empty
            if soup.text == '<!---->':
                raise ValueError('Page empty')

            rows = soup.find('tbody').find_all('tr')[1:]  # find table containing jumpers and order
            if len(rows) == 1 and rows[0].text == '-':
                raise ValueError

            self.__contestants = []
            for row in rows:
                position = row.find('td', class_='poz').text

                jumper = Jumper(parser='None')
                jumper.name = row.find('td', class_='zaw').text
                jumper.get_id()

                self.__contestants.append({'position': position,
                                           'jumper': jumper.id})

        def to_sql(self):
            """
            Exports contestants list to database replacing name with ID
            :return:
            """
            if len(self.__contestants) == 0:
                raise ValueError('No contestants available.')

            # Checking if contestant list is already on database
            stmt = "select 1 from {}.contestants where contest_id = {} limit 1".format(Meta.DB.SCHEMA,
                                                                                       self.__contest_id)
            if Meta.DB.connection().execute(stmt).rowcount > 0:
                print('List for this contest is already available')
                return

            jumpers = []
            for c in self.__contestants:
                jumper = Jumper(parser='None')
                jumper.name = c['jumper']
                jumper.get_id()

                jumpers.append({'contest_id': self.__contest_id,
                                'position': c['position'],
                                'jumper_id': jumper.id})

            Meta.DB.to_sql(jumpers, 'contestants')

    def __init__(self, calendar_id):
        self.__calendar_id = calendar_id

        r = requests.get(Meta.URLS['live'])
        r.encoding = 'UTF-8'

        self.__hill = Hill(r.text)
        self.__hill.get_id()

        self.__source = ''
        self.__contestants = []
        self.__left = -1
        self.__status = None
        self.__type = self._get_type()

    @property
    def hill(self):
        return self.__hill

    @property
    def type(self):
        return self.__type

    def _get_type(self):
        """
        Checks contest table based on database entry
        :return: Contest type
        """
        stmt = "select type from {}.{} where id = {}".format(Meta.DB.SCHEMA,
                                                             'calendar',
                                                             self.__calendar_id)
        cursor = Meta.DB.connection().execute(stmt)
        if cursor.rowcount > 1:
            raise ValueError('Duplicated entry for contest id = {}'.format(self.__calendar_id))
        elif cursor.rowcount == 0:
            raise ValueError('Contest id = {} does not exist'.format(self.__calendar_id))

        return cursor.fetchone()['type']

    def monitor(self, delay=10):
        def download_jump(jumper, tag):
            jumper.get_id()
            jump = Jump(jumper, tag)

            try:
                jump.parse()
            except AttributeError as e:
                unhandled_error('download_jump', e, tag)
                return
            except Exception as e:
                unhandled_error('download_jump', e, tag)
                return

            return jump

        def unhandled_error(step, e, source=''):
            log('Unhandled error occured during {}\n{}'.format(step, str(e)))

            file = r'errors\{}_{}.txt'.format(timestamp('YYYYMMDD'), step)
            with open(file, 'a', encoding='UTF-8') as f:
                f.write('{}\t{}\t{}\n'.format(step, str(e), source))

        last_jumper = ''  # initialize last jumper as empty string
        while True:
            # Check if data is available
            try:
                r = requests.get(Meta.URLS['jump_details'])
                r.encoding = 'UTF-8'

                jumper = Jumper(r.text)
            except AttributeError:
                log('No jump data available yet')
                sleep(delay)
                continue
            except TimeoutError:
                log('Can not retrieve data from the server...' + str(e))
                continue
            except requests.exceptions.ConnectionError as e:
                log('Can not request jump details url...', 'err')
                continue
            except Exception as e:
                unhandled_error('test', e)

            # Check if new data is available
            if jumper.name == last_jumper:
                log('No new data available...')
                sleep(delay)
                continue

            jump = download_jump(jumper, r.text)
            if not jump:
                log('Error occured during parsing, please check log file.')
                continue

            log('New jump data available..')
            last_jumper = jumper.name

            jump.save()  # saves raw data and json
            jump.to_sql(self.__calendar_id)  # export results to database
            print(jump)

            # Download updated list of contestants
            try:
                jumpers_left = Contest.ContestantList(self.__calendar_id)
                jumpers_left.download()
                self.__left = len(jumpers_left.contestants)
            except ValueError:
                self.__left = 0
            except requests.exceptions.ConnectionError:
                log('Can not request contestants url...')
            except Exception as e:
                unhandled_error('contestants', e)
            finally:
                log('Contestants left: {}'.format(self.__left))

            if self.__left == 0 and jump.series == 2 and self.type in ['team competition', 'contest']:
                log("Contest ended.")
                return
            elif self.__left == 0 and self.type not in ['team competition', 'contest']:
                log("Contest ended.")
                return

            sleep(delay)

    @staticmethod
    def parse_archive(url):
        #TODO: Add exporting to SQL
        r = requests.get(url)
        r.encoding = 'ISO 8859-2'

        soup = BeautifulSoup(r.text, 'html.parser')
        header = soup.find('div', id='sLewaDol').find('table')

        rows = header.find_all('tr')[2:]
        for row in rows:
            try:
                jumper = Jumper(str(row), 'archive')
            except Jumper.NoDataAvailableError:
                continue

            jumps = row.find_all('td', class_='odl')

            for j in jumps:
                try:
                    jump = Jump(jumper, str(j), 'archive')
                    print(jump)
                except Jump.DetailedDataError:
                    pass

    def save(self, path):
        file = os.path.join(path, r'results\{}_{}.txt'.format(self.TABLE, timestamp('YYYYMMDD')))

        d = deepcopy(self.__dict__)
        del d['_Contest__source']

        with open(file, 'a') as f:
            f.write(json.dumps(d) + '\n')

    def save_source(self):
        with open(r'raw\contest_source_{}.txt'.format(timestamp('YYYYMMDD')), 'a') as f:
            f.write(self.__source)
            try:
                r = requests.get(Meta.URLS['contestants_list'])
                r.encoding = 'UTF-8'
            except requests.exceptions.ConnectionError:
                print('Can not request contestants_list')
                return
            f.write(r.text)

    def to_sql(self):
        dic = {'calendar_id': self.__calendar_id,
               'hill_id': self.hill.id,
               'contestants_amount': len(self.__contestants)}

        Meta.DB.to_sql(dic, 'contest')


class Jumper:

    class NoDataAvailableError(Exception):
        pass

    def __init__(self, tag='', parser='online'):
        self.__name = ''
        self.__country = ''
        self.__id = -1
        self.__source = tag

        def online_parser(soup):
            table_tag = soup.find('tbody')  # first table contains jumper data
            rows = [x for x in table_tag.children]
            del rows[0]  # remove header

            cols = [x for x in rows[0].children]
            self.__name = cols[0].text
            self.__country = cols[1].find('img').attrs['title']

        def archive_parser(soup):
            try:
                self.__name = soup.find('td', class_='zaw').text
            except AttributeError:
                if len(soup.find('tr', class_='przerwa even')) > 0:
                    raise Jumper.NoDataAvailableError('Split row passed.')

            self.__country = soup.find('td', class_='fla').img['title']

        soup = BeautifulSoup(self.__source, 'html.parser')
        if parser == 'online':
            online_parser(soup)
        elif parser == 'archive':
            archive_parser(soup)

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, value):
        self.__name = value

    @property
    def country(self):
        return self.__country

    @property
    def id(self):
        return self.__id

    def get_id(self):
        """
        Retrieves jumper ID from the database, if jumper does not exist creates new entry
        :return: jumper id
        """
        def add_new(name, country):
            stmt = "insert into {}.jumper (name, country) values ('{}', '{}') returning id"
            stmt = stmt.format(Meta.DB.SCHEMA, name, country)

            return con.execute(stmt).fetchone()['id']

        con = Meta.DB.connection()
        id = con.execute("select id from {}.jumper where name = '{}'".format(Meta.DB.SCHEMA, self.name))

        if id.rowcount == 0:
            _id = add_new(self.name, self.country)  # adds new jumper to dictionary
        elif id.rowcount == 1:
            _id = id.fetchone()['id']  # gets jumpers' id
        else:  # just in case...
            raise KeyError('Multiple instances of jumper {} exist'.format(self.name))

        self.__id = _id
        del con


class Jump:
    class DetailedDataError(Exception):
        pass

    def __init__(self, jumper, tag, parser='online'):
        self.__points = {}
        self.__jumper = jumper

        self.__length = ''
        self.__wind = ''
        self.__series = ''
        self.__source = tag

        self.__hard_space = '\u00a0'

        if parser == 'online':
            self._online_parser()
        elif parser == 'archive':
            self._archive_parser()

    def __str__(self):
        jumper = 'Jumper:\t{}'.format(self.jumper.name)
        jump = 'Lenght:\t{}'.format(self.length)
        wind = 'Wind:\t{}'.format(self.wind)
        bar = 'Bar:\t{}'.format(self.bar)
        speed = 'Speed:\t{}'.format(self.speed)
        points = 'Points:\n'
        points += '\n'.join(['\t{}:\t{}'.format(key, value) for (key, value) in self.points.items()])

        return '\n'.join([jumper, jump, wind, bar, speed, points])

    @property
    def jumper(self):
        return self.__jumper

    @property
    def length(self):
        return self.__length

    @property
    def wind(self):
        return self.__wind[:self.__wind.find(self.__hard_space)]

    @property
    def bar(self):
        return self.__bar

    @property
    def speed(self):
        return self.__speed[:self.__speed.find(self.__hard_space)]

    @property
    def points(self):
        return self.__points

    @property
    def series(self):
        return self.__series

    def _online_parser(self):
        """
        Parses data available in live-data monitor
        :return:
        """
        soup = BeautifulSoup(self.__source, 'html.parser')
        tables = soup.find_all('tbody')

        if len(tables) == 1:
            raise ValueError('No data available')

        summary = tables[0]
        rows = [x for x in summary.children]
        del rows[0]

        row = [x for x in rows[0].children]

        # There is different table depending on series...
        row = row if len(rows) < 3 else [x for x in rows[2].children]
        self.__series = 1 if len(rows) < 3 else 2

        self.__length = row[2].text
        self.__points['total'] = row[3].text

        # Parsing detailed table
        # wind, length, notes etc.
        details = tables[1]
        rows = [x for x in details.find('tbody').children]

        def parse_row(row):
            cols = [x for x in row.children]
            try:
                type = cols[1].text
                points = float(cols[2].text.replace(self.__hard_space + 'pkt', ''))
            except ValueError:
                points = cols[2].text

            return type, points

        # Check if table contains referral notes
        if len(rows) == 4:
            notes_row = rows[0].find_all('td', class_='pkt')
            self.__points['notes'] = [x.text for x in notes_row][:-1]
            del rows[0]  # delete notes data to make code notes-independent

        (self.__wind, self.__points['wind']) = parse_row(rows[0])  # wind power, wind points
        (self.__bar, self.__points['bar']) = parse_row(rows[1])  # bar number, bar points
        (self.__speed, __) = parse_row(rows[2])  # speed

    def _archive_parser(self):
        """
        Parses jump data available in archive
        :return:
        """
        soup = BeautifulSoup(self.__source, 'html.parser')
        jump = soup.find('td', class_='odl')
        data = jump['title'].replace('\r', '').replace('\n\n', '\n').split('\n')

        if data[0] == 'brak szczegółowych danych dla tego skoku':
            raise Jump.DetailedDataError('No detailed data available')

        bar_data = data[1]
        self.__bar = re.search('^[0-9].', bar_data).group(0)

        pattern = '[\+-\-]*[0-9]+.[0-9]+'
        self.__points['bar'] = re.search(pattern, bar_data).group(0)

        wind_data = data[3]
        (self.__wind, self.__points['wind']) = re.findall(pattern, wind_data)

        self.__speed = re.search('[0-9]+.[0-9]+', data[5]).group(0)
        self.__points['notes'] = data[7].replace(' ', '').replace('|', ',')

        self.__length = jump.text

    def to_sql(self, contest_id):
        """
        Exports jump data into database
        :param contest_id: int
        :return:
        """
        dic = {'jumper_id': self.jumper.id,
               'contest_id': contest_id,
               'series': self.series,
               'length': self.length,
               'bar': self.bar,
               'bar_points': self.points['bar'],
               'wind': self.wind,
               'wind_points': self.points['wind']
               }

        try:
            dic['total_points'] = self.points['total']
            dic['style_points'] = ','.join(self.points['notes'])
        except KeyError:
            pass

        Meta.DB.to_sql([dic], 'jump')

    def save(self, path=''):
        """
        Saves jump raw and parsed data into YYYYMMDD.txt file
        :param path: root folder where data should be save
        :return:
        """
        t = timestamp('YYYYMMDD')

        pattern = r'{}\jumps_{}.txt'
        self._save_results(os.path.join(path, pattern.format('results', t)))
        self._save_source(os.path.join(path, pattern.format('raw', t)))

    def _save_results(self, filepath):
        """
        Save jump parsing results
        :param filepath: path to file to be appended
        :return:
        """
        d = deepcopy(self.__dict__)
        del d['_Jump__source']
        del d['_Jump__jumper']

        d['_Jump__jumper'] = self.__jumper.__dict__

        with open(filepath, 'a', encoding='UTF-8') as f:
            f.write(json.dumps(d) + '\n')

    def _save_source(self, filepath):
        """
        Save source data of the jump
        :param filepath:
        :return:
        """
        with open(filepath, 'a', encoding='UTF-8') as f:
            f.write(self.__source + '\n')


def timestamp(format=''):
    dt = datetime.now()
    if format == 'YYYYMMDD':
        return dt.strftime("%Y%m%d")

    return dt.strftime("%Y %m %d %H:%M:%S")


# TODO: Add log type fe warning, error
def log(msg, type=''):
    _msg = timestamp() + '\t' + msg
    print(_msg)

"""
r = requests.get('https://www.skokinarciarskie.pl/index.php?a=wyniki&b=wyniki&cykl=ps&sezon=2018/2019&konkurs_id=8634&seria=2')
r.encoding = 'ISO 8859-2'
r.status_code

soup = BeautifulSoup(r.text, 'html.parser')
print(soup.prettify())

header = soup.find('div', id='sLewaDol').find('table')
rows = header.find_all('tr')[2:]

row = rows[0]
jumper = Jumper(str(row), 'archive')

jumps = row.find_all('td', class_='odl')
data = jumps[0]['title'].replace('\r', '').replace('\n\n', '\n').split('\n')

pattern = '[\+-\-][0-9]+.[0-9]+'

bar_data = data[1]
bar = re.search('^[0-9].', bar_data).group(0)
bar_points = re.search(pattern, bar_data).group(0)

wind_data = data[3]
(wind_power, wind_points) = re.findall('[\+-\-][0-9]+.[0-9]+', wind_data)

speed = re.search('[0-9]+.[0-9]+', data[5]).group(0)
notes = data[7].replace(' ', '').replace('|', ',')

length = jumps[0].text
"""

Contest.parse_archive('https://www.skokinarciarskie.pl/index.php?a=wyniki&b=wyniki&cykl=ps&sezon=2018/2019&konkurs_id=8634&seria=2')

pattern = '[\+\-]*[0-9]+\.[0-9]+'
s = '+0.00 m/s (-1.7 pkt)'
re.findall(pattern, s)