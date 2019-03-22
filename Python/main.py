import argparse
import WebScrapper


def main(contest_id):
    contestants = WebScrapper.Contest.ContestantList(contest_id)
    contestants.download()
    contestants.to_sql()

    contest = WebScrapper.Contest(contest_id)
    contest.monitor()

"""
parser = argparse.ArgumentParser('WebScrapping parser for downloading SkiJumping data from skokinarciarkis.pl')
parser.add_argument('contest_id', type=int, help='Contest id to be parsed from the web page.')

args = parser.parse_args()
main(args['contest_id'])
"""
"""
c = WebScrapper.Calendar()
c.download()
c.to_sql()
"""
main(2)
