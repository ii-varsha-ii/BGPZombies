import datetime
import logging
import sys

from scripts.constants import RRCs
from scripts.intervals import create_intervals
from scripts.utils import get_dates
from scripts.zombies import run_zombies_test
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Extract BGP MRT dump and find BGP Zombies"
    )
    parser.add_argument(
        '-s', '--startdate', type=str, help="Provide start date in format %Y%m%d", required=True
    )
    parser.add_argument(
        '-e', '--enddate', type=str, help="Provide end date in format %Y%m%d", required=True
    )
    parser.add_argument(
        '-r', '--rrc', type=str, help="Provide RRC", required=True
    )
    sub_parsers = parser.add_subparsers(dest="command")
    intervals_parser = sub_parsers.add_parser("intervals", help="Extract BGP MRT dump into intervals.")
    zombies_parser = sub_parsers.add_parser("zombies", help="Find BGP Zombies. Run intervals before running zombies "
                                                            "to extract.")
    args = parser.parse_args()

    try:
        start = datetime.datetime.strptime(args.startdate, "%Y%m%d")
        end = datetime.datetime.strptime(args.enddate, "%Y%m%d")
        year = start.year
        dates = get_dates(start, end)
    except Exception as e:
        logging.error(f"Date format error. {e}")
        sys.exit(1)

    if args.rrc in RRCs:
        rrc = args.rrc
    else:
        logging.error(f"RRC not supported")
        sys.exit(1)

    if args.command == "intervals":
        create_intervals(dates, rrc)
    if args.command == "zombies":
        run_zombies_test(dates, rrc)
