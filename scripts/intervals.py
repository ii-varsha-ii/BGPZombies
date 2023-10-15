import os
import pickle
from pathlib import Path

from mrtparse import *

import pandas as pd
from constants import *
from models import BgpUpdateMessage, Types, MRT_TYPES, BGP4MP_SUBTYPES, BGP4MP_MESSAGE_AS4_TYPES
from utils import __validate_constants


def __updates_sort_by_time(e):
    return e.time


def __get_next_step(prev_step):
    num_prev_step = int(prev_step)
    if num_prev_step % 100 == 55:
        num_prev_step = (int(num_prev_step / 100) + 1) * 100
    else:
        num_prev_step += 5
    count = abs(4 - len(str(num_prev_step)))
    str_prev_step = str(num_prev_step)
    while count:
        count = count - 1
        str_prev_step = "0" + str_prev_step

    return str_prev_step


def __fetch_as_path(path_attr):
    for a in path_attr:
        value = a['value']
        if Types.AS_PATH.value in a['type'].keys():
            return value[0]['value']
    return []


def create_intervals(start, end, month, start_date, end_date):
    list_of_dates = [d.strftime('%Y%m%d') for d in pd.date_range(f'{YEAR}{month}{start_date}', f'{YEAR}{month}{end_date}')]
    for date in list_of_dates:
        step = start
        bgp_updates = []
        while step != end:
            file_name = f"{ZIP_FOLDER}/{RRC}/{YEAR}.{month}/updates.{date}.{step}"
            print(file_name)
            step = __get_next_step(step)
            try:
                m = Reader(file_name)
            except FileNotFoundError as e:
                print(e)
                continue
            for record in m:
                if MRT_TYPES.BGP4MP.value in record.data['type'] \
                        and BGP4MP_SUBTYPES.BGP4MP_MESSAGE_AS4.value in record.data['subtype']:
                    bgp_msg = record.data['bgp_message']
                    if 'type' in bgp_msg \
                            and BGP4MP_MESSAGE_AS4_TYPES.UPDATE.value in bgp_msg['type']:
                        data = record.data
                        for route in data['bgp_message']['withdrawn_routes']:
                            bgp_update_record = BgpUpdateMessage(
                                time=list(data['timestamp'].keys())[0],
                                from_ip=data['peer_ip'],
                                from_as=data['peer_as'],
                            )
                            prefix_with_len = route['prefix'] + '/' + str(route['length'])
                            if prefix_with_len not in PREFIX:
                                continue
                            bgp_update_record.withdraw = route['prefix'] + '/' + str(route['length'])
                            bgp_updates.append(bgp_update_record)
                        for route in data['bgp_message']['nlri']:
                            bgp_update_record = BgpUpdateMessage(
                                time=list(data['timestamp'].keys())[0],
                                from_ip=data['peer_ip'],
                                from_as=data['peer_as'],
                            )
                            prefix_with_len = route['prefix'] + '/' + str(route['length'])
                            if prefix_with_len not in PREFIX:
                                continue
                            bgp_update_record.announce = route['prefix'] + '/' + str(route['length'])
                            if 'path_attributes' in data['bgp_message'] \
                                    and data['bgp_message']['path_attributes']:
                                path_attr = data['bgp_message']['path_attributes']
                                bgp_update_record.as_path = __fetch_as_path(path_attr)
                            bgp_updates.append(bgp_update_record)

        bgp_updates_dict = [dict(_entry) for _entry in bgp_updates]
        dir_name = os.path.dirname(f"{GROUPS_FOLDER}/{RRC}/{YEAR}.{month}/{date}/")
        os.makedirs(dir_name, exist_ok=True)
        pickle.dump(bgp_updates_dict, open(f"{GROUPS_FOLDER}/{RRC}/{YEAR}.{month}/{date}/{start}-{end}", "wb"))


def create_groups():
    if not MONTH:
        list_of_months = MONTHS
    else:
        list_of_months = [MONTH]

    start_date = '01'
    end_date = '30'
    if START_DATE:
        start_date = START_DATE
    if END_DATE:
        end_date = END_DATE

    for month in list_of_months:
        ALL_FILES = ANNOUNCEMENT_FILES + WITHDRAWAL_FILES
        ALL_FILES.sort()
        for item in ALL_FILES:
            start = item.split('-')[0]
            end = item.split('-')[1]
            create_intervals(start, end, month, start_date, end_date)


if __name__ == '__main__':
    if __validate_constants():
        create_groups()
