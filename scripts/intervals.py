import datetime
import os
import pickle

from mrtparse import *

from scripts.constants import *
from scripts.download import download_updates_file, construct_updates_filename, construct_updates_filename_url, \
    construct_output_path
from scripts.models import BgpUpdateMessage, Types, MRT_TYPES, BGP4MP_SUBTYPES, BGP4MP_MESSAGE_AS4_TYPES


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


def fetch_aw(intervals, year, month, date, rrc):
    bgp_updates = []
    for interval in intervals:
        start, end = interval.split('-')
        step = start
        while step != end:
            file_name = construct_updates_filename(ZIP_FOLDER, rrc, year, month, date, step)
            try:
                m = Reader(file_name)
            except FileNotFoundError as e:
                print(f"File not found. Downloading {file_name}...")
                dir_name = construct_output_path(ZIP_FOLDER, rrc, year, month)
                abs_filename = os.path.abspath(dir_name)
                if not os.path.isdir(abs_filename):
                    os.makedirs(dir_name, exist_ok=True)
                file_url = construct_updates_filename_url(rrc, year, month, date, step)
                download_updates_file(file_url, output_path=abs_filename)
                m = Reader(file_name)
                continue
            step = __get_next_step(step)
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
    group_dir = construct_output_path(GROUPS_FOLDER, rrc, year, month)
    group_file_name = os.path.abspath(construct_updates_filename(GROUPS_FOLDER, rrc, year, month, date))
    dir_name = os.path.dirname(group_dir)
    os.makedirs(dir_name, exist_ok=True)
    pickle.dump(bgp_updates_dict, open(group_file_name, "wb"))


def create_intervals(dates, rrc):
    INTERVALS = ANNOUNCEMENT_FILES + WITHDRAWAL_FILES
    INTERVALS.sort()
    for date in dates:
        date_obj = datetime.strptime(date, "%Y%m%d")
        year, month, date = date_obj.year, date_obj.month, date
        fetch_aw(INTERVALS, year, month, date, rrc)
