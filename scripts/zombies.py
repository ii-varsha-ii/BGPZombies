import json
import os
import pickle
from datetime import datetime

import pandas as pd

from scripts.constants import *


def create_prefix_with_peers_announce(updates):
    """
    prefix_with_peers = {
        prefix: {
            peer : (latest_announcement, latest_as_path)   # to collect implicit withdrawals
        }
    }
    """
    prefix_with_peers = {}
    for entry in updates:
        if entry['announce']:
            if entry['announce'] in prefix_with_peers:
                prefix = prefix_with_peers[entry['announce']]
                from_ip_as = entry['from_ip'] + ' ' + entry['from_as']
                if from_ip_as not in prefix:
                    prefix[from_ip_as] = (entry['time'], entry['as_path'])
                else:
                    prev_time = prefix[from_ip_as][0]
                    if entry['time'] >= prev_time:
                        prefix[from_ip_as] = (entry['time'], entry['as_path'])
            else:
                prefix_with_peers[entry['announce']] = {
                    entry['from_ip'] + ' ' + entry['from_as']: (entry['time'], entry['as_path'])
                }
    return prefix_with_peers


def create_prefix_with_peers_withdrawal(updates):
    """
    prefix_with_peers = {
        prefix: {
            peer : (latest_withdrawal)
        }
    }
    """
    prefix_with_peers = {}
    for entry in updates:
        if entry['withdraw']:
            if entry['withdraw'] in prefix_with_peers:
                prefix = prefix_with_peers[entry['withdraw']]
                from_ip_as = entry['from_ip'] + ' ' + entry['from_as']
                if from_ip_as not in prefix:
                    prefix[from_ip_as] = (entry['time'])
                else:
                    prev_time = prefix[from_ip_as]
                    if entry['time'] >= prev_time:
                        prefix[from_ip_as] = (entry['time'])
            else:
                prefix_with_peers[entry['withdraw']] = {
                    entry['from_ip'] + ' ' + entry['from_as']: (entry['time'])
                }
    return prefix_with_peers


def get_list_of_peers(prefix_with_peers):
    """
    unique_peers_for_prefix = {
        prefix: [ peer1, peer2, .... peern ],
        ...
    }
    """
    unique_peers_for_prefix = {}
    for prefix, peer in prefix_with_peers.items():
        for k, v in peer.items():
            if prefix not in unique_peers_for_prefix:
                unique_peers_for_prefix[prefix] = [k]
            else:
                unique_peers_for_prefix[prefix].append(k)
    return unique_peers_for_prefix


def collect_zombies(announced, withdrawn):
    """
    intervals = {
        prefix: {
            peer: [announced, withdrawn]
        }
    }
    if not withdrawn, make it 2147483647
    """
    intervals = {}
    zombie_intervals = {}
    count = 0
    for ka, va in announced.items():
        prefix = ka
        intervals[ka] = {}

        w_peers = {}
        if prefix in withdrawn:
            w_peers = withdrawn[prefix]

        for peer, peer_values in va.items():
            # set the latest announcement start time
            intervals[prefix][peer] = [peer_values[0], ]

            if peer in w_peers.keys():
                intervals[prefix][peer].append(w_peers[peer])
            else:
                count += 1
                zombie_intervals[prefix] = {}
                intervals[prefix][peer].append(2147483647)
                zombie_intervals[prefix][peer] = intervals[prefix][peer]
    return intervals, zombie_intervals, count


def run_zombies_test():
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

    total_zombies = 0
    total_zombie_intervals = {}
    for month in list_of_months:
        list_of_dates = [d.strftime('%Y%m%d') for d in pd.date_range(f'{YEAR}{month}{start_date}', f'{YEAR}{month}{end_date}')]
        for date in list_of_dates:
            announce_idx = 0
            withdrawal_idx = 0
            print(f"fetching for {date}")
            total_zombie_intervals[date] = {RRC: {}}
            graph_intervals = {date: {RRC: {}}}
            while announce_idx < len(ANNOUNCEMENT_FILES) and withdrawal_idx < len(WITHDRAWAL_FILES):
                _announcement = ANNOUNCEMENT_FILES[announce_idx]
                _withdrawal = WITHDRAWAL_FILES[withdrawal_idx]
                fa = open(f"{GROUPS_FOLDER}/{RRC}/{YEAR}.{month}/{date}/{_announcement}", 'rb')
                updates = pickle.load(fa)
                print(f"Loaded {_announcement} announce file")
                prefix_with_peers_a = create_prefix_with_peers_announce(updates)

                fw = open(f"{GROUPS_FOLDER}/{RRC}/{YEAR}.{month}/{date}/{_withdrawal}", 'rb')
                updates = pickle.load(fw)
                print(f"Loaded {_withdrawal} withdraw file")
                prefix_with_peers_w = create_prefix_with_peers_withdrawal(updates)
                intervals, zombie_intervals, count = collect_zombies(prefix_with_peers_a, prefix_with_peers_w)
                if count:
                    print(f"Found {count} zombies")
                    print(f"Found zombies at intervals: ", zombie_intervals)
                    total_zombie_intervals[date][RRC][announce_idx] = zombie_intervals
                graph_intervals[date][RRC][announce_idx] = intervals
                total_zombies += count
                announce_idx += 1
                withdrawal_idx += 1
            os.makedirs(os.path.dirname(f"{OUTPUT_FOLDER}/{RRC}/{YEAR}.{month}/"), exist_ok=True)
            f = open(f"{OUTPUT_FOLDER}/{RRC}/{YEAR}.{month}/graph_intervals.{date}", "w")
            json.dump(graph_intervals, f)
            if not total_zombie_intervals[date][RRC]:
                del total_zombie_intervals[date]

    print_zombies(total_zombies, total_zombie_intervals)


def print_zombies(total_zombies, zombie_intervals):
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")
    print(f"Total no of zombies: {total_zombies}")
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")
    print(f"RRC: {RRC}")
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")
    print("|\tDATE\t\t|\tPREFIX\t\t|\tPEER\t\t\t|\tANNOUNCEMENT\t\t|\tEXP WITHDRAWAL\t|")
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")
    for date, values in zombie_intervals.items():
        intervals = values[RRC]
        for idx, prefix_vals in intervals.items():
            exp_withdrawal = WITHDRAWAL_FILES[idx]
            for _prefix, peer_vals in prefix_vals.items():
                prefix = _prefix
                for _peer, _announcement in peer_vals.items():
                    peer = _peer
                    ts = int(_announcement[0])
                    announcement = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"|\t{date}\t|\t{prefix}\t|\t{peer}\t|\t{announcement}\t|\t{exp_withdrawal}\t|")
