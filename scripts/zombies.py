import json
import logging
import os
import pickle
from datetime import datetime

from scripts.constants import *
from scripts.utils import get_utc_timestamp, intervals_str


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


def run_zombies_test(dates, rrc):
    total_zombies = 0
    total_zombie_intervals = {}
    for date in dates:
        date_obj = datetime.strptime(date, "%Y%m%d")
        logging.debug(f"fetching for {date}")
        total_zombie_intervals[date] = {rrc: {}}
        graph_intervals = {date: {rrc: {}}}
        fa = open(f"{GROUPS_FOLDER}/{rrc}/{date_obj.year}.{date_obj.month:02d}/{date}", 'rb')
        updates = pickle.load(fa)
        for announce, withdrawal in zip(announce_times, withdrawal_times):
            start_a, end_a = get_utc_timestamp(date + " " + announce)
            a_updates = [d for d in updates if start_a < d['time'] < end_a]
            prefix_with_peers_a = create_prefix_with_peers_announce(a_updates)

            start_w, end_w = get_utc_timestamp(date + " " + withdrawal)
            w_updates = [d for d in updates if start_w < d['time'] < end_w]
            prefix_with_peers_w = create_prefix_with_peers_withdrawal(w_updates)
            intervals, zombie_intervals, count = collect_zombies(prefix_with_peers_a, prefix_with_peers_w)
            if count:
                logging.debug(f"Found {count} zombies")
                logging.debug(f"Found zombies at intervals: ", zombie_intervals)
                total_zombie_intervals[date][rrc][intervals_str(announce, withdrawal)] = zombie_intervals
            graph_intervals[date][rrc][intervals_str(announce, withdrawal)] = intervals
            total_zombies += count
        os.makedirs(os.path.dirname(f"{OUTPUT_FOLDER}/{rrc}/{date_obj.year}.{date_obj.month}/"), exist_ok=True)
        f = open(f"{OUTPUT_FOLDER}/{rrc}/{date_obj.year}.{date_obj.month}/graph_intervals.{date}", "w")
        json.dump(graph_intervals, f)
        if not total_zombie_intervals[date][rrc]:
            del total_zombie_intervals[date]

    print_zombies(total_zombies, total_zombie_intervals, rrc)


def print_zombies(total_zombies, zombie_intervals, rrc):
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")
    print(f"Total no of zombies: {total_zombies}")
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")
    print(f"RRC: {rrc}")
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")
    print("|\tDATE\t\t|\tPREFIX\t\t\t|\tPEER\t\t\t\t|\tANNOUNCEMENT\t\t|\tANNOUNCE, WITHDRAW\t|")
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")
    for date, values in zombie_intervals.items():
        intervals = values[rrc]
        for exp_interval, prefix_vals in intervals.items():
            for _prefix, peer_vals in prefix_vals.items():
                prefix = _prefix
                for _peer, _announcement in peer_vals.items():
                    peer = _peer
                    ts = int(_announcement[0])
                    announcement = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"|\t{date}\t|\t{prefix}\t|\t{peer}\t|\t{announcement}\t|\t\t{exp_interval}\t\t\t|")
