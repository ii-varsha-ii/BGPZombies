from mrtparse import *

from scripts.constants import PREFIX
from scripts.models import MRT_TYPES, TABLE_DUMP_V2_SUBTYPES


class BGPDump:
    pass


if __name__ == '__main__':

    file_name = '../dumps/rrc06/2017.09/bview.20170901.0800'
    try:
        m = Reader(file_name)
        print(m)
    except FileNotFoundError as e:
        print(e)
    for record in m:
        if MRT_TYPES.TABLE_DUMP_V2.value in record.data['type'] \
                and TABLE_DUMP_V2_SUBTYPES.IPV4_UNICAST.value in record.data['subtype']:
            data = record.data
            print(data['prefix']+'/24')
            if data['prefix'] + '/24' in PREFIX:
                print(record)