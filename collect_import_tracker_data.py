from s3_parser import AmazonS3
from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict
from definitions import ROOT_DIR
from db import MySqlHelper
import datetime, os, pickle, json
import shutil
import pandas as pd

@logging_channels(['clare_test'])
@timing
def collectLastHour():
    date = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y-%m-%d")
    hour = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(hours=1), pattern="%H")
    s3 = AmazonS3()
    data_list_filter = s3.dumpDateHourDataFilter(date, hour, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")
    return data_list_filter

@logging_channels(['clare_test'])
@timing
def collectYesterday():
    date = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(days=1), pattern="%Y-%m-%d")
    # hour = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(hours=1), pattern="%H")
    s3 = AmazonS3()
    data_list_filter = s3.dumpDateDataFilter(date, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")
    return data_list_filter

## store 30days
@logging_channels(['clare_test'])
def delete_expired_folder():
    root_folder = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(days=30), pattern="%Y/%m/%d")
    path_folder = os.path.join(ROOT_DIR, "s3data", root_folder)
    if os.path.exists(path_folder):
        shutil.rmtree(path_folder)
    else:
        print("folder does not exist")


def get_data_by_date(date_utc8):
    file_list = get_a_day_file_list(date_utc8)
    data_list = []
    for file in file_list:
        if os.path.isfile(file):
            with open(file, 'rb') as f:
                data_list += pickle.load(f)
    return data_list


def get_a_day_file_list(date_utc8):
    datetime_utc0 = datetime.datetime.strptime(date_utc8, "%Y-%m-%d") + datetime.timedelta(hours=-8)
    datetime_list = [datetime_utc0 + datetime.timedelta(hours=x) for x in range(24)]
    file_list = [os.path.join(ROOT_DIR, "s3data", datetime_to_str(root_folder, pattern="%Y/%m/%d/%H"), "rawData.pickle") for root_folder in datetime_list]
    return  file_list

@timing
def fetch_parse_key_settings(web_id):
    query = f"""SELECT parsed_purchase_key_level1, parsed_purchase_key_level2, parsed_purchase_key_level3, 
                        parsed_purchase_key_level1_rename, parsed_purchase_key_level2_rename, parsed_purchase_key_level3_rename 
                        FROM cdp_tracking_settings where web_id='{web_id}'"""
    print(query)
    data = MySqlHelper("rheacache-db0").ExecuteSelect(query)
    purchased_key_tuple = data[0]._data[:3]
    purchased_key_rename_tuple = data[0]._data[3:]
    return purchased_key_tuple, purchased_key_rename_tuple


def remove_blank(*args):
    results = []
    for arg in args:
        results += [list(filter(lambda x: x!='_', arg))]
    return results

if __name__ == "__main__":
    # date = "2022-01-10"
    # hour = "11"
    # date = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y-%m-%d")
    # hour = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(hours=1), pattern="%H")
    # s3 = AmazonS3()
    # data_list_filter = s3.dumpDateHourDataFilter("2022-01-10", "09", dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")



    # data_list_filter = collectLastHour()
    # # delete_expired_folder()

    # file_list = get_a_day_file_list("2022-01-10")
    #
    data_list = get_data_by_date("2022-01-11")
    # data_list2 = get_data_by_date("2022-01-11")

    data_list_filter = filterListofDictByDict(data_list, dict_criteria={"event_type":"purchase"})
    # data_list_filter2 = filterListofDictByDict(data_list2, dict_criteria={"event_type":"purchase"})

    #### parse purchase
    columns = ['product_id', 'name', 'variant', 'quantity', 'price', 'total_price', 'shipping_price', 'coupon', 'currency']
    df_purchase = pd.DataFrame(columns=columns)



    # a = data_list_filter[4]
    # dict_purchased = json.loads(a['purchase'])
    # dict_purchased_parsed = {}
    # purchased_key_tuple, purchased_key_rename_tuple = fetch_parse_key_settings('omo')
    # purchased_key_list, purchased_key_rename_list = remove_blank(purchased_key_tuple, purchased_key_rename_tuple)
    # # purchased_key_list, purchased_key_rename_list = list(filter(lambda x: x!='_', purchased_key_tuple))
    # # purchased_key_rename_list = list(filter(lambda x: x!='_', purchased_key_rename_tuple))
    #
    # ## parse purchase object
    # for i,(purchased_key, purchased_key_rename) in enumerate(zip(purchased_key_list, purchased_key_rename_list)):
    #     ## step1: parse first level
    #     if i==0:
    #         purchased_key1_list, purchased_key1_rename_list = purchased_key.split(','), purchased_key_rename.split(',')
    #         # parse and rename key
    #         [dict_purchased_parsed.update({key_rename: dict_purchased[key]}) for key, key_rename in zip(purchased_key1_list, purchased_key1_rename_list)]
    #     ## step2: parse last key of purchased_key_list and remove level2 key
    #     elif i==1:
    #         # delete items and parse in step3
    #         del dict_purchased_parsed[purchased_key]
    #         purchased_key2 = purchased_key_list[1]
    #         if purchased_key2=='items':
    #             items = dict_purchased['items'][0]
    #         else:
    #             items = dict_purchased[purchased_key2]
    #     ## step3: parse 3rd level
    #     elif i==2:
    #         purchased_key3_list, purchased_key3_rename_list = purchased_key.split(','), purchased_key_rename.split(',')
    #         # parse and rename key
    #         [dict_purchased_parsed.update({key_rename: items[key]}) for key, key_rename in zip(purchased_key3_list, purchased_key3_rename_list)]
    #         # for key, key_rename in zip(purchased_key3_list, purchased_key3_rename_list):
    #         #     dict_purchased_parsed.update({key_rename: items[key]})
    #     else:
    #         break
    #



