from s3_parser import AmazonS3, TrackingParser
from db import MySqlHelper
from basic import datetime_to_str, to_datetime, timing, logging_channels, datetime_range, filterListofDictByDict
from definitions import ROOT_DIR
import datetime, os, pickle
import shutil
import pandas as pd


@logging_channels(['clare_test'])
@timing
def collectLastHour():
    datetime_lastHour = datetime.datetime.utcnow()-datetime.timedelta(hours=1)
    date = datetime_to_str(datetime_lastHour, pattern="%Y-%m-%d")
    hour = datetime_to_str(datetime_lastHour, pattern="%H")
    s3 = AmazonS3()
    data_list_filter = s3.dumpDateHourDataFilter(date, hour, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")
    return data_list_filter, datetime_lastHour

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
def delete_expired_folder(n_day=30):
    root_folder = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(days=n_day), pattern="%Y/%m/%d")
    path_folder = os.path.join(ROOT_DIR, "s3data", root_folder)
    if os.path.exists(path_folder):
        shutil.rmtree(path_folder)
        print(f"remove folder at {path_folder}")

    else:
        print("folder does not exist")


def get_data_by_date(date_utc0):
    file_list = get_a_day_file_list(date_utc0)
    data_list = []
    for file in file_list:
        if os.path.isfile(file):
            with open(file, 'rb') as f:
                data_list += pickle.load(f)
    return data_list


def get_a_day_file_list(date_utc0):
    datetime_list = datetime_range(date_utc0, hour_sep=1)[:-1]
    file_list = [os.path.join(ROOT_DIR, "s3data", datetime_to_str(root_folder, pattern="%Y/%m/%d/%H"), "rawData.pickle") for root_folder in datetime_list]
    return file_list

# def get_file_byHour(date_utc0, hour_utc0='00'):
#     """
#
#     Parameters
#     ----------
#     date_utc0: with format, '2022-01-01' or '2022/01/01'
#     hour_utc0: with format, '00'-'23'
#
#     Returns: data_list
#     -------
#
#     """
#
#     path = os.path.join(ROOT_DIR, "s3data", date_utc0.replace('-','/'), hour_utc0, "rawData.pickle")
#     if os.path.isfile(path):
#         with open(path, 'rb') as f:
#             data_list = pickle.load(f)
#     return data_list

@timing
def fetch_enable_analysis_web_id():
    query = f"""SELECT web_id
                        FROM cdp_tracking_settings where enable_analysis=1"""
    print(query)
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    web_id_list = [d[0] for d in data]
    return web_id_list


def get_coupon_events_statistics(date, df_sendCoupon, df_acceptCoupon, df_discardCoupon):
    df_list = [df_sendCoupon, df_acceptCoupon, df_discardCoupon]
    columns = ['n_events_sendCoupon', 'n_uuid_sendCoupon',
               'n_events_acceptCoupon', 'n_uuid_acceptCoupon',
                'n_events_discardCoupon', 'n_uuid_discardCoupon']
    web_id_list = list(set(pd.concat([df_sendCoupon['web_id'], df_acceptCoupon['web_id'], df_discardCoupon['web_id']])))
    df_coupon_stat_all = pd.DataFrame()
    for web_id in web_id_list:
        data_dict = {'web_id': web_id, 'date': date}
        for i, df in enumerate(df_list):
            if df.shape[0] == 0:
                n_events, n_uuid = 0, 0
            else:
                n_events = df.shape[0]
                n_uuid = len(set(df['uuid']))
            data_dict.update({columns[2 * i]: [n_events], columns[2 * i + 1]: [n_uuid]})
        df_coupon_stat_all = pd.concat([df_coupon_stat_all, pd.DataFrame.from_dict(data_dict)])
    return df_coupon_stat_all

@logging_channels(['clare_test'])
@timing
def get_tracker_statistics(web_id, date, df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased):
    data_dict = {'web_id': web_id, 'date': date}
    df_list = [df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased]
    columns = ['n_events_load', 'n_uuid_load', 'n_events_leave', 'n_uuid_leave', 'n_events_timeout',
               'n_uuid_timeout',
               'n_events_addCart', 'n_uuid_addCart', 'n_events_removeCart', 'n_uuid_removeCart',
               'n_events_purchase', 'n_uuid_purchase']
    for i, df in enumerate(df_list):
        if df.shape[0] == 0:
            n_events, n_uuid = 0, 0
        else:
            n_events = df.shape[0]
            n_uuid = len(set(df['uuid']))
        data_dict.update({columns[2 * i]: [n_events], columns[2 * i + 1]: [n_uuid]})
    df_stat = pd.DataFrame.from_dict(data_dict)
    return df_stat

@logging_channels(['clare_test'])
@timing
def save_tracker_statistics(df_stat):
    query = MySqlHelper.generate_insertDup_SQLquery(df_stat, 'clean_event_stat', df_stat.columns[1:])
    MySqlHelper('tracker').ExecuteUpdate(query, df_stat.to_dict('records'))

@logging_channels(['clare_test'])
@timing
def parseSave_sixEvents_collectStat(web_id, date_utc8):
    tracking = TrackingParser(web_id, date_utc8, date_utc8)
    ## add six events df to instance
    df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = tracking.get_six_events_df()
    save_six_clean_events(df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased)
    ## save statistics to table, clean_event_stat
    df_stat = get_tracker_statistics(web_id, date_utc8, df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased)
    df_stat['n_uuid_load_purchased_before'] = 0 if df_loaded.shape[0] == 0 else len(set(df_loaded.query("is_purchased_before==1")['uuid']))
    df_stat['n_uuid_purchase_purchased_before'] = 0 if df_purchased.shape[0] == 0 else len(set(df_purchased.query("is_purchased_before==1")['uuid']))
    return df_stat

@logging_channels(['clare_test'])
@timing
def save_six_clean_events(df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased):
    db = 'tracker'
    ## load events
    MySqlHelper.ExecuteUpdatebyChunk(df_loaded, db, 'clean_event_load', chunk_size=100000)
    ## leave events
    MySqlHelper.ExecuteUpdatebyChunk(df_leaved, db, 'clean_event_leave', chunk_size=100000)
    ## timeout events
    MySqlHelper.ExecuteUpdatebyChunk(df_timeout, db, 'clean_event_timeout', chunk_size=100000)
    ## addCart events
    MySqlHelper.ExecuteUpdatebyChunk(df_addCart, db, 'clean_event_addCart', chunk_size=100000)
    ## removeCart events
    MySqlHelper.ExecuteUpdatebyChunk(df_removeCart, db, 'clean_event_removeCart', chunk_size=100000)
    ## removeCart events
    MySqlHelper.ExecuteUpdatebyChunk(df_purchased, db, 'clean_event_purchase', chunk_size=100000)

@logging_channels(['clare_test'])
@timing
def save_three_clean_coupon_events_toSQL(df_sendCoupon, df_acceptCoupon, df_discardCoupon):
    db = 'tracker'
    ## sendCoupon events
    MySqlHelper.ExecuteUpdatebyChunk(df_sendCoupon, db, 'clean_event_sendCoupon', chunk_size=100000)
    ## acceptCoupon events
    MySqlHelper.ExecuteUpdatebyChunk(df_acceptCoupon, db, 'clean_event_acceptCoupon', chunk_size=100000)
    ## discardCoupon events
    MySqlHelper.ExecuteUpdatebyChunk(df_discardCoupon, db, 'clean_event_discardCoupon', chunk_size=100000)



if __name__ == "__main__":
    ## load data from s3
    data_list_filter, datetime_lastHour = collectLastHour()
    ## save collection to s3 every hour
    AmazonS3('elephants3').upload_tracker_data(datetime_utc0=datetime_lastHour)
    ## save six events to db including drop_duplicates (by web_id)
    web_id_all = fetch_enable_analysis_web_id()
    # web_id_all = ['nineyi11']
    date_utc8 = datetime_to_str(datetime_lastHour)
    df_stat_all = pd.DataFrame()
    for web_id in web_id_all:
        df_stat = parseSave_sixEvents_collectStat(web_id, date_utc8)
        df_stat_all = pd.concat([df_stat_all, df_stat])
    ## save three coupon events to db including drop_duplicates
    df_sendCoupon, df_acceptCoupon, df_discardCoupon = TrackingParser().get_three_coupon_events_df(web_id=None, data_list=data_list_filter, use_db=False)
    save_three_clean_coupon_events_toSQL(df_sendCoupon, df_acceptCoupon, df_discardCoupon)
    ## save to clean_event_stat,
    ## 1. six events statistics
    save_tracker_statistics(df_stat_all)
    ## 2. three coupon events statistics
    df_coupon_stat_all = get_coupon_events_statistics(date_utc8, df_sendCoupon, df_acceptCoupon, df_discardCoupon)
    save_tracker_statistics(df_coupon_stat_all)
    ## delete folder at today_utc0-n
    delete_expired_folder(30)


