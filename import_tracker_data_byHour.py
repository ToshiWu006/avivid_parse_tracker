from s3_parser import AmazonS3, TrackingParser
from db import DBhelper
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

@timing
def collectDateHour(date, hour):
    date_time = datetime.datetime.strptime(f"{date} {hour}", "%Y-%m-%d %H")
    s3 = AmazonS3()
    data_list_filter = s3.dumpDateHourDataFilter(date, hour, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")
    return data_list_filter, date_time

# @logging_channels(['clare_test'])
# @timing
# def collectYesterday():
#     date = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(days=1), pattern="%Y-%m-%d")
#     # hour = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(hours=1), pattern="%H")
#     s3 = AmazonS3()
#     data_list_filter = s3.dumpDateDataFilter(date, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")
#     return data_list_filter

## store n_day
@logging_channels(['clare_test'])
def delete_expired_folder(n_day=30):
    root_folder = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(days=n_day), pattern="%Y/%m/%d")
    path_folder = os.path.join(ROOT_DIR, "s3data", root_folder)
    if os.path.exists(path_folder):
        shutil.rmtree(path_folder)
        print(f"remove folder at {path_folder}")
    else:
        print("folder does not exist")

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
    data = DBhelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    web_id_list = [d[0] for d in data]
    return web_id_list


def get_coupon_events_statistics(date, df_sendCoupon, df_acceptCoupon, df_discardCoupon):
    df_list = [df_sendCoupon, df_acceptCoupon, df_discardCoupon]
    columns = ['n_events_sendCoupon', 'n_uuid_sendCoupon',
               'n_events_acceptCoupon', 'n_uuid_acceptCoupon',
                'n_events_discardCoupon', 'n_uuid_discardCoupon']
    web_id_list = []
    for df in df_list:
        if df.shape[0]==0:
            continue
        else:
            web_id_list += list(df['web_id'])
    web_id_list = list(set(web_id_list))
    # web_id_list = list(set(pd.concat([df_sendCoupon['web_id'], df_acceptCoupon['web_id'], df_discardCoupon['web_id']])))
    df_coupon_stat_all = pd.DataFrame()
    for web_id in web_id_list:
        data_dict = {'web_id': web_id, 'date': date}
        for i, df in enumerate(df_list):
            df = df.query(f"web_id=='{web_id}'")
            if df.shape[0] == 0:
                n_events, n_uuid = 0, 0
            else:
                n_events = df.shape[0]
                n_uuid = len(set(df['uuid']))
            data_dict.update({columns[2 * i]: [n_events], columns[2 * i + 1]: [n_uuid]})
        df_coupon_stat_all = pd.concat([df_coupon_stat_all, pd.DataFrame.from_dict(data_dict)])
    return df_coupon_stat_all

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

@logging_channels(['clare_test'], report_args=False)
@timing
def save_tracker_statistics(df_stat):
    if df_stat.shape[0]!=0:
        query = DBhelper.generate_insertDup_SQLquery(df_stat, 'clean_event_stat', df_stat.columns[1:])
        DBhelper('tracker').ExecuteUpdate(query, df_stat.to_dict('records'))
    else:
        print("no available data")


def get_web_id_df(df, web_id):
    if df.shape[0]==0:
        return pd.DataFrame()
    else:
        return df.query(f"web_id=='{web_id}'")


@logging_channels(['clare_test'], report_args=False)
@timing
def save_clean_events(*df_all, event_type_list):
    db = 'tracker'
    for df, event_type in zip(df_all, event_type_list):
        DBhelper.ExecuteUpdatebyChunk(df, db, f"clean_event_{event_type}", chunk_size=100000)


@timing
def get_tracker_statistics_all(date, df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased):
    web_id_list = list(set(df_loaded['web_id']))
    df_stat_all = pd.DataFrame()
    for web_id in web_id_list:
        data_dict = {'web_id': web_id, 'date': date}
        df_list = [df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased]
        columns = ['n_events_load', 'n_uuid_load', 'n_sessions_load',
                   'n_events_leave', 'n_uuid_leave', 'n_sessions_leave',
                   'n_events_timeout', 'n_uuid_timeout', 'n_sessions_timeout',
                   'n_events_addCart', 'n_uuid_addCart', 'n_sessions_addCart',
                   'n_events_removeCart', 'n_uuid_removeCart', 'n_sessions_removeCart',
                   'n_events_purchase', 'n_uuid_purchase', 'n_sessions_purchase'] # please order it!!
        for i, df in enumerate(df_list):
            df = get_web_id_df(df, web_id)
            if df.shape[0] == 0:
                n_events, n_uuid, n_sessions = 0, 0, 0
            else:
                df_sessions = df[['uuid', 'session_id']]
                sessions_list = df_sessions.to_dict("list")
                sessions_set = list(zip(*[sessions_list['uuid'], sessions_list['session_id']]))
                n_events = df.shape[0]
                n_uuid = len(set(df['uuid']))
                n_sessions = len(set(sessions_set))
            data_dict.update({columns[3 * i]: [n_events], columns[3 * i + 1]: [n_uuid], columns[3 * i + 2]: [n_sessions]})
        df_stat = pd.DataFrame.from_dict(data_dict)
        df_stat['n_uuid_load_purchased_before'] = 0 if df_loaded.shape[0] == 0 else len(
            set(df_loaded.query(f"web_id=='{web_id}'").query("is_purchased_before==1")['uuid']))
        df_stat['n_uuid_purchase_purchased_before'] = 0 if df_purchased.shape[0] == 0 else len(
            set(df_purchased.query(f"web_id=='{web_id}'").query("is_purchased_before==1")['uuid']))
        df_stat_all = pd.concat([df_stat_all, df_stat])
    return df_stat_all

@logging_channels(['clare_test'], report_args=False)
@timing
def update_statistics_table(date_utc8):
    event_type_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase', 'sendCoupon', 'acceptCoupon', 'discardCoupon']
    df_list = TrackingParser.get_multiple_df(event_type_list, date_utc8, date_utc8)

    df_stat_all = get_tracker_statistics_all(date_utc8, *df_list[:-3])
    df_coupon_stat_all = get_coupon_events_statistics(date_utc8, *df_list[-3:])
    save_tracker_statistics(df_stat_all)
    save_tracker_statistics(df_coupon_stat_all)
    return df_stat_all, df_coupon_stat_all

def import_tracker_data_byDateHour(date, hour):
    ## load data from s3
    data_list_filter, date_hour = collectDateHour(date, hour)
    # ## test
    # data_list_filter_event = filterListofDictByDict(data_list_filter, dict_criteria={"event_type":'purchase'})

    ## save collection to s3 every hour
    AmazonS3('elephants3').upload_tracker_data(datetime_utc0=date_hour)
    ## save six events to db including drop_duplicates (by web_id)
    date_utc8 = datetime_to_str(date_hour+datetime.timedelta(hours=8))
    ## get all df(11 events) this hour for all web_id
    event_type_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase',
                       'sendCoupon', 'acceptCoupon', 'discardCoupon', 'enterCoupon', 'acceptAf']
    df_hour_list = TrackingParser.get_multiple_df(data_list=data_list_filter, event_type_list=event_type_list)
    ## save 9 events to db
    save_clean_events(*df_hour_list, event_type_list=event_type_list)
    ## statistics
    df_stat_all, df_coupon_stat_all = update_statistics_table(date_utc8)


if __name__ == "__main__":
    ## load data from s3
    data_list_filter, datetime_lastHour = collectLastHour()
    # ## test
    # data_list_filter_event = filterListofDictByDict(data_list_filter, dict_criteria={"event_type":'purchase'})

    ## save collection to s3 every hour
    AmazonS3('elephants3').upload_tracker_data(datetime_utc0=datetime_lastHour)
    ## save six events to db including drop_duplicates (by web_id)
    date_utc8 = datetime_to_str(datetime.datetime.utcnow()+datetime.timedelta(hours=8-1))
    ## get all df(9 events) this hour for all web_id
    event_type_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase',
                       'sendCoupon', 'acceptCoupon', 'discardCoupon', 'enterCoupon', 'acceptAf']
    df_hour_list = TrackingParser.get_multiple_df(data_list=data_list_filter, event_type_list=event_type_list)
    ## save 9 events to db
    save_clean_events(*df_hour_list, event_type_list=event_type_list)
    ## statistics
    df_stat_all, df_coupon_stat_all = update_statistics_table(date_utc8)
    ## delete folder at today_utc0-n
    delete_expired_folder(3)