from s3_parser import AmazonS3, TrackingParser
from db import MySqlHelper
from basic import datetime_to_str, to_datetime, timing, logging_channels, datetime_range, filterListofDictByDict
from definitions import ROOT_DIR
import datetime, os, pickle
import shutil


@logging_channels(['clare_test'])
@timing
def collectLastHour():
    datetime_lastHour = datetime.datetime.utcnow()-datetime.timedelta(hours=1)
    date = datetime_to_str(datetime_lastHour, pattern="%Y-%m-%d")
    hour = datetime_to_str(datetime_lastHour, pattern="%H")
    s3 = AmazonS3()
    data_list_filter = s3.dumpDateHourDataFilter(date, hour, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")
    return data_list_filter, datetime_lastHour

# def collectLastHour_cleanData():
#     datetime_lastHour = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
#     # date = datetime_to_str(datetime_lastHour, pattern="%Y-%m-%d")
#     # hour = datetime_to_str(datetime_lastHour, pattern="%H")
#     s3 = AmazonS3()
#
#     root_folder = datetime_to_str(to_datetime(datetime_lastHour, "%Y-%m-%d %H:%M:%S"), pattern="%Y/%m/%d/%H")
#     # sub_folder = datetime_to_str(to_datetime(f'{date}-{hour}', "%Y-%m-%d-%H"), '%H')
#     sub_folder_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase'] ## event_type
#     for sub_folder in sub_folder_list:
#         path_folder = os.path.join(ROOT_DIR, "s3data", root_folder, sub_folder)
#         filename = "cleanData.pickle"
#         df =
#         s3.PickleDump(data_list_filter, path_folder, filename)


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
    ## save three coupon events to db including drop_duplicates
    df_sendCoupon, df_acceptCoupon, df_discardCoupon = TrackingParser().get_three_coupon_events_df(web_id=None, data_list=data_list_filter, use_db=False)
    save_three_clean_coupon_events_toSQL(df_sendCoupon, df_acceptCoupon, df_discardCoupon)
    ## delete folder at today_utc0-n
    delete_expired_folder(30)
