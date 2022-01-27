from s3_parser import AmazonS3, TrackingParser
from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict
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

if __name__ == "__main__":
    ## load data from s3
    data_list_filter, datetime_lastHour = collectLastHour()

    ## save collection to s3 every hour
    AmazonS3('elephants3').upload_tracker_data(datetime_utc0=datetime_lastHour)

    ## save to db
    # datetime_utc8 = datetime_lastHour+datetime.timedelta(hours=8)
    # date_utc8 = datetime.datetime.strftime(datetime_utc8, '%Y/%m/%d')
    # hour_utc8 = datetime.datetime.strftime(datetime_utc8, '%H')
    # TrackingParser.save_raw_event_table(data_list_filter, date_utc8, hour_utc8)

    ## delete folder at today_utc0-n
    delete_expired_folder(30)
