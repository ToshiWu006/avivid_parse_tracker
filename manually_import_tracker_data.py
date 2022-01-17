from s3_parser import AmazonS3
from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict
from definitions import ROOT_DIR
import datetime, os, pickle
import shutil
import argparse


@logging_channels(['clare_test'])
@timing
def collectByDateHour(date, hour):
    # date = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(days=1), pattern="%Y-%m-%d")
    # hour = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(hours=1), pattern="%H")
    s3 = AmazonS3()
    data_list_filter = s3.dumpDateHourDataFilter(date, hour, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")
    return data_list_filter


@logging_channels(['clare_test'])
@timing
def collectLastHour():
    date = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(hours=1), pattern="%Y-%m-%d")
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
    return  file_list


if __name__ == "__main__":
    # date = "2022-01-10"
    # hour = "11"
    parser = argparse.ArgumentParser(description='Download data from elephant s3')
    parser.add_argument("-date", "--date_UTC0", help="date with formate 2022-01-01")
    parser.add_argument("-hour", "--hour_UTC0", help="hour with formate 05")
    args = parser.parse_args()
    date, hour = args.date_UTC0, args.hour_UTC0
    collectByDateHour(date, hour)

    # date = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y-%m-%d")
    # hour = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(hours=1), pattern="%H")
    # s3 = AmazonS3()
    # data_list_filter = s3.dumpDateHourDataFilter("2022-01-10", "09", dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d")



    # data_list_filter = collectLastHour()
    # # delete_expired_folder()

    # file_list = get_a_day_file_list("2022-01-10")

    # data_list = get_data_by_date("2022-01-10")

    # data_list_filter = filterListofDictByDict(data_list, dict_criteria={"event_type": "removeCart"})