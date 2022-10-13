from s3_parser import AmazonS3
from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict
from definitions import ROOT_DIR
import datetime, os, pickle
import argparse
from import_tracker_data_byHour import import_tracker_data_byDateHour


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
    parser = argparse.ArgumentParser(description='Import and download data from elephant s3')
    parser.add_argument("-dt1", "--date1_UTC0", help="starting datetime with formate 2022-01-01 00:00:00")
    parser.add_argument("-dt2", "--date2_UTC0", help="ending datetime with formate 2022-01-01 10:00:00")
    args = parser.parse_args()
    datetime_start, datetime_end = args.date1_UTC0, args.date2_UTC0
    datetime_start = datetime.datetime.strptime(datetime_start, "%Y-%m-%d %H:%M:%S")
    datetime_end = datetime.datetime.strptime(datetime_end, "%Y-%m-%d %H:%M:%S")

    print(f"import events from s3, datetime from '{datetime_start}' to '{datetime_end}'")
    n_hours = (datetime_end - datetime_start).seconds//3600 + 1
    for date_time in [datetime_start + datetime.timedelta(hours=dh) for dh in range(n_hours)]:
        date, hour = datetime.datetime.strftime(date_time, "%Y-%m-%d"), datetime.datetime.strftime(date_time, "%H")
        print(date, hour)
        ## 11 events
        ## ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase', 'sendCoupon', 'acceptCoupon', 'discardCoupon', 'enterCoupon', 'acceptAf']
        import_tracker_data_byDateHour(date, hour)