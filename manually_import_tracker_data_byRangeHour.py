from s3_parser import AmazonS3
from basic import datetime_to_str, timing, logging_channels, datetime_range, to_datetime
from definitions import ROOT_DIR
import datetime, os, pickle
import shutil
import argparse
import pandas as pd
from import_tracker_data_byHour import fetch_enable_analysis_web_id, parseSave_sixEvents_collectStat



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Import data from s3')
    parser.add_argument("-dt1", "--datetime1_UTC0", help="starting date with formate 2022-01-01 8:00:00")
    parser.add_argument("-dt2", "--datetime2_UTC0", help="ending date with formate 2022-01-01 10:00:00")
    args = parser.parse_args()
    datetime_start = to_datetime(args.datetime1_UTC0, pattern='%Y-%m-%d %H:%M:%S')
    datetime_end = to_datetime(args.datetime2_UTC0, pattern='%Y-%m-%d %H:%M:%S')

    n_hour = int((datetime_end - datetime_start).seconds/3600)
    dt_list = [datetime_start + datetime.timedelta(hours=1*x) for x in range(n_hour+1)]

    for dt in dt_list:
        ## load data from s3
        date = datetime_to_str(dt, pattern="%Y-%m-%d")
        hour = datetime_to_str(dt, pattern="%H")
        s3 = AmazonS3()
        data_list_filter = s3.dumpDateHourDataFilter(date, hour, dict_criteria={'event_type': None, 'web_id': None},
                                                     pattern="%Y-%m-%d")
        ## save collection to s3 every hour
        AmazonS3('elephants3').upload_tracker_data(datetime_utc0=dt)
        ## save six events to db including drop_duplicates (by web_id)
        web_id_all = fetch_enable_analysis_web_id()
        # web_id_all = ['draimior']
        date_utc8 = datetime_to_str(dt)
        for web_id in web_id_all:
            parseSave_sixEvents_collectStat(web_id, date_utc8, data_list_filter)