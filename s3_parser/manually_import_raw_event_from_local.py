from s3_parser import AmazonS3, TrackingParser
from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict, to_datetime
from definitions import ROOT_DIR
import datetime, os, pickle, argparse
import shutil





if __name__ == "__main__":


    parser = argparse.ArgumentParser(description='Import data from local storage')
    parser.add_argument("-dt1", "--datetime1_UTC0", help="starting datetime with formate 2022-01-01 01:00:00")
    parser.add_argument("-dt2", "--datetime2_UTC0", help="ending datetime with formate 2022-01-01 01:00:00")
    args = parser.parse_args()
    dt1, dt2 = args.datetime1_UTC0, args.datetime2_UTC0

    if dt1==None or dt2==None:
        ## default import last hour
        dt1 = datetime.datetime.utcnow()-datetime.timedelta(hours=1)
        dt2 = datetime.datetime.utcnow()-datetime.timedelta(hours=1)
        # ## manually
        dt1, dt2 = '2022-01-19 16:00:00', '2022-01-26 06:00:00' ## UTC+0
        # dt1, dt2 = '2022-01-17 00:00:00', '2022-01-17 23:00:00'
        dt1, dt2 = to_datetime(dt1, "%Y-%m-%d %H:%M:%S"), to_datetime(dt2, "%Y-%m-%d %H:%M:%S")

    delta_hour = int((dt2-dt1).seconds/3600) + (dt2-dt1).days*24
    datetime_list = [dt1 + datetime.timedelta(hours=x) for x in range(delta_hour+1)]
    data_list = []
    for dt in datetime_list:
        data_list += TrackingParser.get_file_byDatetime(dt)
        dt_utc8 = dt + datetime.timedelta(hours=8)
        date_utc8 = datetime.datetime.strftime(dt_utc8, '%Y/%m/%d')
        hour_utc8 = datetime.datetime.strftime(dt_utc8, '%H')
        df_list = TrackingParser.save_raw_event_table(data_list, date_utc8, hour_utc8)
        # print(f"finish saving {dt} into sql table")


