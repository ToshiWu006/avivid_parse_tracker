import argparse, datetime
from import_tracker_data_byHour import import_tracker_data_byDateHour

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import and download data from elephant s3')
    parser.add_argument("-dt1", "--date1_UTC0", help="starting datetime with formate 2022-01-01 00:00:00")
    parser.add_argument("-dt2", "--date2_UTC0", help="ending datetime with formate 2022-01-01 10:00:00")
    args = parser.parse_args()
    datetime_start, datetime_end = args.date1_UTC0, args.date2_UTC0
    datetime_start = datetime.datetime.strptime(datetime_start, "%Y-%m-%d %H:%M:%S")
    datetime_end = datetime.datetime.strptime(datetime_end, "%Y-%m-%d %H:%M:%S")
    print(f"import events from s3, datetime from '{datetime_start}' to '{datetime_end}'")
    dt = (datetime_end - datetime_start)
    n_hours = dt.days * 24 + dt.seconds//3600 + 1
    for date_time in [datetime_start + datetime.timedelta(hours=dh) for dh in range(n_hours)]:
        date, hour = datetime.datetime.strftime(date_time, "%Y-%m-%d"), datetime.datetime.strftime(date_time, "%H")
        print(date, hour)
        ## 11 events
        ## ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase', 'sendCoupon', 'acceptCoupon', 'discardCoupon', 'enterCoupon', 'acceptAf']
        import_tracker_data_byDateHour(date, hour)