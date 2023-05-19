from s3_parser import TrackingParser
from db import DBhelper
from basic import date_range, to_datetime
from import_tracker_data_byHour import save_clean_events, save_tracker_statistics, get_tracker_statistics_all, get_coupon_events_statistics
import datetime, argparse



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Import data from local storage')
    parser.add_argument("-w", "--web_id", help="web_id wanted to be imported to sql, if None, fetch all")
    parser.add_argument("-dt1", "--date1_UTC8", help="starting date with formate 2022-01-01")
    parser.add_argument("-dt2", "--date2_UTC8", help="ending date with formate 2022-01-01")
    parser.add_argument("-e", "--event_type", help="event_type,ex: load, addCart, removeCart, purchase, leave, timeout")

    args = parser.parse_args()
    web_id = args.web_id
    date_start = '-'.join('0' + i if len(i) == 1 else i for i in args.date1_UTC8.split('-'))
    date_end = args.date2_UTC8 if args.date2_UTC8 else date_start
    event_type = args.event_type
    print(f"import events form local, web_id: {web_id} and date from '{date_start}' to '{date_end}'")

    if date_start==None or date_end==None:
        ## default import last hour
        date_start = datetime.datetime.utcnow() + datetime.timedelta(days=-1, hours=8)
        date_end = datetime.datetime.utcnow() + datetime.timedelta(days=-1, hours=8)
        ## to str
        date_start, date_end = datetime.datetime.strftime(date_start, "%Y-%m-%d"), datetime.datetime.strftime(date_end, "%Y-%m-%d")
        # tracking = TrackingParser(web_id, date_utc8, date_utc8)
    num_days = (to_datetime(date_end) - to_datetime(date_start)).days+1
    date_list = date_range(date_start, num_days=num_days)
    event_type_list = event_type.split(',') if event_type else ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase',
                                                       'sendCoupon', 'acceptCoupon', 'discardCoupon', 'enterCoupon',
                                                       'sendAfAd', 'acceptAf', 'acceptAd']
    for date in date_list:
        print(f"date: {date}, input web_id: {web_id} and event_type: {event_type}")
        ## enter web_id and event_type
        df_list = TrackingParser.get_multiple_df(event_type_list, date, date, web_id)
        save_clean_events(*df_list, event_type_list=event_type_list)
        if not event_type:
            save_tracker_statistics(get_tracker_statistics_all(date, *df_list[:6]))
            save_tracker_statistics(get_coupon_events_statistics(date, *df_list[6:9]))
