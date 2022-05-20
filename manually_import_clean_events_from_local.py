from s3_parser import TrackingParser
from db import DBhelper
from basic import date_range, to_datetime
from import_tracker_data_byHour import save_six_clean_events, save_clean_events
import datetime, argparse



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Import data from local storage')
    parser.add_argument("-w", "--web_id", help="web_id wanted to be imported to sql, if None, fetch all")
    parser.add_argument("-dt1", "--date1_UTC8", help="starting date with formate 2022-01-01")
    parser.add_argument("-dt2", "--date2_UTC8", help="ending date with formate 2022-01-01")
    parser.add_argument("-e", "--event_type", help="event_type,ex: load, addCart, removeCart, purchase, leave, timeout")

    args = parser.parse_args()
    web_id = args.web_id
    date_start, date_end = args.date1_UTC8, args.date2_UTC8
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
    event_type_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase',
                       'sendCoupon', 'acceptCoupon', 'discardCoupon']
    for date in date_list:
        ## enter web_id and event_type
        if web_id and event_type:
            print(f"date: {date}, input web_id: {web_id} and event_type: {event_type}")
            event_type_list = [event_type]
            df_list = TrackingParser.get_multiple_df(event_type_list, date, date, web_id)
        ## enter web_id only, get all events
        elif web_id and not event_type:
            print(f"date: {date}, input web_id: {web_id} only and event_type: {event_type}")
            df_list = TrackingParser.get_multiple_df(event_type_list, date, date, web_id)
        ## enter event_type only
        elif not web_id and event_type:
            print(f"date: {date}, input web_id: {web_id} and event_type: {event_type} only")
            event_type_list = [event_type]
            df_list = TrackingParser.get_multiple_df(event_type_list, date, date)
        ## both are not entered, get all events and all web_id
        else:
            print(f"date: {date}, not input web_id: {web_id} and event_type: {event_type}")
            df_list = TrackingParser.get_multiple_df(event_type_list, date, date)
        save_clean_events(*df_list, event_type_list=event_type_list)

