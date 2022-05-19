from s3_parser import TrackingParser
from db import DBhelper
from import_tracker_data_byHour import save_six_clean_events
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

    if date_start == None or date_end == None:
        ## default import last hour
        date_start = datetime.datetime.utcnow() + datetime.timedelta(days=-1, hours=8)
        date_end = datetime.datetime.utcnow() + datetime.timedelta(days=-1, hours=8)
        date_start, date_end = datetime.datetime.strftime(date_start, "%Y-%m-%d"), datetime.datetime.strftime(date_end, "%Y-%m-%d")
        # tracking = TrackingParser(web_id, date_utc8, date_utc8)

    if web_id==None:
        tracking = TrackingParser(date_start, date_end)
        df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = tracking.get_six_events_df_all(tracking.data_list)  # get_six_events_df_all

    else:
        tracking = TrackingParser(web_id, date_start, date_end)
        ## add six events df to instance
        df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = tracking.get_six_events_df()

    if event_type==None:
        ## import all events
        save_six_clean_events(df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased)
    else:
        db = 'tracker'
        if event_type=='load':
            ## load events
            DBhelper.ExecuteUpdatebyChunk(df_loaded, db, 'clean_event_load', chunk_size=100000)
        elif event_type=='leave':
            ## leave events
            DBhelper.ExecuteUpdatebyChunk(df_leaved, db, 'clean_event_leave', chunk_size=100000)
        elif event_type == 'timeout':
            ## timeout events
            DBhelper.ExecuteUpdatebyChunk(df_timeout, db, 'clean_event_timeout', chunk_size=100000)
        elif event_type == 'addCart':
            ## addCart events
            DBhelper.ExecuteUpdatebyChunk(df_addCart, db, 'clean_event_addCart', chunk_size=100000)
        elif event_type == 'removeCart':
            ## removeCart events
            DBhelper.ExecuteUpdatebyChunk(df_removeCart, db, 'clean_event_removeCart', chunk_size=100000)
        elif event_type == 'purchase':
            ## removeCart events
            DBhelper.ExecuteUpdatebyChunk(df_purchased, db, 'clean_event_purchase', chunk_size=100000)


