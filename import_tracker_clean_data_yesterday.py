from s3_parser import TrackingParser
from basic import datetime_to_str, timing, logging_channels
from db import MySqlHelper
import datetime
import pandas as pd

@logging_channels(['clare_test'])
@timing
def save_six_clean_events(tracking_obj):
    db = 'tracker'
    ## load events
    MySqlHelper.ExecuteUpdatebyChunk(tracking_obj.df_loaded, db, 'clean_event_load', chunk_size=100000)
    ## leave events
    MySqlHelper.ExecuteUpdatebyChunk(tracking_obj.df_leaved, db, 'clean_event_leave', chunk_size=100000)
    ## timeout events
    MySqlHelper.ExecuteUpdatebyChunk(tracking_obj.df_timeout, db, 'clean_event_timeout', chunk_size=100000)
    ## addCart events
    MySqlHelper.ExecuteUpdatebyChunk(tracking_obj.df_addCart, db, 'clean_event_addCart', chunk_size=100000)
    ## removeCart events
    MySqlHelper.ExecuteUpdatebyChunk(tracking_obj.df_removeCart, db, 'clean_event_removeCart', chunk_size=100000)
    ## removeCart events
    MySqlHelper.ExecuteUpdatebyChunk(tracking_obj.df_purchased, db, 'clean_event_purchase', chunk_size=100000)


@timing
def fetch_enable_analysis_web_id():
    query = f"""SELECT web_id
                        FROM cdp_tracking_settings where enable_analysis=1"""
    print(query)
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    web_id_list = [d[0] for d in data]
    return web_id_list

## daily import at 02:00 UTC+8
if __name__ == "__main__":

    date_utc8 = datetime_to_str((datetime.datetime.utcnow()+datetime.timedelta(hours=8)-datetime.timedelta(days=1)).date())
    web_id_all = fetch_enable_analysis_web_id()
    # web_id_all = ['lovingfamily']
    for web_id in web_id_all:
        tracking = TrackingParser(web_id, date_utc8, date_utc8)
        # tracking = TrackingParser(web_id, '2022-02-01', '2022-02-07')
        save_six_clean_events(tracking)
