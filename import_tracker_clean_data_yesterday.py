from s3_parser import TrackingParser
from basic import datetime_to_str, timing, logging_channels, filterListofDictByDict
from db import MySqlHelper
import datetime
import pandas as pd

@logging_channels(['clare_test'])
@timing
def save_six_clean_events(df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased):
    db = 'tracker'
    ## load events
    MySqlHelper.ExecuteUpdatebyChunk(df_loaded, db, 'clean_event_load', chunk_size=100000)
    ## leave events
    MySqlHelper.ExecuteUpdatebyChunk(df_leaved, db, 'clean_event_leave', chunk_size=100000)
    ## timeout events
    MySqlHelper.ExecuteUpdatebyChunk(df_timeout, db, 'clean_event_timeout', chunk_size=100000)
    ## addCart events
    MySqlHelper.ExecuteUpdatebyChunk(df_addCart, db, 'clean_event_addCart', chunk_size=100000)
    ## removeCart events
    MySqlHelper.ExecuteUpdatebyChunk(df_removeCart, db, 'clean_event_removeCart', chunk_size=100000)
    ## removeCart events
    MySqlHelper.ExecuteUpdatebyChunk(df_purchased, db, 'clean_event_purchase', chunk_size=100000)


@timing
def fetch_enable_analysis_web_id():
    query = f"""SELECT web_id
                        FROM cdp_tracking_settings where enable_analysis=1"""
    print(query)
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    web_id_list = [d[0] for d in data]
    return web_id_list

def get_tracker_statistics(web_id, date, df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased):
    data_dict = {'web_id': web_id, 'date': date}
    df_list = [df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased]
    columns = ['n_events_load', 'n_uuid_load', 'n_events_leave', 'n_uuid_leave', 'n_events_timeout',
               'n_uuid_timeout',
               'n_events_addCart', 'n_uuid_addCart', 'n_events_removeCart', 'n_uuid_removeCart',
               'n_events_purchase', 'n_uuid_purchase']
    for i, df in enumerate(df_list):
        if df.shape[0] == 0:
            n_events, n_uuid = 0, 0
        else:
            n_events = df.shape[0]
            n_uuid = len(set(df['uuid']))
        data_dict.update({columns[2 * i]: [n_events], columns[2 * i + 1]: [n_uuid]})
    df_stat = pd.DataFrame.from_dict(data_dict)
    return df_stat

@logging_channels(['clare_test'])
@timing
def save_tracker_statistics(df_stat):
    # query = MySqlHelper.generate_update_SQLquery(df_stat, 'clean_event_stat')
    query = MySqlHelper.generate_insertDup_SQLquery(df_stat, 'clean_event_stat', df_stat.columns[1:])
    MySqlHelper('tracker').ExecuteUpdate(query, df_stat.to_dict('records'))



@logging_channels(['clare_test'])
@timing
def main_import_tracker(web_id, date_utc8):
    tracking = TrackingParser(web_id, date_utc8, date_utc8)
    ## add six events df to instance
    df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = tracking.get_six_events_df()
    save_six_clean_events(df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased)
    ## save statistics to table, clean_event_stat
    df_stat = get_tracker_statistics(web_id, date_utc8, df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased)
    df_stat['n_uuid_load_purchased_before'] = 0 if df_loaded.shape[0] == 0 else len(set(df_loaded.query("is_purchased_before==1")['uuid']))
    df_stat['n_uuid_purchase_purchased_before'] = 0 if df_purchased.shape[0] == 0 else len(set(df_purchased.query("is_purchased_before==1")['uuid']))
    save_tracker_statistics(df_stat)
    return df_stat




## daily import at 02:00 UTC+8
## import six clean events and import statistics of clean events
if __name__ == "__main__":

    date_utc8 = datetime_to_str((datetime.datetime.utcnow()+datetime.timedelta(hours=8)-datetime.timedelta(days=1)).date())
    # date_utc8 = '2022-02-21'
    web_id_all = fetch_enable_analysis_web_id()
    # web_id_all = ['nineyi40269']
    for web_id in web_id_all:
        df_stat = main_import_tracker(web_id, date_utc8)


    # ## test
    # tracking = TrackingParser('lovingfamily', '2022-02-11', '2022-02-11')
    # df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = tracking.get_six_events_df()
    # data_filter = filterListofDictByDict(tracking.data_list, {'web_id':'lovingfamily', 'event_type':'addCart'})
    # df = tracking.get_df('lab52', tracking.data_list, 'load')


    #product_id,product_price,product_category,product_category_name,product_name,product_quantity
    #ecomm_prodid,ecomm_totalvalue,empty,empty,empty,empty,empty

    #product_category,product_category_name,product_id,product_name,product_price,product_quantity