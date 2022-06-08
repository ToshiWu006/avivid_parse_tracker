from db import DBhelper
from basic import timing, logging_channels, to_datetime, curdate
import pandas as pd
from definitions import ROOT_DIR


@timing
def fetch_afad_activity_running():
    query = f"""
    SELECT id, web_id, start_time, date_add(end_time,INTERVAL 1 DAY) as end_time
    FROM addfan_activity WHERE curdate() between start_time and end_time and activity_enable=1 and ad_enable=1 and activity_delete=0
    and web_id != 'rick'    
    """
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'activity_start', 'activity_end']
    df_afad = pd.DataFrame(data, columns=columns)
    return df_afad


@timing
def fetch_afad_activity_just_expired():
    query = f"""
    SELECT id, web_id, start_time, date_add(end_time,INTERVAL 1 DAY) as end_time2
    FROM addfan_activity WHERE DATE(update_time) between start_time and end_time 
    and DATEDIFF(curdate(), end_time) between 0 and 1
    and activity_enable=1 and ad_enable=1 and activity_delete=0
    and web_id != 'rick'
    """
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'activity_start', 'activity_end']
    df_afad = pd.DataFrame(data, columns=columns)
    return df_afad


@timing
def fetch_af_count(web_id, ad_id, activity_start, activity_end):
    query = f"""
    SELECT count(distinct uuid) as addfan_growth FROM tracker.clean_event_acceptAf
    WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
    AND web_id='{web_id}' AND ad_id='{ad_id}'    
    """
    data = DBhelper("tracker").ExecuteSelect(query)
    df_count = pd.DataFrame(data, columns=['addfan_growth'])
    df_count['id'] = [ad_id] * df_count.shape[0]
    return df_count


if __name__ == "__main__":
    ## update addfan in running
    df_afad = fetch_afad_activity_running()
    df_count_all = pd.DataFrame()
    for i, row in df_afad.iterrows():
        ad_id, web_id, activity_start, activity_end = row
        df_count = fetch_af_count(web_id, ad_id, activity_start, activity_end)
        df_count_all = pd.concat([df_count_all, df_count])
    ## update to table
    query = DBhelper.generate_insertDup_SQLquery(df_count_all, 'addfan_activity', ['addfan_growth'])
    DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_count_all.to_dict('records'))

    ## update addfan just expired whithin a day
    df_afad_just_expired = fetch_afad_activity_just_expired()
    df_count_expired_all = pd.DataFrame()
    for i, row in df_afad_just_expired.iterrows():
        ad_id, web_id, activity_start, activity_end = row
        df_count = fetch_af_count(web_id, ad_id, activity_start, activity_end)
        df_count_expired_all = pd.concat([df_count_expired_all, df_count])
    ## update to table
    query = DBhelper.generate_insertDup_SQLquery(df_count_expired_all, 'addfan_activity', ['addfan_growth'])
    DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_count_expired_all.to_dict('records'))




