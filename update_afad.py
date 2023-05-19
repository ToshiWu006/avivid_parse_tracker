from db import DBhelper
from basic import timing
import pandas as pd



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
def fetch_afad_activity_by_id(ids):
    query = f"""
    SELECT id, web_id, start_time, date_add(end_time,INTERVAL 1 DAY) as end_time
    FROM addfan_activity WHERE id in ('{"','".join(list(map(str, ids)))}')
    and web_id != 'rick'    
    """
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'activity_start', 'activity_end']
    df_afad = pd.DataFrame(data, columns=columns)
    return df_afad

@timing
def fetch_afad_activity_just_expired():
    query = f"""SELECT id, web_id, start_time, date_add(end_time,INTERVAL 1 DAY) as end_time2
                FROM addfan_activity WHERE DATE(update_time) between start_time and end_time 
                and DATEDIFF(curdate(), end_time) between 0 and 1
                and activity_enable=1 and ad_enable=1 and activity_delete=0
                and web_id != 'rick'"""
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'activity_start', 'activity_end']
    df_afad = pd.DataFrame(data, columns=columns)
    return df_afad

def check_ad_equal_af(web_id, ad_id):
    query = f"""SELECT id FROM addfan_activity
                WHERE web_id='{web_id}' AND id='{ad_id}' AND ad_url = ad_btn_url"""
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    return bool(data)

@timing
def fetch_afad_count(web_id, ad_id, activity_start, activity_end):
    query = f"""SELECT count(distinct uuid) as addfan_growth FROM tracker.clean_event_acceptAf
                WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                AND web_id='{web_id}' AND ad_id='{ad_id}'"""
    data = DBhelper("tracker").ExecuteSelect(query)
    df_count = pd.DataFrame(data, columns=['addfan_growth'])
    if check_ad_equal_af(web_id, ad_id):
        query = f"""SELECT count(distinct uuid) as addfan_growth FROM tracker.clean_event_acceptAd
                        WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                        AND web_id='{web_id}' AND ad_id='{ad_id}'"""
        data = DBhelper("tracker").ExecuteSelect(query)
        df_count['addfan_growth'] += data[0].addfan_growth
    query = f"""SELECT count(*) as addfan_impression FROM tracker.clean_event_sendAfAd
                WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                AND web_id='{web_id}' AND ad_id='{ad_id}'"""
    data = DBhelper("tracker").ExecuteSelect(query)
    df_count = pd.concat([df_count, pd.DataFrame(data, columns=['addfan_impression'])], axis=1)

    df_count['id'] = [ad_id] * df_count.shape[0]
    return df_count


if __name__ == "__main__":
    ## update addfan in running
    df_afad = fetch_afad_activity_running()
    df_count_all = pd.DataFrame()
    for i, row in df_afad.iterrows():
        ad_id, web_id, activity_start, activity_end = row
        df_count = fetch_afad_count(web_id, ad_id, activity_start, activity_end)
        df_count_all = pd.concat([df_count_all, df_count])
    ## update to table
    query = DBhelper.generate_insertDup_SQLquery(df_count_all, 'addfan_activity', ['addfan_growth', 'addfan_impression'])
    DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_count_all.to_dict('records'))

    ## update addfan just expired whithin a day
    df_afad_just_expired = fetch_afad_activity_just_expired()
    df_count_expired_all = pd.DataFrame()
    for i, row in df_afad_just_expired.iterrows():
        ad_id, web_id, activity_start, activity_end = row
        df_count = fetch_afad_count(web_id, ad_id, activity_start, activity_end)
        df_count_expired_all = pd.concat([df_count_expired_all, df_count])
    ## update to table
    query = DBhelper.generate_insertDup_SQLquery(df_count_expired_all, 'addfan_activity', ['addfan_growth'])
    DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_count_expired_all.to_dict('records'))


    # manually update addfan in running
    # df_afad = fetch_afad_activity_by_id([549,559,566,578,586,590,591,594,598,599,602,603,606,607,609,610,611,613,621,626,627,638,642,645,652,655,663,668,673,677,679,685,696])
