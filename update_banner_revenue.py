from s3_parser import TrackingParser
from db import DBhelper
from basic import logging_channels
import collections, datetime, argparse
from collections import defaultdict
import pandas as pd
from sys import exit
from definitions import ROOT_DIR

@logging_channels(['clare_test'], save_local=True, ROOT_DIR=ROOT_DIR)
def prepare_segment_revenues(date, hour, is_save=False) -> list:
    if type(hour) == str:
        hour = int(hour)
    data_list = TrackingParser.get_file_byHour(date, hour, utc=8)
    df_purchase = TrackingParser.get_df(data_list=data_list, event_type='purchase')
    df_click = TrackingParser.get_df_click(web_id=None, data_list=data_list, event_type=None)
    del data_list
    web_id_all = list(set(df_click['web_id']))
    # build click set, (web_id, uuid, session)
    click_dict_set = collections.defaultdict(set)
    for i,row in df_click.iterrows():
        recommend_type = row.get('recommend_type', -1)
        # web_id, uuid, session_id = row['web_id'], row['uuid'], row['session_id']
        web_id, uuid, session_id = row.get('web_id', '_'), row.get('uuid', '_'), row.get('session_id', 0)

        click_dict_set[recommend_type].add((web_id, uuid, session_id))

    # clean purchased list
    purchase_dict_ts = collections.defaultdict(int)
    for i,row in df_purchase.iterrows():
        total_price = max(float(row.get('total_price', 0)), 0)
        product_price = 0 if row.get('product_price', 0) == -1 else float(row.get('product_price', 0))
        product_quantity = max(float(row.get('product_quantity', 0)), 0)
        # web_id, uuid, session_id, ts = row['web_id'], row['uuid'], row['session_id'], row['timestamp']
        web_id, uuid, session_id, ts = row.get('web_id', '_'), row.get('uuid', '_'), row.get('session_id', 0), row.get('timestamp', 0)

        if total_price:
            purchase_dict_ts[(web_id, uuid, session_id, ts)] = total_price
        else: # bad total price, sum up using
            purchase_dict_ts[(web_id, uuid, session_id, ts)] += product_price * product_quantity

    # build revenue map, key:value = (web_id, uuid, session_id):revenue
    purchase_dict = collections.defaultdict(int)
    purchase_set = set()
    for (web_id, uuid, session_id, ts), revenue in purchase_dict_ts.items():
        purchase_dict[(web_id, uuid, session_id)] += revenue
        purchase_set.add((web_id, uuid, session_id))

    # segment revenue, transaction
    revenue_seg = []
    revenue_seg_dict, transaction_seg_dict = defaultdict(int), defaultdict(int)
    for i in range(11): ## 11 types of banner
        banner = purchase_set.intersection(click_dict_set[i]) # (web_id, uuid, session_id)
        res = []
        for key in banner: # (web_id, uuid, session_id)
            rev = purchase_dict[key]
            res.append((key[0], i, rev)) # (web_id, recommend_type, revenue)
            revenue_seg_dict[(key[0], i)] += rev
            transaction_seg_dict[(key[0], i)] += 1
        revenue_seg.extend(res)
        # remove current set in purchase_set
        purchase_set.difference_update(banner)

    # build saved data
    results = [
    {'web_id':web_id, 'recommend_type':i, 'revenue':revenue_seg_dict[(web_id, i)],
    'clicks':df_click.query(f"web_id=='{web_id}' and recommend_type=={i}").shape[0],
    'sessions':len(set(df_click.query(f"web_id=='{web_id}' and recommend_type=={i}")[['uuid', 'session_id']].itertuples(index=False))),
    'transaction':transaction_seg_dict[(web_id, i)],
    'date':date, 'hour':hour} for web_id in web_id_all for i in range(11)
    ]
    results = list(filter(lambda x: x['clicks'] != 0, results))
    df = pd.DataFrame.from_dict(results)

    if is_save and results:
        query = DBhelper.generate_insertDup_SQLquery(df, 'segment_revenues', df.columns)
        DBhelper("roas_report").ExecuteUpdate(query, results)
    return df, df_purchase

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='update segment revenues of each banner from local storage')
    parser.add_argument("-date", "--date", help="date with format '2022-01-01' using utc 8")
    parser.add_argument("-hour", "--hour", help="hour on the date using utc8")
    parser.add_argument("-is", "--is_save", help="update to table or not, format: 'T' or 'F'(None, others)")

    args = parser.parse_args()
    date, hour, is_save = args.date, args.hour, args.is_save
    is_save = True if is_save == 'T' else False
    if not date and not hour:
        # auto mode
        date_time = datetime.datetime.utcnow() + datetime.timedelta(hours=8-1)
        date, hour = datetime.datetime.strftime(date_time, "%Y-%m-%d"), datetime.datetime.strftime(date_time, "%H")
        print(f"no date and hour input, use last one hour: {date} {hour}")
    elif not date or not hour:
        print("Please input both date and hour")
        exit()
    df, df_purchase = prepare_segment_revenues(date, hour, is_save=is_save)
    # df, df_purchase = prepare_segment_revenues(date, 10, is_save=is_save)