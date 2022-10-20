from s3_parser import TrackingParser
from db import DBhelper
from basic import logging_channels
import collections, datetime, argparse
from collections import defaultdict
import pandas as pd
from definitions import ROOT_DIR


def fetch_users(date)->dict:
    query = f"""SELECT web_id, n_uuid_load FROM tracker.clean_event_stat where date='{date}'"""
    data = DBhelper("tracker").ExecuteSelect(query)
    res = defaultdict(int)
    for web_id, users in data:
        res[web_id] = users
    return res


@logging_channels(['clare_test'], save_local=True, ROOT_DIR=ROOT_DIR)
def prepare_daily_segment_revenues(date, is_save=False) -> list:
    df_purchase = TrackingParser.get_df(date_utc8_start=date, date_utc8_end=date, event_type='purchase')
    users_dict = fetch_users(date)
    web_id_all = list(set(users_dict.keys()))

    # clean purchased list
    purchase_dict_ts = collections.defaultdict(int)
    for i, row in df_purchase.iterrows():
        total_price = max(float(row['total_price']), 0)
        product_price = 0 if row['product_price'] == -1 else float(row['product_price'])
        product_quantity = max(float(row['product_quantity']), 0)
        web_id, uuid, session_id, ts = row['web_id'], row['uuid'], row['session_id'], row['timestamp']
        if total_price:
            purchase_dict_ts[(web_id, uuid, session_id, ts)] = total_price
        else:  # bad total price, sum up using
            purchase_dict_ts[(web_id, uuid, session_id, ts)] += product_price * product_quantity

    # build revenue map, key:value = (web_id, uuid, session_id):revenue
    total_revenue_dict, transactions_dict = defaultdict(int), defaultdict(int)
    for (web_id, uuid, session_id, ts), revenue in purchase_dict_ts.items():
        total_revenue_dict[web_id] += revenue
        transactions_dict[web_id] += 1

    results = [
        {'web_id': web_id, 'total_revenue': total_revenue_dict[web_id],
         'users': users_dict[web_id], 'transactions': transactions_dict[web_id], 'date': date}
        for web_id in web_id_all]
    df = pd.DataFrame.from_dict(results)

    if is_save and results:
        query = DBhelper.generate_insertDup_SQLquery(df, 'website_total_segment_revenue', df.columns)
        DBhelper("roas_report").ExecuteUpdate(query, results)
    return df, df_purchase

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='update segment revenues of each banner from local storage')
    parser.add_argument("-date", "--date", help="date with format '2022-01-01' using utc 8")
    parser.add_argument("-is", "--is_save", help="update to table or not, format: 'T'(None) or 'F'(others)")

    args = parser.parse_args()
    date, is_save = args.date, args.is_save
    is_save = True if is_save == 'T' or not is_save else False
    if not date:
        # auto mode
        date_time = datetime.datetime.utcnow() + datetime.timedelta(hours=8-24)
        date = datetime.datetime.strftime(date_time, "%Y-%m-%d")
        print(f"no date input, use yesterday: {date}")
    df, df_purchase = prepare_daily_segment_revenues(date, is_save=is_save)
