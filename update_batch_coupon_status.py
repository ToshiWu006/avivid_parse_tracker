from db import DBhelper
from basic import timing, logging_channels, to_datetime, curdate
import pandas as pd
from definitions import ROOT_DIR

def fetch_running_batch_coupon_activity():
    query = f"""SELECT web_id, link_code FROM web_push.addfan_activity WHERE coupon_code_mode in (1,2) and curdate() between start_time and end_time and activity_enable=1 and coupon_enable=1 and activity_delete=0
    and web_id != 'rick'"""
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    df = pd.DataFrame(data, columns=['web_id', 'link_code'])
    return df

def fetch_accept_coupons(web_id, link_code)->set:
    query = f"SELECT DISTINCT coupon_code FROM tracker.clean_event_acceptCoupon WHERE web_id='{web_id}' and link_code='{link_code}'"
    print(query)
    data = DBhelper("tracker").ExecuteSelect(query)
    coupon_codes = [d[0] for d in data]
    return set(coupon_codes)

def fetch_is_sent_coupon_codes(web_id, link_code)->set:
    query = f"""SELECT coupon_code FROM web_push.addfan_coupon where web_id='{web_id}' and link_code='{link_code}' and is_sent=1 and update_time < date_add(NOW(), INTERVAL -2 HOUR)"""
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    coupon_codes = [d[0] for d in data]
    return set(coupon_codes)

def update_is_sent(web_id, link_code, coupon_code):
    query = f"""update web_push.addfan_coupon set is_sent=1 where web_id='{web_id}' and link_code='{link_code}' and coupon_code='{coupon_code}'"""
    DBhelper("rhea1-db0", is_ssh=True).ExecuteDelete(query)

def build_df_update(link_code:str, coupon_codes:list):
    data = list(zip(*[[link_code]*len(coupon_codes), coupon_codes]))
    df_update = pd.DataFrame(data, columns=['link_code', 'coupon_code'])
    df_update['is_sent'] = [0]*len(coupon_codes)
    return df_update


@logging_channels(['clare_test'], save_local=True, ROOT_DIR=ROOT_DIR)
def main_update_coupon_status(web_id, link_code, is_update=True):
    coupon_codes_accept = fetch_accept_coupons(web_id, link_code)
    coupon_codes_sent = fetch_is_sent_coupon_codes(web_id, link_code)
    coupon_codes_not_accept = coupon_codes_sent.difference(coupon_codes_accept)
    if not list(coupon_codes_not_accept):
        print("no need to update")
        return pd.DataFrame()
    # change is_sent coupon to be available
    df_update = build_df_update(link_code, list(coupon_codes_not_accept))
    if is_update:
        query = DBhelper.generate_insertDup_SQLquery(df_update, 'addfan_coupon', ['is_sent'])
        print(query)
        DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_update.to_dict('records'))
    return df_update

if __name__ == "__main__":
    df_activity = fetch_running_batch_coupon_activity()
    for i, row in df_activity.iterrows():
        web_id, link_code = row
        df_update = main_update_coupon_status(web_id, link_code, is_update=True)