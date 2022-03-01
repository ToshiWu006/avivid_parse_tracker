from db import MySqlHelper
from basic import datetime_to_str, timing, logging_channels
import pandas as pd

@timing
def fetch_coupon_activity_running():
    query = f"""
    SELECT id, web_id, link_code, coupon_limit, coupon_type, coupon_amount, start_time, end_time
    FROM addfan_activity WHERE curdate() between start_time and end_time and activity_enable=1 and coupon_enable=1
    and web_id != 'rick'    
    """
    print(query)
    data = MySqlHelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'link_code', 'coupon_limit', 'coupon_type', 'coupon_amount', 'activity_start', 'activity_end']
    coupon_price_limit = [int(d[3].split('limit-bill=')[1]) if d[3].find('limit-bill=')!=-1 else 0 for d in data]
    df_coupon = pd.DataFrame(data, columns=columns).drop(columns=['coupon_limit'])
    df_coupon['coupon_price_limit'] = coupon_price_limit
    return df_coupon


@timing
def fetch_coupon_cost(web_id, coupon_type, coupon_amount):
    """

    Parameters
    ----------
    coupon_type: 折扣類型 {0:無設定 1:免運 2:元 3:% 4:n送n}
    coupon_amount: fill blank

    Returns
    -------

    """
    if coupon_type==1:
        query = f"SELECT avg_shipping_price FROM addfan_stat WHERE web_id='{web_id}'"
        print(query)
        coupon_cost = int(MySqlHelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)[0][0])
    elif coupon_type==2:
        coupon_cost = int(coupon_amount)
    elif coupon_type==3:
        query = f"SELECT avg_total_price FROM addfan_stat WHERE web_id='{web_id}'"
        print(query)
        ## coupon_amount must between 0 and 10
        coupon_cost = float(MySqlHelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)[0][0])*(10-coupon_amount)/10
    else:
        coupon_cost = 100
    return coupon_cost


@timing
def fetch_update_revenue_cost_n_coupon(coupon_id, coupon_cost, activity_start, activity_end):
    query = f"""
    SELECT 
        sum(temp.total_price) as revenue, COUNT(temp.total_price)*{coupon_cost} as cost
    FROM
        (SELECT 
            a.uuid AS uuid,
                b.session_id AS session_b,
                a.link_code,
                AVG(b.total_price) AS total_price
        FROM
            tracker.clean_event_acceptCoupon a
        INNER JOIN tracker.clean_event_purchase b ON a.uuid = b.uuid
            AND a.session_id = b.session_id
            AND b.total_price > {coupon_price_limit}
        WHERE
            a.link_code = '{link_code}'
        GROUP BY uuid , session_b) AS temp        
    """
    print(query)
    data = MySqlHelper("tracker").ExecuteSelect(query)
    df_ROAS = pd.DataFrame(data, columns=['revenue', 'cost'])
    df_ROAS['id'] = [coupon_id]
    coupon_sent, coupon_accept = fetch_n_coupon_sent_accept(coupon_id, activity_start, activity_end)
    df_ROAS[['coupon_sent', 'coupon_accept']] = [[coupon_sent, coupon_accept]]
    df_ROAS['coupon_used'] = [int(df_ROAS['cost']/coupon_cost)]
    update_col = ['revenue', 'cost', 'coupon_sent', 'coupon_accept', 'coupon_used']
    query_update = MySqlHelper.generate_insertDup_SQLquery(df_ROAS, 'addfan_activity', update_col)
    MySqlHelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query_update, df_ROAS.to_dict('records'))
    return df_ROAS

@timing
def fetch_n_coupon_sent_accept(coupon_id, activity_start, activity_end):
    query_sent = f"""
    SELECT
        COUNT(id) AS coupon_sent
    FROM
        tracker.clean_event_sendCoupon
    WHERE
        coupon_id = {coupon_id}
        AND date_time BETWEEN '{activity_start}' AND '{activity_end}'
    """
    print(query_sent)
    query_accept = f"""
    SELECT
        COUNT(id) AS coupon_accept
    FROM
        tracker.clean_event_acceptCoupon
    WHERE
        coupon_id = {coupon_id}
        AND date_time BETWEEN '{activity_start}' AND '{activity_end}'
    """
    print(query_accept)
    data_sent = MySqlHelper("tracker").ExecuteSelect(query_sent)
    data_accept = MySqlHelper("tracker").ExecuteSelect(query_accept)
    coupon_sent, coupon_accept = data_sent[0][0], data_accept[0][0]
    return coupon_sent, coupon_accept


@logging_channels(['clare_test'])
def main_update_addFan_ROAS(web_id, coupon_id, coupon_type, coupon_amount, activity_start, activity_end):
    coupon_cost = fetch_coupon_cost(web_id, coupon_type, coupon_amount)
    df_ROAS = fetch_update_revenue_cost_n_coupon(coupon_id, coupon_cost, activity_start, activity_end)
    return df_ROAS

if __name__ == "__main__":
    df_coupon = fetch_coupon_activity_running()
    for i,row in df_coupon.iterrows():
        coupon_id, web_id, link_code, coupon_type, coupon_amount, activity_start, activity_end, coupon_price_limit = row
        df_ROAS = main_update_addFan_ROAS(web_id, coupon_id, coupon_type, coupon_amount, activity_start, activity_end)

        # coupon_sent, coupon_accept = fetch_n_coupon_sent_accept(coupon_id, activity_start, activity_end)
        # coupon_cost = fetch_coupon_cost(web_id, coupon_type, coupon_amount)
        # df_ROAS = fetch_update_revenue_cost(coupon_id)


