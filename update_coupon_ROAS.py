from db import MySqlHelper
from basic import datetime_to_str, timing, logging_channels, to_datetime, curdate
import pandas as pd
import datetime
@timing
def fetch_coupon_activity_running():
    query = f"""
    SELECT id, web_id, link_code, coupon_limit, coupon_type, coupon_amount, 
    start_time, date_add(end_time,INTERVAL 1 DAY) as end_time, coupon_total
    FROM addfan_activity WHERE curdate() between start_time and end_time and activity_enable=1 and coupon_enable=1 and activity_delete=0
    and web_id != 'rick'    
    """
    print(query)
    data = MySqlHelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'link_code', 'coupon_limit', 'coupon_type',
               'coupon_amount', 'activity_start', 'activity_end', 'coupon_total']
    coupon_price_limit = [int(d[3].split('limit-bill=')[1]) if d[3].find('limit-bill=')!=-1 else 0 for d in data]
    df_coupon = pd.DataFrame(data, columns=columns).drop(columns=['coupon_limit'])
    df_coupon['coupon_price_limit'] = coupon_price_limit
    return df_coupon


@timing
def fetch_coupon_activity_just_expired():
    query = f"""
    SELECT id, web_id, link_code, coupon_limit, coupon_type, coupon_amount, 
    start_time, date_add(end_time,INTERVAL 1 DAY) as end_time2, coupon_total
    FROM addfan_activity WHERE DATE(update_time) between start_time and end_time 
    and DATEDIFF(curdate(), end_time) between 0 and 1
    and activity_enable=1 and coupon_enable=1 and activity_delete=0
    and web_id != 'rick'
    """
    print(query)
    data = MySqlHelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'link_code', 'coupon_limit', 'coupon_type',
               'coupon_amount', 'activity_start', 'activity_end', 'coupon_total']
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

""""
SELECT 
	sum(temp.total_price) as revenue, COUNT(temp.total_price)*100 as cost
FROM
	(SELECT 
		a.uuid AS uuid,
		b.session_id AS session_b,
		a.link_code,
		AVG(b.total_price) AS total_price
	FROM
		(select * from tracker.clean_event_acceptCoupon where web_id='nineyi11' and link_code = 'aAX9Qe0IAa') as a
	INNER JOIN (select * from tracker.clean_event_purchase where web_id='nineyi11' and total_price>1000) as b ON a.uuid = b.uuid
		AND a.session_id = b.session_id
	GROUP BY uuid , session_b) AS temp
"""


@timing
def fetch_update_revenue_cost_n_coupon(web_id, coupon_id, link_code, coupon_cost, coupon_price_limit, coupon_total, activity_start, activity_end):
    query = f"""
            SELECT 
                IFNULL(SUM(temp.total),0) AS revenue, COUNT(temp.total) * {coupon_cost} AS cost
            FROM
                (SELECT 
                    a.uuid AS uuid,	b.session_id AS session_b, b.total
                FROM
                    (SELECT * FROM tracker.clean_event_acceptCoupon 
                    WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}' 
                    AND web_id = '{web_id}' AND link_code = '{link_code}') AS a
                INNER JOIN (SELECT 
                    uuid, session_id, timestamp, SUM(product_price * product_quantity) AS total
                FROM
                    tracker.clean_event_purchase
                WHERE
                    date_time BETWEEN '{activity_start}' AND '{activity_end}'
                    AND web_id = '{web_id}'
                GROUP BY uuid , session_id, timestamp
                HAVING SUM(product_price * product_quantity) > {coupon_price_limit}) AS b ON a.uuid = b.uuid
                    AND a.session_id = b.session_id
                GROUP BY uuid , session_b) AS temp
    """
    # query = f"""
    # SELECT
    #     sum(temp.total_price) as revenue, COUNT(temp.total_price)*{coupon_cost} as cost
    # FROM
    #     (SELECT
    #         a.uuid AS uuid,
    #         b.session_id AS session_b,
    #         a.link_code,
    #         SUM(b.product_price) AS total_price
    #     FROM
    #         (select * from tracker.clean_event_acceptCoupon where web_id='{web_id}' and link_code = '{link_code}') as a
    #     INNER JOIN (select * from tracker.clean_event_purchase where web_id='{web_id}' and total_price>{coupon_price_limit}) as b ON a.uuid = b.uuid
    #         AND a.session_id = b.session_id
    #     GROUP BY uuid , session_b) AS temp
    # """
    print(query)
    data = MySqlHelper("tracker").ExecuteSelect(query)
    df_ROAS = pd.DataFrame(data, columns=['revenue', 'cost'])
    df_ROAS['id'] = [coupon_id]
    coupon_sent, coupon_accept = fetch_n_coupon_sent_accept(coupon_id, activity_start, activity_end)
    coupon_used = int(df_ROAS['cost']/coupon_cost)
    df_ROAS[['coupon_sent', 'coupon_accept', 'coupon_used']] = [[coupon_sent, coupon_accept, coupon_used]]
    days_remain = (to_datetime(activity_end)-curdate(utc=8)).days
    df_ROAS['avg_n_coupon'] = [(coupon_total-coupon_used)/days_remain]
    update_col = ['revenue', 'cost', 'coupon_sent', 'coupon_accept', 'coupon_used', 'avg_n_coupon']
    query_update = MySqlHelper.generate_insertDup_SQLquery(df_ROAS, 'addfan_activity', update_col)
    MySqlHelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query_update, df_ROAS.to_dict('records'))
    return df_ROAS

@timing
def fetch_n_coupon_sent_accept(coupon_id, activity_start, activity_end):
    query_sent = f"""
    SELECT
        COUNT(DISTINCT uuid) AS coupon_sent
    FROM
        tracker.clean_event_sendCoupon
    WHERE
        coupon_id = {coupon_id}
        AND date_time BETWEEN '{activity_start}' AND '{activity_end}'
    """
    print(query_sent)
    query_accept = f"""
    SELECT
        COUNT(DISTINCT uuid) AS coupon_accept
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
def main_update_addFan_ROAS(web_id, coupon_id, link_code, coupon_price_limit, coupon_type, coupon_amount, coupon_total, activity_start, activity_end):
    coupon_cost = fetch_coupon_cost(web_id, coupon_type, coupon_amount)
    df_ROAS = fetch_update_revenue_cost_n_coupon(web_id, coupon_id, link_code, coupon_cost, coupon_price_limit, coupon_total, activity_start, activity_end)
    return df_ROAS

if __name__ == "__main__":
    df_coupon = fetch_coupon_activity_running()
    for i,row in df_coupon.iterrows():
        coupon_id, web_id, link_code, coupon_type, coupon_amount, activity_start, activity_end, coupon_total, coupon_price_limit = row
        df_ROAS = main_update_addFan_ROAS(web_id, coupon_id, link_code, coupon_price_limit, coupon_type, coupon_amount, coupon_total, activity_start, activity_end)

    ## update ROAS just expired
    df_coupon_just_expired = fetch_coupon_activity_just_expired()
    for i,row in df_coupon_just_expired.iterrows():
        coupon_id, web_id, link_code, coupon_type, coupon_amount, activity_start, activity_end, coupon_total, coupon_price_limit = row
        df_ROAS_expired = main_update_addFan_ROAS(web_id, coupon_id, link_code, coupon_price_limit, coupon_type, coupon_amount, coupon_total, activity_start, activity_end)


    # df_ROAS = fetch_update_revenue_cost_n_coupon('nineyi11', 13, 'aAX9Qe0IAa', 100, 1000, '2022-02-24', '2022-03-02')

