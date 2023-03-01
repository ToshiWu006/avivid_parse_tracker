from db import DBhelper
from basic import timing, logging_channels, to_datetime, curdate
import pandas as pd
from definitions import ROOT_DIR
import datetime
from log_utils import slackBot

@timing
def fetch_coupon_activity_all():
    query = f"""
    SELECT id, web_id, link_code, coupon_limit, coupon_type, coupon_amount, 
    start_time, date_add(end_time,INTERVAL 1 DAY) as end_time, coupon_total
    FROM addfan_activity WHERE web_id != 'rick'    
    """
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'link_code', 'coupon_limit', 'coupon_type',
               'coupon_amount', 'activity_start', 'activity_end', 'coupon_total']
    coupon_price_limit = [int(d[3].split('limit-bill=')[1]) if d[3].find('limit-bill=')!=-1 else 0 for d in data]
    df_coupon = pd.DataFrame(data, columns=columns).drop(columns=['coupon_limit'])
    df_coupon['coupon_price_limit'] = coupon_price_limit
    return df_coupon

@timing
def fetch_coupon_activity_by_id(coupon_id):
    query = f"""
    SELECT id, web_id, link_code, coupon_limit, coupon_type, coupon_amount, 
    start_time, date_add(end_time,INTERVAL 1 DAY) as end_time, coupon_total
    FROM addfan_activity WHERE web_id != 'rick' and id={coupon_id}
    """
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'link_code', 'coupon_limit', 'coupon_type',
               'coupon_amount', 'activity_start', 'activity_end', 'coupon_total']
    coupon_price_limit = [int(d[3].split('limit-bill=')[1]) if d[3].find('limit-bill=')!=-1 else 0 for d in data]
    df_coupon = pd.DataFrame(data, columns=columns).drop(columns=['coupon_limit'])
    df_coupon['coupon_price_limit'] = coupon_price_limit
    return df_coupon

@timing
def fetch_coupon_activity_running():
    query = f"""
    SELECT id, web_id, link_code, coupon_limit, coupon_type, coupon_amount, 
    start_time, date_add(end_time,INTERVAL 1 DAY) as end_time, coupon_total
    FROM addfan_activity WHERE curdate() between start_time and end_time and activity_enable=1 and coupon_enable=1 and activity_delete=0
    and web_id != 'rick'    
    """
    print(query)
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
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
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['id', 'web_id', 'link_code', 'coupon_limit', 'coupon_type',
               'coupon_amount', 'activity_start', 'activity_end', 'coupon_total']
    coupon_price_limit = [int(d[3].split('limit-bill=')[1]) if d[3].find('limit-bill=')!=-1 else 0 for d in data]
    df_coupon = pd.DataFrame(data, columns=columns).drop(columns=['coupon_limit'])
    df_coupon['coupon_price_limit'] = coupon_price_limit
    return df_coupon

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
    query_imp = f"""
    SELECT count(*) FROM (
    SELECT a.uuid FROM
        (SELECT uuid FROM tracker.clean_event_acceptCoupon
        WHERE coupon_id = {coupon_id} AND date_time BETWEEN '{activity_start}' AND '{activity_end}') a 
    UNION 
    SELECT b.uuid FROM
        (SELECT uuid FROM tracker.clean_event_discardCoupon
        WHERE coupon_id = {coupon_id} AND date_time BETWEEN '{activity_start}' AND '{activity_end}') b) x
    """
    print(query_imp)

    data_sent = DBhelper("tracker").ExecuteSelect(query_sent)
    data_accept = DBhelper("tracker").ExecuteSelect(query_accept)
    data_imp = DBhelper("tracker").ExecuteSelect(query_imp)
    coupon_sent, coupon_accept, coupon_impression = data_sent[0][0], data_accept[0][0], data_imp[0][0]
    return coupon_sent, coupon_accept, coupon_impression

def fetch_calc_types():
    query = f"""
    SELECT web_id, type_total_price, type_cal_cost FROM web_push.cdp_tracking_settings where enable_analysis=1
    """
    print(query)
    data = DBhelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    type_total_price_dict, type_cal_cost_dict = {}, {}
    for d in data:
        type_total_price_dict.update({d[0]:d[1]})
        type_cal_cost_dict.update({d[0]:d[2]})
    return type_total_price_dict, type_cal_cost_dict


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
def fetch_coupon_used_revenue_cost(web_id, coupon_id, coupon_type, coupon_cost, coupon_amount, coupon_price_limit,
                                   activity_start, activity_end, type_total_price, type_cal_cost):
    if type_total_price==0: ## use product_price * product_quantity
        if type_cal_cost==1: ## use coupon column to directly calculate cost (coupon column is value)
            query = f"""
                    SELECT
                    IFNULL(SUM(temp.total),0) AS revenue, IFNULL(SUM(temp.coupon),0) AS cost, 
                    COUNT(temp.total) as coupon_used
                    FROM
                    (SELECT
                        a.uuid AS uuid,	b.session_id AS session_b, b.total, b.coupon
                    FROM
                        (SELECT * FROM tracker.clean_event_acceptCoupon
                        WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                        AND web_id = '{web_id}' AND coupon_id = '{coupon_id}') AS a
                    INNER JOIN (SELECT
                        uuid, session_id, timestamp, SUM(product_price * product_quantity) AS total, AVG(ABS(coupon)) as coupon
                    FROM
                        tracker.clean_event_purchase
                    WHERE
                        date_time BETWEEN '{activity_start}' AND '{activity_end}'
                        AND web_id = '{web_id}' AND coupon!=0
                    GROUP BY uuid , session_id, timestamp
                    HAVING SUM(product_price * product_quantity) > {coupon_price_limit}) AS b ON a.uuid = b.uuid
                        AND a.session_id = b.session_id
                    GROUP BY uuid , session_b) AS temp
                    """
        elif type_cal_cost==2: ## use coupon column verify, coupon column store coupon_code case
            query = f"""
                    SELECT
                        IFNULL(SUM(temp.total),0) AS revenue, COUNT(temp.total) * {coupon_cost} AS cost, 
                        COUNT(temp.total) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid,	b.session_id AS session_b, b.total
                        FROM
                            (SELECT * FROM tracker.clean_event_acceptCoupon
                            WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                            AND web_id = '{web_id}' AND coupon_id = '{coupon_id}') AS a
                        INNER JOIN (SELECT
                            uuid, session_id, timestamp, SUM(product_price * product_quantity) AS total, coupon
                        FROM
                            tracker.clean_event_purchase
                        WHERE
                            date_time BETWEEN '{activity_start}' AND '{activity_end}'
                            AND web_id = '{web_id}'
                        GROUP BY uuid , session_id, timestamp
                        HAVING SUM(product_price * product_quantity) > {coupon_price_limit}) AS b ON a.uuid = b.uuid
                            AND a.session_id = b.session_id AND a.coupon_code=b.coupon
                        GROUP BY uuid , session_b) AS temp
                    """
        elif type_cal_cost==3: ## stricter than else case
            query = f"""
                    SELECT
                        IFNULL(SUM(temp.total),0) AS revenue, COUNT(temp.total) * {coupon_cost} AS cost, 
                        COUNT(temp.total) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid,	b.session_id AS session_b, b.total
                        FROM
                            (SELECT * FROM tracker.clean_event_acceptCoupon
                            WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                            AND web_id = '{web_id}' AND coupon_id = '{coupon_id}') AS a
                        INNER JOIN (SELECT
                            uuid, session_id, timestamp, SUM(product_price * product_quantity) AS total
                        FROM
                            tracker.clean_event_purchase
                        WHERE
                            date_time BETWEEN '{activity_start}' AND '{activity_end}'
                            AND web_id = '{web_id}' AND avivid_coupon='1'
                        GROUP BY uuid , session_id, timestamp
                        HAVING SUM(product_price * product_quantity) > {coupon_price_limit}) AS b ON a.uuid = b.uuid
                            AND a.session_id = b.session_id
                        GROUP BY uuid , session_b) AS temp
                    """
        elif type_cal_cost == 4: ## join three events, acceptCoupon, enterCoupon, purchase without same session
            query = f"""
                    SELECT
                        IFNULL(SUM(temp.total),0) AS revenue, COUNT(temp.total) * {coupon_cost} AS cost, 
                        COUNT(temp.total) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid,	b.session_id AS session_b, b.total
                        FROM
                            (SELECT * FROM tracker.clean_event_acceptCoupon
                            WHERE date_time >= '{activity_start}'
                            AND web_id = '{web_id}' AND coupon_id = '{coupon_id}') AS a
                        INNER JOIN 
                        (SELECT uuid, session_id, timestamp, SUM(product_price * product_quantity) AS total
                        FROM tracker.clean_event_purchase WHERE
                            date_time >= '{activity_start}' AND web_id = '{web_id}'
                        GROUP BY uuid , session_id, timestamp
                        HAVING SUM(product_price * product_quantity) > {coupon_price_limit}) AS b 
                        ON a.uuid = b.uuid
                        INNER JOIN 
                        (SELECT uuid, session_id, coupon_code FROM tracker.clean_event_enterCoupon WHERE 
                        date_time >= '{activity_start}' AND web_id = '{web_id}') AS c 
                        on a.uuid=c.uuid AND a.coupon_code=c.coupon_code
                        GROUP BY uuid , session_b) AS temp
                    """
        elif type_cal_cost == 5: ## join three events, acceptCoupon, enterCoupon, purchase
            query = f"""
                    SELECT
                        IFNULL(SUM(temp.total),0) AS revenue, COUNT(temp.total) * {coupon_cost} AS cost, 
                        COUNT(temp.total) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid,	b.session_id AS session_b, b.total
                        FROM
                            (SELECT * FROM tracker.clean_event_acceptCoupon
                            WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                            AND web_id = '{web_id}' AND coupon_id = '{coupon_id}') AS a
                        INNER JOIN 
                        (SELECT uuid, session_id, timestamp, SUM(product_price * product_quantity) AS total
                        FROM tracker.clean_event_purchase WHERE
                            date_time BETWEEN '{activity_start}' AND '{activity_end}' AND web_id = '{web_id}'
                        GROUP BY uuid , session_id, timestamp
                        HAVING SUM(product_price * product_quantity) > {coupon_price_limit}) AS b 
                        ON a.uuid = b.uuid AND a.session_id = b.session_id
                        INNER JOIN 
                        (SELECT uuid, session_id, coupon_code FROM tracker.clean_event_enterCoupon WHERE 
                        date_time BETWEEN '{activity_start}' AND '{activity_end}' AND web_id = '{web_id}') AS c 
                        on a.uuid=c.uuid AND a.session_id=c.session_id AND a.coupon_code=c.coupon_code
                        GROUP BY uuid , session_b) AS temp
                    """
        else: ## do not use coupon column
            query = f"""
                    SELECT
                        IFNULL(SUM(temp.total),0) AS revenue, COUNT(temp.total) * {coupon_cost} AS cost, 
                        COUNT(temp.total) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid,	b.session_id AS session_b, b.total
                        FROM
                            (SELECT * FROM tracker.clean_event_acceptCoupon
                            WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                            AND web_id = '{web_id}' AND coupon_id = '{coupon_id}') AS a
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
    else:
        if type_cal_cost == 1:  ## use coupon column to directly calculate cost (coupon column is value)
            query = f"""
                    SELECT
                        IFNULL(sum(temp.total_price),0) as revenue, COUNT(temp.total_price)*{coupon_cost} as cost,
                        COUNT(temp.total_price) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid,
                            b.session_id AS session_b,
                            b.total_price,
                            AVG(ABS(b.coupon)) as coupon
                        FROM
                            (select * from tracker.clean_event_acceptCoupon where web_id='{web_id}' and coupon_id = '{coupon_id}') as a
                        INNER JOIN (select * from tracker.clean_event_purchase where web_id='{web_id}' and total_price>{coupon_price_limit} and coupon!=0) as b ON a.uuid = b.uuid
                            AND a.session_id = b.session_id
                        GROUP BY uuid , session_b) AS temp
                    """

        elif type_cal_cost == 2:  ## use coupon column verify, coupon column store coupon_code case
            query = f"""
                    SELECT
                        IFNULL(sum(temp.total_price),0) as revenue, COUNT(temp.total_price)*{coupon_cost} as cost,
                        COUNT(temp.total_price) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid,
                            b.session_id AS session_b,
                            b.total_price
                        FROM
                            (select * from tracker.clean_event_acceptCoupon where web_id='{web_id}' and coupon_id = '{coupon_id}') as a
                        INNER JOIN (select * from tracker.clean_event_purchase where web_id='{web_id}' and total_price>{coupon_price_limit}) as b ON a.uuid = b.uuid
                            AND a.session_id = b.session_id AND a.coupon_code = b.coupon
                        GROUP BY uuid , session_b) AS temp
                    """
        elif type_cal_cost==3: ## stricter than else case
            query = f"""
                    SELECT
                        IFNULL(sum(temp.total_price),0) as revenue, COUNT(temp.total_price)*{coupon_cost} as cost,
                        COUNT(temp.total_price) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid, b.session_id AS session_b, b.total_price
                        FROM
                            (select * from tracker.clean_event_acceptCoupon where web_id='{web_id}' and coupon_id = '{coupon_id}') as a
                        INNER JOIN 
                        (select * from tracker.clean_event_purchase where web_id='{web_id}' and total_price>{coupon_price_limit} AND avivid_coupon='1') as b ON a.uuid = b.uuid
                            AND a.session_id = b.session_id
                        GROUP BY uuid , session_b) AS temp
                    """
        elif type_cal_cost==4: ## join three events, acceptCoupon, enterCoupon, purchase
            query = f"""
                    SELECT
                        IFNULL(sum(temp.total_price),0) as revenue, COUNT(temp.total_price)*{coupon_cost} as cost,
                        COUNT(temp.total_price) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid, b.session_id AS session_b, b.total_price
                        FROM
                            (select * from tracker.clean_event_acceptCoupon WHERE 
                            date_time >= '{activity_start}' AND 
                            web_id='{web_id}' AND coupon_id='{coupon_id}') AS a
                        INNER JOIN 
                        (select * from tracker.clean_event_purchase where 
                        date_time >= '{activity_start}' AND
                        web_id='{web_id}' and total_price>{coupon_price_limit}) AS b 
                        ON a.uuid = b.uuid
                        INNER JOIN 
                        (SELECT uuid, session_id, coupon_code FROM tracker.clean_event_enterCoupon WHERE 
                        date_time >= '{activity_start}' AND web_id = '{web_id}') AS c 
                        on a.uuid=c.uuid AND a.coupon_code=c.coupon_code
                        GROUP BY uuid , session_b) AS temp
                    """
        elif type_cal_cost==5: ## join three events, acceptCoupon, enterCoupon, purchase
            query = f"""
                    SELECT
                        IFNULL(sum(temp.total_price),0) as revenue, COUNT(temp.total_price)*{coupon_cost} as cost,
                        COUNT(temp.total_price) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid, b.session_id AS session_b, b.total_price
                        FROM
                            (select * from tracker.clean_event_acceptCoupon WHERE 
                            date_time BETWEEN '{activity_start}' AND '{activity_end}' AND
                            web_id='{web_id}' AND coupon_id='{coupon_id}') AS a
                        INNER JOIN 
                        (select * from tracker.clean_event_purchase where 
                        date_time BETWEEN '{activity_start}' AND '{activity_end}' AND
                        web_id='{web_id}' and total_price>{coupon_price_limit}) AS b 
                        ON a.uuid = b.uuid AND a.session_id = b.session_id
                        INNER JOIN 
                        (SELECT uuid, session_id, coupon_code FROM tracker.clean_event_enterCoupon WHERE 
                        date_time BETWEEN '{activity_start}' AND '{activity_end}' AND web_id = '{web_id}') AS c 
                        on a.uuid=c.uuid AND a.session_id=c.session_id AND a.coupon_code=c.coupon_code
                        GROUP BY uuid , session_b) AS temp
                    """
        else:
            query = f"""
                    SELECT
                        IFNULL(sum(temp.total_price),0) as revenue, COUNT(temp.total_price)*{coupon_cost} as cost,
                        COUNT(temp.total_price) as coupon_used
                    FROM
                        (SELECT
                            a.uuid AS uuid,
                            b.session_id AS session_b,
                            b.total_price
                        FROM
                            (select * from tracker.clean_event_acceptCoupon where web_id='{web_id}' and coupon_id = '{coupon_id}') as a
                        INNER JOIN (select * from tracker.clean_event_purchase where web_id='{web_id}' and total_price>{coupon_price_limit}) as b ON a.uuid = b.uuid
                            AND a.session_id = b.session_id
                        GROUP BY uuid , session_b) AS temp
                    """
    print(query)
    data = DBhelper("tracker").ExecuteSelect(query)
    df_ROAS = pd.DataFrame(data, columns=['revenue', 'cost', 'coupon_used'])
    if coupon_type==3: # % off
        ## replace calculation of cost
        df_ROAS['cost'] = int(int(df_ROAS['revenue'])*(10-float(coupon_amount))/10)
    df_ROAS['id'] = [coupon_id]
    return df_ROAS

def get_n_coupon_stat(df_ROAS, coupon_id, activity_start, activity_end, coupon_total):
    coupon_used = int(df_ROAS['coupon_used'])
    coupon_sent, coupon_accept, coupon_impression = fetch_n_coupon_sent_accept(coupon_id, activity_start, activity_end)
    df_ROAS[['coupon_sent', 'coupon_accept', 'coupon_impression']] = [[coupon_sent, coupon_accept, coupon_impression]]
    days_remain = (to_datetime(activity_end)-curdate(utc=8)).days
    df_ROAS['avg_n_coupon'] = [0] if days_remain==0 else [(coupon_total-coupon_used)/days_remain]
    return df_ROAS

@logging_channels(['clare_test'], save_local=True, ROOT_DIR=ROOT_DIR)
def update_ROAS(df_ROAS, is_save=True, update_col=None):
    if not update_col:
        update_col = ['revenue', 'cost', 'coupon_sent', 'coupon_impression', 'coupon_accept', 'coupon_used', 'avg_n_coupon']
    query_update = DBhelper.generate_insertDup_SQLquery(df_ROAS, 'addfan_activity', update_col)
    if is_save:
        DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query_update, df_ROAS.to_dict('records'))

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
        coupon_cost = int(DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)[0][0])
    elif coupon_type==2:
        coupon_cost = int(coupon_amount)
    elif coupon_type==3:
        print(f"use total cost * (1-{coupon_amount}/10)")
        coupon_cost = -1 # assign temp value
    else:
        coupon_cost = 100
    return coupon_cost


@logging_channels(['clare_test'], save_local=True, ROOT_DIR=ROOT_DIR)
def main_update_addFan_ROAS(web_id, coupon_id, coupon_price_limit,
                            coupon_type, coupon_amount, coupon_total,
                            activity_start, activity_end,
                            type_total_price, type_cal_cost,
                            is_save=True):
    coupon_cost = fetch_coupon_cost(web_id, coupon_type, coupon_amount)
    df_ROAS = fetch_coupon_used_revenue_cost(web_id, coupon_id, coupon_type,
                                            coupon_cost, coupon_amount, coupon_price_limit,
                                            activity_start, activity_end,
                                            type_total_price, type_cal_cost)
    df_ROAS = get_n_coupon_stat(df_ROAS, coupon_id, activity_start, activity_end, coupon_total)
    update_ROAS(df_ROAS, is_save=is_save)

    return df_ROAS


@logging_channels(['clare_test'], save_local=True, ROOT_DIR=ROOT_DIR)
def fetch_ROAS_by_daily(coupon_id_list):
    coupon_id_list = [str(id) for id in coupon_id_list]
    if not coupon_id_list:
        print("coupon_id_list is empty, do nothing")
        return pd.DataFrame()
    query = f"""
    SELECT web_id, coupon_id, sum(coupon_imp), sum(coupon_accepted), sum(coupon_used), sum(cost), sum(revenue) 
    FROM web_push.addfan_daily_ROAS WHERE coupon_id in ({','.join(coupon_id_list)}) group by coupon_id
    """
    data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    columns = ['web_id', 'id', 'coupon_impression', 'coupon_accept', 'coupon_used', 'cost', 'revenue']
    df_ROAS = pd.DataFrame(data, columns=columns)
    return df_ROAS
@logging_channels(['coupon_test'], save_local=True, ROOT_DIR=ROOT_DIR)
def check_coupon_exception():
    msg , exceptionIDSet = [], set()
    for days in range(10, 0, -1):
        query = f"""SELECT id, web_id FROM web_push.addfan_activity
                    WHERE web_id!='rick' and ad_image_url = '_'  
                    and activity_delete = 0 and coupon_enable = 1 and activity_enable = 1 
                    and start_time < DATE_SUB(CURDATE(), INTERVAL {days} DAY) and end_time > curdate() 
                    and coupon_used = 0;
                    """
        data = DBhelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
        if len(data) > 0:
            for i in data:
                if i[0] not in exceptionIDSet:
                    exceptionIDSet.add(i[0])
                    msg.append(f"{i[1]}'s coupon id :{i[0]} hasn't been used for {days} days! Please check!")
    if msg:
        slackBot(["coupon_test"]).send_message('\n'.join([datetime.datetime.today().strftime('%Y-%m-%d')] + msg))


if __name__ == "__main__":
    if datetime.datetime.now().hour == 9:
        check_coupon_exception()
    df_coupon = fetch_coupon_activity_running()
    coupon_id_list = list(df_coupon['id'])
    df_ROAS = fetch_ROAS_by_daily(coupon_id_list)
    update_ROAS(df_ROAS, is_save=True, update_col=list(df_ROAS.columns))

    # expired
    df_coupon_just_expired = fetch_coupon_activity_just_expired()
    coupon_id_exp_list = list(df_coupon_just_expired['id'])
    df_ROAS_expired = fetch_ROAS_by_daily(coupon_id_exp_list)
    update_ROAS(df_ROAS_expired, is_save=True, update_col=list(df_ROAS_expired.columns))

    # manually
    # df_coupon_manually = fetch_coupon_activity_by_id(380)
    # coupon_id_list = list(df_coupon_manually['id'])
    # df_ROAS_manually = fetch_ROAS_by_daily(coupon_id_list)
    # update_ROAS(df_ROAS_manually, is_save=True, update_col=list(df_ROAS_manually.columns))

    #
    # ## type_total_price: 0(use price*quantity), 1,others(use total_price),
    # ## type_cal_cost: 0,others(use coupon_cost), 1(use coupon column in purchase_event table)
    # type_total_price_dict, type_cal_cost_dict = fetch_calc_types()
    # df_coupon = fetch_coupon_activity_running()
    # # df_coupon = df_coupon.query(f"web_id=='coway'")
    # df_ROAS_all = pd.DataFrame()
    # for i,row in df_coupon.iterrows():
    #     coupon_id, web_id, link_code, coupon_type, coupon_amount, activity_start, activity_end, coupon_total, coupon_price_limit = row
    #     type_total_price = type_total_price_dict[web_id] if web_id in type_total_price_dict.keys() else 0
    #     type_cal_cost = type_cal_cost_dict[web_id] if web_id in type_cal_cost_dict.keys() else 0
    #     df_ROAS = main_update_addFan_ROAS(web_id, coupon_id, coupon_price_limit,
    #                                       coupon_type, coupon_amount, coupon_total,
    #                                       activity_start, activity_end,
    #                                       type_total_price, type_cal_cost,
    #                                       is_save=True)
    #     df_ROAS_all = pd.concat([df_ROAS_all,df_ROAS])
    # ## update ROAS just expired
    # df_coupon_just_expired = fetch_coupon_activity_just_expired()
    # df_ROAS_expired_all = pd.DataFrame()
    # for i,row in df_coupon_just_expired.iterrows():
    #     coupon_id, web_id, link_code, coupon_type, coupon_amount, activity_start, activity_end, coupon_total, coupon_price_limit = row
    #     type_total_price = type_total_price_dict[web_id] if web_id in type_total_price_dict.keys() else 0
    #     type_cal_cost = type_cal_cost_dict[web_id] if web_id in type_cal_cost_dict.keys() else 0
    #     df_ROAS_expired = main_update_addFan_ROAS(web_id, coupon_id, coupon_price_limit,
    #                                               coupon_type, coupon_amount, coupon_total,
    #                                               activity_start, activity_end,
    #                                               type_total_price, type_cal_cost,
    #                                               is_save=True)
    #     df_ROAS_expired_all = pd.concat([df_ROAS_expired_all,df_ROAS])


    # ## force to update all existing coupon
    # df_coupon_all = fetch_coupon_activity_all()
    # df_coupon_all = df_coupon_all.query(f"web_id=='nineyi2012'")
    # df_ROAS_alls = pd.DataFrame()
    # for i,row in df_coupon_all.iterrows():
    #     coupon_id, web_id, link_code, coupon_type, coupon_amount, activity_start, activity_end, coupon_total, coupon_price_limit = row
    #     type_total_price = type_total_price_dict[web_id] if web_id in type_total_price_dict.keys() else 0
    #     type_cal_cost = type_cal_cost_dict[web_id] if web_id in type_cal_cost_dict.keys() else 0
    #     df_ROAS = main_update_addFan_ROAS(web_id, coupon_id, coupon_price_limit,
    #                                               coupon_type, coupon_amount, coupon_total,
    #                                               activity_start, activity_end,
    #                                               type_total_price, type_cal_cost,
    #                                               is_save=True)
    #     df_ROAS_alls = pd.concat([df_ROAS_alls,df_ROAS])
