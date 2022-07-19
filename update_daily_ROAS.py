import datetime
import pandas as pd
from basic import to_datetime, datetime_to_str, date_range, timing, curdate, logging_channels
from db import DBhelper
from update_coupon_ROAS import fetch_coupon_activity_running, fetch_calc_types, fetch_coupon_cost
from definitions import ROOT_DIR


@timing
def fetch_coupon_used_revenue_cost(web_id, coupon_id, coupon_type, coupon_cost, coupon_price_limit,
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
                            WHERE date_time BETWEEN '{activity_start}' AND '{activity_end}'
                            AND web_id = '{web_id}' AND coupon_id = '{coupon_id}') AS a
                        INNER JOIN 
                        (SELECT uuid, session_id, timestamp, SUM(product_price * product_quantity) AS total
                        FROM tracker.clean_event_purchase WHERE
                            date_time BETWEEN '{activity_start}' AND '{activity_end}' AND web_id = '{web_id}'
                        GROUP BY uuid , session_id, timestamp
                        HAVING SUM(product_price * product_quantity) > {coupon_price_limit}) AS b 
                        ON a.uuid = b.uuid
                        INNER JOIN 
                        (SELECT uuid, session_id, coupon_code FROM tracker.clean_event_enterCoupon WHERE 
                        date_time BETWEEN '{activity_start}' AND '{activity_end}' AND web_id = '{web_id}') AS c 
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
        elif type_cal_cost==4: ## join three events, acceptCoupon, enterCoupon, purchase(remove session)
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
                        ON a.uuid = b.uuid
                        INNER JOIN 
                        (SELECT uuid, session_id, coupon_code FROM tracker.clean_event_enterCoupon WHERE 
                        date_time BETWEEN '{activity_start}' AND '{activity_end}' AND web_id = '{web_id}') AS c 
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
    return df_ROAS


def fetch_accept_coupons(web_id, date_start, date_end, coupon_id):
    date_end = datetime_to_str(to_datetime(date_end) + datetime.timedelta(days=1))
    query = f"""
    SELECT DATE(date_time), count(distinct uuid) FROM tracker.clean_event_acceptCoupon 
    where web_id='{web_id}' and date_time BETWEEN '{date_start}' AND '{date_end}' AND coupon_id={coupon_id}
    group by DATE(date_time)
    """
    data = DBhelper("tracker").ExecuteSelect(query)
    df_accept = pd.DataFrame(data, columns=['date_time', 'coupon_accepted'])
    df_accept = df_accept.astype({"date_time": str})
    return df_accept

def fetch_imp_coupons(web_id, date_start, date_end, coupon_id):
    n_day = (to_datetime(date_end) - to_datetime(date_start)).days + 1
    date_list = date_range(date_start, n_day)
    df_imp_concat = pd.DataFrame()
    for date in date_list:
        date_start = date
        date_end = datetime_to_str(to_datetime(date) + datetime.timedelta(days=1))
        query = f"""
                SELECT count(*) FROM (
                SELECT a.uuid FROM
                    (SELECT uuid FROM tracker.clean_event_acceptCoupon
                    WHERE web_id='{web_id}' and date_time BETWEEN '{date_start}' AND '{date_end}' AND coupon_id={coupon_id}) a 
                UNION 
                SELECT b.uuid FROM
                    (SELECT uuid FROM tracker.clean_event_discardCoupon
                    WHERE web_id='{web_id}' and date_time BETWEEN '{date_start}' AND '{date_end}' AND coupon_id={coupon_id}) b) x
                """
        data = DBhelper("tracker").ExecuteSelect(query)
        df_imp = pd.DataFrame(data, columns=['coupon_imp'])
        df_imp['date_time'] = [datetime_to_str(date_start)]
        df_imp_concat = pd.concat([df_imp_concat, df_imp])
    return df_imp_concat

def fetch_used_coupons(web_id, coupon_id, coupon_type, coupon_cost, coupon_price_limit,
                       date_start, date_end, type_total_price, type_cal_cost):
    n_day = (to_datetime(date_end) - to_datetime(date_start)).days + 1
    date_list = date_range(date_start, n_day)
    df_used_concat = pd.DataFrame()
    for date in date_list:
        date_start = date
        date_end = datetime_to_str(to_datetime(date) + datetime.timedelta(days=1))
        df_used = fetch_coupon_used_revenue_cost(web_id, coupon_id, coupon_type, coupon_cost, coupon_price_limit,
                                                 date_start, date_end, type_total_price, type_cal_cost)
        # df_used.drop(['id'], axis=1, inplace=True)
        # query = get_query(web_id, date_start, date_end, coupon_id, type_total_price, type_cal_cost)
        # query = f"""
        #         SELECT
        #             IFNULL(SUM(temp.total),0) AS revenue, IFNULL(SUM(temp.coupon),0) AS cost,
        #             COUNT(temp.total) as coupon_used
        #         FROM
        #         (SELECT
        #             a.uuid AS uuid,	b.session_id AS session_b, b.total, b.coupon
        #         FROM
        #             (SELECT * FROM tracker.clean_event_acceptCoupon
        #             WHERE date_time BETWEEN '{date_start}' AND '{date_end}'
        #             AND web_id = '{web_id}' AND coupon_id={coupon_id}) AS a
        #         INNER JOIN (SELECT
        #             uuid, session_id, timestamp, SUM(product_price * product_quantity) AS total, AVG(ABS(coupon)) as coupon
        #         FROM
        #             tracker.clean_event_purchase
        #         WHERE
        #             date_time BETWEEN '{date_start}' AND '{date_end}'
        #             AND web_id = '{web_id}' AND coupon!=0
        #         GROUP BY uuid , session_id, timestamp
        #         HAVING SUM(product_price * product_quantity) > 1000) AS b ON a.uuid = b.uuid
        #             AND a.session_id = b.session_id
        #         GROUP BY uuid , session_b) AS temp
        #         """
        # data = DBhelper("tracker").ExecuteSelect(query)
        # df_used = pd.DataFrame(data, columns=['revenue', 'cost', 'coupon_used'])
        df_used['date_time'] = [datetime_to_str(date_start)]
        df_used_concat = pd.concat([df_used_concat, df_used])
    return df_used_concat


def merge_df(df_accept, df_imp, df_used):
    df = pd.concat([df_accept.set_index('date_time'), df_imp.set_index('date_time'), df_used.set_index('date_time')], axis=1)
    df.dropna(inplace=True)
    df = df.astype(int).reset_index()
    return df


def fetch_stat_by_id(web_id, coupon_id, coupon_type, coupon_cost, coupon_price_limit,
                     date_start, date_end, type_total_price, type_cal_cost, today):
    if (to_datetime(date_end) - today).days > 0:
        date_end = datetime_to_str(today)
    df_accept = fetch_accept_coupons(web_id, date_start, date_end, coupon_id)
    df_imp = fetch_imp_coupons(web_id, date_start, date_end, coupon_id)
    df_used = fetch_used_coupons(web_id, coupon_id, coupon_type, coupon_cost, coupon_price_limit,
                                 date_start, date_end, type_total_price, type_cal_cost)
    df = pd.concat([df_accept.set_index('date_time'), df_imp.set_index('date_time'), df_used.set_index('date_time')], axis=1)
    df.fillna(0, inplace=True)
    df = df.astype(int)
    return df.reset_index()


def init_daily_ROAS():
    today = curdate(utc=8)
    type_total_price_dict, type_cal_cost_dict = fetch_calc_types()
    df_coupon = fetch_coupon_activity_running()
    # df_coupon = df_coupon.query(f"web_id=='lab52'") # lovingfamily
    df_daily_all = pd.DataFrame()
    for i, row in df_coupon.iterrows():
        coupon_id, web_id, link_code, coupon_type, coupon_amount, date_start, date_end, coupon_total, coupon_price_limit = row
        type_total_price = type_total_price_dict[web_id] if web_id in type_total_price_dict.keys() else 0
        type_cal_cost = type_cal_cost_dict[web_id] if web_id in type_cal_cost_dict.keys() else 0
        coupon_cost = fetch_coupon_cost(web_id, coupon_type, coupon_amount)
        df_daily = fetch_stat_by_id(web_id, coupon_id, coupon_type, coupon_cost, coupon_price_limit,
                                    date_start, date_end, type_total_price, type_cal_cost, today)
        df_daily[['web_id', 'coupon_id']] = [[web_id, coupon_id]] * df_daily.shape[0]
        df_daily_all = pd.concat([df_daily_all, df_daily])

    update_cols = ['web_id', 'coupon_imp', 'coupon_accepted', 'coupon_used', 'revenue', 'cost']
    query = DBhelper.generate_insertDup_SQLquery(df_daily_all, 'addfan_daily_ROAS', update_cols)
    DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_daily_all.to_dict('records'))
    return df_daily_all

# execute every hour
@logging_channels(['clare_test'], save_local=True, ROOT_DIR=ROOT_DIR)
def update_daily():
    today = curdate(utc=8)
    type_total_price_dict, type_cal_cost_dict = fetch_calc_types()
    df_coupon = fetch_coupon_activity_running()
    # df_coupon = df_coupon.query(f"web_id=='lab52'") # lovingfamily
    df_daily_all = pd.DataFrame()
    for i, row in df_coupon.iterrows():
        coupon_id, web_id, link_code, coupon_type, coupon_amount, date_start, date_end, coupon_total, coupon_price_limit = row
        date_start, date_end = datetime_to_str(today), datetime_to_str(today)
        type_total_price = type_total_price_dict[web_id] if web_id in type_total_price_dict.keys() else 0
        type_cal_cost = type_cal_cost_dict[web_id] if web_id in type_cal_cost_dict.keys() else 0
        coupon_cost = fetch_coupon_cost(web_id, coupon_type, coupon_amount)
        df_daily = fetch_stat_by_id(web_id, coupon_id, coupon_type, coupon_cost, coupon_price_limit,
                                    date_start, date_end, type_total_price, type_cal_cost, today)
        df_daily[['web_id', 'coupon_id']] = [[web_id, coupon_id]] * df_daily.shape[0]
        df_daily_all = pd.concat([df_daily_all, df_daily])

    update_cols = ['web_id', 'coupon_imp', 'coupon_accepted', 'coupon_used', 'revenue', 'cost']
    query = DBhelper.generate_insertDup_SQLquery(df_daily_all, 'addfan_daily_ROAS', update_cols)
    DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_daily_all.to_dict('records'))
    return df_daily_all


if __name__ == "__main__":
    # date_start = '2022-07-18'
    # date_end = '2022-07-18'
    # coupon_id = 268 # 232(new) ,245(old), 268 old
    # web_id = 'i3fresh'

    # to_datetime(activity_end)
    ## init
    # df_daily_all = init_daily_ROAS()

    ## update every hour
    df_daily_all = update_daily()


    #
    # today = curdate(utc=8)
    # type_total_price_dict, type_cal_cost_dict = fetch_calc_types()
    # df_coupon = fetch_coupon_activity_running()
    # df_coupon = df_coupon.query(f"web_id=='lab52'") # lovingfamily
    # df_daily_all = pd.DataFrame()
    # for i, row in df_coupon.iterrows():
    #     coupon_id, web_id, link_code, coupon_type, coupon_amount, date_start, date_end, coupon_total, coupon_price_limit = row
    #     date_start, date_end = datetime_to_str(today), datetime_to_str(today)
    #     type_total_price = type_total_price_dict[web_id] if web_id in type_total_price_dict.keys() else 0
    #     type_cal_cost = type_cal_cost_dict[web_id] if web_id in type_cal_cost_dict.keys() else 0
    #     coupon_cost = fetch_coupon_cost(web_id, coupon_type, coupon_amount)
    #     df_daily = fetch_stat_by_id(web_id, coupon_id, coupon_type, coupon_cost, coupon_price_limit,
    #                                 date_start, date_end, type_total_price, type_cal_cost, today)
    #     df_daily[['web_id', 'coupon_id']] = [[web_id, coupon_id]] * df_daily.shape[0]
    #     df_daily['web_id']
    #     df_daily_all = pd.concat([df_daily_all, df_daily])
    #
    # update_cols = ['web_id', 'coupon_imp', 'coupon_accepted', 'coupon_used', 'revenue', 'cost']
    # query = DBhelper.generate_insertDup_SQLquery(df_daily_all, 'addfan_daily_ROAS', update_cols)
    # DBhelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_daily_all.to_dict('records'))



    # df_accept = fetch_accept_coupons('i3fresh', date_start, date_end, coupon_id)
    # df_imp = fetch_imp_coupons('i3fresh', date_start, date_end, coupon_id)
    # df_used = fetch_used_coupons('i3fresh', date_start, date_end, coupon_id)
    # df = pd.concat([df_accept.set_index('date'), df_imp.set_index('date'), df_used.set_index('date')], axis=1)

    # df = fetch_stat_by_id(web_id, coupon_id, date_start, date_end)

    # pd.merge(df_accept.set_index('date'), df_imp.set_index('date'), left_index=True, right_index=True)
    # df_accept.to_csv(f'accept_{coupon_id}.csv',index=False)
    # df_imp.to_csv(f'imp_{coupon_id}.csv',index=False)
    # df_used.to_csv(f'used_{coupon_id}.csv',index=False)