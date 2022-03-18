from db import MySqlHelper
from mining import MiningTracking
from s3_parser import TrackingParser
from basic import datetime_to_str, logging_channels, filterListofDictByDict, timing, to_datetime
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


@timing
def fetch_mining_settings(web_id):
    query = f"SELECT n_day_testing,lower_bound,model_key,n_weight FROM cdp_tracking_settings where web_id='{web_id}'"
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    n_day_testing, lower_bound, features_join, n_weight = data[0]
    features_select = features_join.split(',')
    return n_day_testing, lower_bound, features_select, n_weight


@timing
def fetch_train_model(web_id):
    query = f"SELECT model_value,model_intercept FROM cdp_tracking_settings where web_id='{web_id}'"
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    model_coeff_join, intercept = data[0]
    coeff = np.array(model_coeff_join.split(',')).astype(float)
    return coeff, intercept


@timing
def fetch_addfan_web_id():
    query = f"SELECT web_id FROM cdp_tracking_settings where enable_addfan=1"
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    web_id_list = [d[0] for d in data]
    return web_id_list


@timing
def fetch_running_activity():
    query = f"""
    SELECT id, web_id, (coupon_total-coupon_used)/(1+datediff(end_time, curdate())) as avg_n_coupon
    FROM addfan_activity WHERE datediff(start_time,curdate())<=2 and curdate()<=end_time and activity_enable=1 and coupon_enable=1
    and activity_delete=0 and web_id != 'rick'
    """
    data = MySqlHelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
    if data == []:
        print(f"no available coupon in running")
        return pd.DataFrame()
    df_running_activity = pd.DataFrame(data=data, columns=['id', 'web_id', 'avg_n_coupon'])
    return df_running_activity


# ## update avg coupon budget of all coupon campaigns
# @logging_channels(['clare_test'])
# @timing
# def update_avg_coupon_a_day(web_id, update_db=True):
#     query = f"""SELECT id,ifnull((coupon_total-coupon_sent)/(1+datediff(end_time, curdate())),0) as avg_n_coupon FROM addfan_activity
#     where web_id='{web_id}' and curdate() between start_time and end_time and activity_enable=1 and coupon_enable=1"""
#     data = MySqlHelper("rhea1-db0", is_ssh=True).ExecuteSelect(query)
#     if data==[]:
#         print(f"no available coupon in running")
#         return pd.DataFrame()
#     df_n_coupon = pd.DataFrame(data=data, columns=['id', 'avg_n_coupon'])
#     if update_db:
#         query_update = MySqlHelper.generate_updateTable_SQLquery('addfan_activity', ['avg_n_coupon'], ['id'])
#         MySqlHelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query_update, df_n_coupon.to_dict('records'))
#     return df_n_coupon


@timing
def fetch_avg_shipping_purchase(web_id):
    query_shipping = f"""
                        SELECT avg(shipping_price) FROM clean_event_purchase 
                        where web_id='{web_id}' and shipping_price!=0 group by web_id"""
    # query_purchase = f"""
    #                     SELECT avg(total_price) FROM clean_event_purchase
    #                     where web_id='{web_id}' group by web_id"""
    query_purchase = f"""
    SELECT 
        SUM(a.total) / COUNT(a.total)
    FROM
        (SELECT 
            SUM(product_price * product_quantity) AS total
        FROM
            tracker.clean_event_purchase
        WHERE
            web_id = '{web_id}'
        GROUP BY uuid , session_id , timestamp) a    
    """
    print(query_shipping, query_purchase)
    avg_shipping_price = MySqlHelper("tracker").ExecuteSelect(query_shipping)[0][0]
    avg_total_price = MySqlHelper("tracker").ExecuteSelect(query_purchase)[0][0]
    return int(avg_shipping_price), int(avg_total_price)


@logging_channels(['clare_test'])
@timing
def main_update_avg_shipping_purchase(web_id, update_db=True):
    avg_shipping_price, avg_total_price = fetch_avg_shipping_purchase(web_id)
    df_stat = pd.DataFrame()
    df_stat[['web_id', 'avg_shipping_price', 'avg_total_price']] = [[web_id, avg_shipping_price, avg_total_price]]
    if update_db:
        ## update model parameters
        # query = MySqlHelper.generate_updateTable_SQLquery('cdp_tracking_settings', df_stat.columns[1:], ['web_id'])
        # MySqlHelper("rheacache-db0", is_ssh=True).ExecuteUpdate(query, df_stat.to_dict('records'))
        query2 = MySqlHelper.generate_insertDup_SQLquery(df_stat, 'addfan_stat', df_stat.columns[1:])
        MySqlHelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query2, df_stat.to_dict('records'))
    return df_stat


@logging_channels(['clare_test'])
@timing
def main_update_purchcase_bound(coupon_id, web_id, avg_n_coupon, data_use_db=True, update_db=True,
                                plot_n_cum=False, plot_ROC=False, use_fit_cum=True):
    """

    Parameters
    ----------
    coupon_id: int, to update activity in addfan_activity
    web_id: str, to fetch testing data
    avg_n_coupon: float, to compute lower_bound at given avg_n_coupon
    data_use_db: True or False, fetch testing data from db(True) or local(False)
    update_db: True or False, dose update to table
    plot_n_cum: True or False, does draw cumulative plot
    plot_ROC: True or False, does draw ROC curve
    use_fit_cum: True, use fitted cumulative curve. False, directly use discrete data

    Returns: DataFrame, df_activity_param use to update table
    -------

    """
    ## settings
    n_day_testing, lower_bound, features_select, n_weight = fetch_mining_settings(web_id)
    # n_day = 1  ## use 1 for update bound
    ## get columns if use db fetching
    keys_collect = ['uuid', 'device', 'session_id', 'timestamp', 'pageviews',
                    'time_pageview_total', 'click_count_total', 'is_purchased_before']
    if data_use_db:
        ## use db
        datetime_utc8_yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)
        date_utc8_start = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=n_day_testing)).date())
        date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
        df_test = MiningTracking.fetch_training_data(web_id, date_utc8_start, date_utc8_end, keys_collect)
    else:
        # use local
        datetime_utc8_yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)
        date_utc8_start = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=n_day_testing - 1)).date())
        date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
        df_test = MiningTracking.get_clean_data(web_id, date_utc8_start, date_utc8_end)
    # ## remove repeat uuid
    # df_test.drop_duplicates(subset=['uuid'], inplace=True)
    ## remove repeat uuid and select larger one
    df_test = df_test.groupby(["uuid"]).max().reset_index()
    ## remove desktop
    df_test = df_test.query("device!=0")
    ## remove is_purchased_before
    df_test = df_test.query("is_purchased_before==0")
    df_test.drop(columns=['device', 'is_purchased_before'], inplace=True)
    ## 1. fetch model
    coeff, intercept = fetch_train_model(web_id)
    ## 2.compute probability
    model = MiningTracking()
    model.predict_prob(df_test, features_select, coeff, intercept, inplace=True)
    lower_bound_limit = model.cal_prob(np.zeros((1,len(coeff))), coeff, intercept)[0]

    ## 3.compute upper bound at given lower bound
    if use_fit_cum:
        n_cum, prob = np.histogram(np.array(df_test['prob']), bins=100)
        n_cum_weight = n_cum / n_day_testing  ## normalize to traffic of a day
        cumulative = sum(n_cum_weight) - np.cumsum(n_cum_weight)
        prob = prob[:-1]
        cumulative = cumulative[prob > 0.06]
        prob = prob[prob > 0.06]
        poly_coeff = np.polyfit(x=cumulative, y=prob, deg=10)
        equation = np.poly1d(poly_coeff)
        max_n_coupon = cumulative[0]
        if avg_n_coupon * n_weight >= max_n_coupon:
            print(f"{avg_n_coupon * n_weight} excess one day traffic: {max_n_coupon}")
            lower_bound = equation(max_n_coupon)  ## max coupon
        else:
            print(f"{avg_n_coupon * n_weight} do not excess one day traffic: {max_n_coupon}")
            lower_bound = equation(avg_n_coupon * n_weight)
        # upper_bound = equation(n_coupon_per_day*n_weight)
        if plot_n_cum:
            print(f"result of selecting probability greater than {lower_bound:.3f} "
                  f"and n_coupon_per_day is weighted with {n_weight} (={avg_n_coupon * n_weight})")
            plt.figure(figsize=(10, 8))
            plt.plot(cumulative, prob, 'bo')
            plt.plot(cumulative, equation(cumulative), 'r--')
            plt.xlabel('number of coupons per day')
            plt.ylabel('set lower bound')
            plt.show()
    else:
        df_test_order = df_test.sort_values(by='prob', ascending=False)
        max_n_coupon = int(df_test.query("pageviews>1").shape[0] / n_day_testing)
        # max_n_coupon = int(n_day*0.8)
        if avg_n_coupon * n_weight >= max_n_coupon:
            print(f"{avg_n_coupon * n_weight} excess one day traffic: {max_n_coupon}")
            lower_bound = df_test_order.iloc[int(max_n_coupon * n_day_testing) - 1]['prob']  ## max coupon
        else:
            print(f"{avg_n_coupon * n_weight} do not excess one day traffic: {max_n_coupon}")
            lower_bound = df_test_order.iloc[int(avg_n_coupon * n_day_testing * n_weight) - 1]['prob']
    if lower_bound < lower_bound_limit:
        print(f"lower_bound({lower_bound}) <= lower_bound_limit({lower_bound_limit}), use lower_bound_limit*1.1")
    else:
        print(f"lower_bound({lower_bound}) > lower_bound_limit({lower_bound_limit}), use lower_bound")
    lower_bound = lower_bound_limit*1.1 if lower_bound <= lower_bound_limit else lower_bound
    ## 4-1.predict 0 or 1
    model.predict(df_test, lower=lower_bound, upper=1.0, inplace=True)
    ## 4-2.precision and estimate n_coupon at a day
    precision = df_test.query("is_purchased==1&predict==1").shape[0] / (df_test.query("predict==1").shape[0] + 0.1)
    estimate_n_coupon = int(len(set(df_test.query(f"predict==1")['uuid'])) / n_day_testing)
    print(
        f"test for model precision is {precision}, estimating number of coupon sent in one day will be {estimate_n_coupon}")
    ## 5.confusion matrix
    model.get_confusion_matrix(np.array(df_test['is_purchased']), np.array(df_test['predict']))
    if plot_ROC:
        ## 6.ROC curve
        model.get_ROC_curve(np.array(df_test['is_purchased']), np.array(df_test['prob']))
    ## 7.build purchase bound to be saved
    df_activity_param = pd.DataFrame()
    df_activity_param['id'] = [coupon_id]
    df_activity_param['max_n_coupon'] = [max_n_coupon]
    df_activity_param['avg_n_coupon'] = [avg_n_coupon]
    df_activity_param['lower_bound'] = [lower_bound]
    df_activity_param['estimated_n_coupon'] = [estimate_n_coupon]
    df_activity_param['model_precision'] = [precision]
    ## update model parameters
    if update_db:
        ## update every activity
        query = MySqlHelper.generate_updateTable_SQLquery('addfan_activity', df_activity_param.columns[1:], ['id'])
        MySqlHelper("rhea1-db0", is_ssh=True).ExecuteUpdate(query, df_activity_param.to_dict('records'))
    return df_activity_param, df_test


## update lower_bound, model_precision, estimate_n_coupon, avg_n_coupon everyday (or 8 hour)
if __name__ == "__main__":

    #### update addfan_stat and cdp_tracking_settings table
    # web_id_list = fetch_addfan_web_id()
    # # web_id_list = ['94monster']
    # for web_id in web_id_list:
    #     ## update addfan_stat table
    #     df_stat = main_update_avg_shipping_purchase(web_id, update_db=True)

    #### update lower_bound, model_precision, estimate_n_coupon, avg_n_coupon
    df_running_activity = fetch_running_activity()
    # df_running_activity = pd.DataFrame([[13,'nineyi11',2222.2]], columns=['id','web_id','avg_n_coupon'])
    # df_running_activity = pd.DataFrame([[49,'lovingfamily',166.67]], columns=['id','web_id','avg_n_coupon'])
    # df_running_activity = pd.DataFrame([[78, '94monster', 375]], columns=['id', 'web_id', 'avg_n_coupon'])

    for i, row in df_running_activity.iterrows():
        coupon_id, web_id, avg_n_coupon = row
        df_activity_param, df_test = main_update_purchcase_bound(coupon_id, web_id, avg_n_coupon,
                                                                 data_use_db=True, update_db=True,
                                                                 plot_n_cum=False, plot_ROC=False,
                                                                 use_fit_cum=False)
