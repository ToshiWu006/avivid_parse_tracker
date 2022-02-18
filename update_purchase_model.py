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
    query = f"SELECT test_size,n_day_mining,lower_bound,model_key,n_weight FROM cdp_tracking_settings where web_id='{web_id}'"
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    test_size, n_day, lower_bound, features_join, n_weight = data[0]
    features_select = features_join.split(',')
    return test_size, n_day, lower_bound, features_select, n_weight

@timing
def fetch_avg_shipping_purchase(web_id):
    query_shipping = f"""
                        SELECT avg(shipping_price) FROM clean_event_purchase 
                        where web_id='{web_id}' and shipping_price!=0 group by web_id"""
    query_purchase = f"""
                        SELECT avg(total_price) FROM clean_event_purchase 
                        where web_id='{web_id}' group by web_id"""
    print(query_shipping, query_purchase)
    avg_shipping_price = MySqlHelper("tracker").ExecuteSelect(query_shipping)[0][0]
    avg_total_price = MySqlHelper("tracker").ExecuteSelect(query_purchase)[0][0]
    return int(avg_shipping_price), int(avg_total_price)


@logging_channels(['clare_test'])
@timing
def main_update_avg_shipping_purchase(web_id):
    avg_shipping_price, avg_total_price = fetch_avg_shipping_purchase(web_id)
    df_avg = pd.DataFrame()
    df_avg[['web_id', 'avg_shipping_price', 'avg_total_price']] = [[web_id, avg_shipping_price, avg_total_price]]
    ## update model parameters
    query = MySqlHelper.generate_updateTable_SQLquery('cdp_tracking_settings', df_avg.columns[1:], ['web_id'])
    #query = f"UPDATE cdp_tracking_settings SET web_id=:web_id,avg_shipping_price=:avg_shipping_price,avg_total_price=:avg_total_price WHERE web_id=:web_id"
    MySqlHelper("rheacache-db0", is_ssh=True).ExecuteUpdate(query, df_avg.to_dict('records'))
    return df_avg

@logging_channels(['clare_test'])
@timing
def main_update_model(web_id, data_use_db=True):
    test_size, n_day, lower_bound, features_select, n_weight = fetch_mining_settings(web_id)
    ## get number of coupons
    n_coupon_per_day = 50
    ## 0.get clean data
    keys_collect = ['uuid', 'session_id', 'timestamp', 'pageviews', 'time_pageview_total', 'click_count_total']
    ## left yesterday as testing set, use yesterday-1-n_day ~ yesterday-1
    if data_use_db:
        ## use db
        datetime_utc8_yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)
        date_utc8_start = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=n_day)).date())
        date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
        df_collect_group = MiningTracking.fetch_training_data(web_id, date_utc8_start, date_utc8_end, keys_collect)
    else:
        ## use local
        datetime_utc8_yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)
        date_utc8_start = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=n_day - 1)).date())
        date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
        df_collect_group = MiningTracking.get_clean_data(web_id, date_utc8_start, date_utc8_end)

    ## 1.train
    model = MiningTracking()
    model.train_logistic(df_collect_group, features_select, test_size=test_size, is_train_test_split=False)

    # ## compute probability
    # model.predict_prob(model.df_train, features_select, model.coeff, model.intercept, inplace=True)
    # ## predict 0 or 1
    # model.predict(model.df_train, lower=0.5, upper=1, inplace=True)
    # ## precision and estimate n_coupon at a day
    # precision = model.df_train.query("is_purchased==1&predict==1").shape[0] / model.df_train.query("predict==1").shape[0]
    # n_coupon_estimate = int(len(set(model.df_train.query(f"predict==1")['uuid'])) / n_day)
    # ## confusion matrix
    # model.get_confusion_matrix(np.array(model.df_train['is_purchased']), np.array(model.df_train['predict']))

    ## 2.build model to be saved
    coeff = model.coeff
    intercept = model.intercept
    df_model = pd.DataFrame()
    df_model['web_id'] = [web_id]
    df_model['model_value'] = [','.join([f"{coeff[0, i]:.5f}" for i in range(coeff.shape[1])])]
    df_model['model_intercept'] = [float(intercept)]
    ## update model parameters
    query = MySqlHelper.generate_insertDup_SQLquery(df_model, 'cdp_tracking_settings', list(df_model.columns)[1:])
    MySqlHelper("rheacache-db0", is_ssh=True).ExecuteUpdate(query, df_model.to_dict('records'))
    return df_model

if __name__ == "__main__":
    ## settings
    # web_id = "nineyi11"
    web_id_list = ["nineyi11"]
    for web_id in web_id_list:
        ## update avg shipping price and avg total price
        df_avg = main_update_avg_shipping_purchase(web_id)
        ## update model (coefficient and intercept)
        df_model = main_update_model(web_id, data_use_db=True)

        # ## settings
        # test_size, n_day, lower_bound, features_select, n_weight = fetch_mining_settings(web_id)
        # ## left yesterday as testing set, use yesterday-1-n_day ~ yesterday-1
        # # datetime_utc8_yesterday = datetime.datetime.utcnow()+datetime.timedelta(hours=8)-datetime.timedelta(days=1)
        # # date_utc8_start = datetime_to_str((datetime_utc8_yesterday-datetime.timedelta(days=n_day-1+1)).date())
        # # date_utc8_end = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=1)).date())
        # # get number of coupons
        # n_coupon_per_day = 50
        # # n_coupon_per_day *= n_weight
        #
        #
        # ## 0.get clean data
        # keys_collect = ['uuid', 'session_id', 'timestamp', 'pageviews', 'time_pageview_total', 'click_count_total']
        # # df_collect_group = MiningTracking.fetch_training_data(web_id, date_utc8_start, date_utc8_end, keys_collect)
        # # df_collect_group = MiningTracking.get_clean_data(web_id, date_utc8_start, date_utc8_end)
        #
        # ## left yesterday as testing set, use yesterday-1-n_day ~ yesterday-1
        # ## use db
        # datetime_utc8_yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)
        # date_utc8_start = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=n_day)).date())
        # date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
        # df_collect_group = MiningTracking.fetch_training_data(web_id, date_utc8_start, date_utc8_end, keys_collect)
        # ## use local
        # # datetime_utc8_yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)
        # # date_utc8_start = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=n_day - 1)).date())
        # # date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
        # # df_collect_group = MiningTracking.get_clean_data(web_id, date_utc8_start, date_utc8_end)
        #
        #
        # ## 1.train
        # model = MiningTracking()
        # model.train_logistic(df_collect_group, features_select, test_size=test_size, is_train_test_split=False)
        #
        # # ## compute probability
        # # model.predict_prob(model.df_train, features_select, model.coeff, model.intercept, inplace=True)
        # # ## predict 0 or 1
        # # model.predict(model.df_train, lower=0.5, upper=1, inplace=True)
        # # ## precision and estimate n_coupon at a day
        # # precision = model.df_train.query("is_purchased==1&predict==1").shape[0] / model.df_train.query("predict==1").shape[0]
        # # n_coupon_estimate = int(len(set(model.df_train.query(f"predict==1")['uuid'])) / n_day)
        # # ## confusion matrix
        # # model.get_confusion_matrix(np.array(model.df_train['is_purchased']), np.array(model.df_train['predict']))
        #
        # ## 2.build model to be saved
        # features = model.features_select
        # coeff = model.coeff
        # intercept = model.intercept
        # df_model = pd.DataFrame()
        # df_model['web_id'] = [web_id]
        # df_model['model_value'] = [','.join([f"{coeff[0,i]:.5f}" for i in range(coeff.shape[1])])]
        # df_model['model_intercept'] = [float(intercept)]
        #
        # # ## update model parameters
        # query = MySqlHelper.generate_insertDup_SQLquery(df_model, 'cdp_tracking_settings', list(df_model.columns)[1:])
        # MySqlHelper("rheacache-db0", is_ssh=True).ExecuteUpdate(query, df_model.to_dict('records'))



