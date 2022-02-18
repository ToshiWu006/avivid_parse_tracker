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
def fetch_train_model(web_id):
    query = f"SELECT model_value,model_intercept FROM cdp_tracking_settings where web_id='{web_id}'"
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    model_coeff_join, intercept = data[0]
    coeff = np.array(model_coeff_join.split(',')).astype(float)
    return coeff, intercept




@logging_channels(['clare_test'])
@timing
def main_update_purchcase_bound(web_id, data_use_db=True):
    ## settings
    test_size, n_day, lower_bound, features_select, n_weight = fetch_mining_settings(web_id)
    del test_size, n_day
    n_day = 1  ## use 1 for update bound
    ## get number of coupons
    n_coupon_per_day = 50
    ## get clean data
    keys_collect = ['uuid', 'session_id', 'timestamp', 'pageviews', 'time_pageview_total', 'click_count_total']
    if data_use_db:
        ## use db
        datetime_utc8_yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)
        date_utc8_start = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=n_day)).date())
        date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
        df_test = MiningTracking.fetch_training_data(web_id, date_utc8_start, date_utc8_end, keys_collect)
    else:
        # use local
        datetime_utc8_yesterday = datetime.datetime.utcnow()+datetime.timedelta(hours=8)-datetime.timedelta(days=1)
        date_utc8_start = datetime_to_str((datetime_utc8_yesterday-datetime.timedelta(days=n_day-1)).date())
        date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
        df_test = MiningTracking.get_clean_data(web_id, date_utc8_start, date_utc8_end)
    ## remove repeat uuid
    df_test.drop_duplicates(subset=['uuid'], inplace=True)

    ## 1. fetch model
    coeff, intercept = fetch_train_model(web_id)
    ## 2.compute probability
    model = MiningTracking()
    model.predict_prob(df_test, features_select, coeff, intercept, inplace=True)
    ## 3.compute upper bound at given lower bound
    n_cum, prob = np.histogram(np.array(df_test['prob']), bins=50)
    n_cum_weight = n_cum / n_day  ## normalize to traffic of a day
    cumulative = sum(n_cum_weight) - np.cumsum(n_cum_weight)
    prob = prob[:-1]
    cumulative = cumulative[prob > 0.1]
    prob = prob[prob > 0.1]
    poly_coeff = np.polyfit(x=cumulative, y=prob, deg=10)
    equation = np.poly1d(poly_coeff)
    lower_bound = equation(n_coupon_per_day * n_weight)

    # upper_bound = equation(n_coupon_per_day*n_weight)
    print(f"result of selecting probability greater than {lower_bound:.3f} "
          f"and n_coupon_per_day is weighted with {n_weight} (={n_coupon_per_day * n_weight})")
    # plt.figure(figsize=(10,8))
    # plt.plot(cumulative, prob, 'bo')
    # plt.plot(cumulative, equation(cumulative), 'r--')
    # plt.xlabel('number of coupons per day')
    # plt.ylabel('set lower bound')
    # plt.show()

    ## 4-1.predict 0 or 1
    model.predict(df_test, lower=lower_bound, upper=1.0, inplace=True)
    ## 4-2.precision and estimate n_coupon at a day
    precision = df_test.query("is_purchased==1&predict==1").shape[0] / df_test.query("predict==1").shape[0]
    n_coupon_estimate = int(len(set(df_test.query(f"predict==1")['uuid'])) / n_day)
    print(
        f"test for model precision is {precision}, estimating number of coupon sent in one day will be {n_coupon_estimate}")
    ## 5.confusion matrix
    model.get_confusion_matrix(np.array(df_test['is_purchased']), np.array(df_test['predict']))
    ## 6.ROC curve
    # model.get_ROC_curve(np.array(df_test['is_purchased']), np.array(df_test['prob']))
    ## 7.build model to be saved
    df_model = pd.DataFrame()
    df_model['web_id'] = [web_id]
    df_model['lower_bound'] = [lower_bound]
    df_model['model_precision'] = [precision]
    df_model['n_coupon_estimate'] = [n_coupon_estimate]
    ## update model parameters
    query = MySqlHelper.generate_updateTable_SQLquery('cdp_tracking_settings', df_model.columns[1:], ['web_id'])
    MySqlHelper("rheacache-db0", is_ssh=True).ExecuteUpdate(query, df_model.to_dict('records'))


if __name__ == "__main__":

    ## settings
    # web_id = "nineyi11"
    web_id_list = ["nineyi11"]
    for web_id in web_id_list:
        main_update_purchcase_bound(web_id, data_use_db=True)


