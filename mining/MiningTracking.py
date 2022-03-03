from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict, to_datetime
from definitions import ROOT_DIR
from db import MySqlHelper
import datetime, os, pickle, json
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
import matplotlib.pyplot as plt
from s3_parser import TrackingParser
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_curve
from sklearn import metrics

"""
protocol:
    ## 0.get clean data according to 
    ## 1.train model
    ## 2.compute probability
    ## 3.compute upper bound at given lower bound
    ## 4-1.predict 0 or 1
    ## 4-2.precision and estimate n_coupon at a day
    ## 5.confusion matrix
    ## 6.ROC curve
    ## 7.build model to be saved
"""


class MiningTracking:
    @staticmethod
    def get_clean_data(web_id, date_utc8_start, date_utc8_end, keys_collect=None):
        tracking = TrackingParser(web_id, date_utc8_start, date_utc8_end)
        df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = tracking.get_six_events_df()
        if keys_collect==None:
            # keys_collect = ['uuid', 'session_id', 'url_last', 'url_now', 'timestamp',
            #                 'device', 'pageviews', 'time_pageview_total', 'landing_count', 'click_count_total',
            #                 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth',
            #                 'is_purchased',
            #                 'max_time_no_move_last', 'max_time_no_scroll_last', 'max_time_no_click_last', 'is_addCart',
            #                 'is_removeCart']
            keys_collect = ['uuid', 'session_id', 'timestamp', 'pageviews', 'time_pageview_total',
                            'click_count_total', 'is_addCart', 'is_removeCart', 'is_purchased']
        df_loaded, df_purchased, df_addCart, df_removeCart = clean_df_uuid(df_loaded, df_purchased, df_addCart,
                                                                           df_removeCart)
        df_clean = append_3actions_by_uuid_session(df_loaded, df_purchased, df_addCart, df_removeCart, keys_collect)
        return df_clean

    @staticmethod
    @timing
    def fetch_training_data(web_id, date_utc8_start, date_utc8_end, keys_collect=None):
        if keys_collect == None:
            keys_collect = ['uuid', 'session_id', 'timestamp', 'pageviews', 'time_pageview_total',
                            'click_count_total']
        df_loaded = MiningTracking.fetch_event_data(web_id, date_utc8_start, date_utc8_end, 'load', keys_collect)
        df_purchased = MiningTracking.fetch_event_data(web_id, date_utc8_start, date_utc8_end, 'purchase', keys_collect)
        df_addCart = MiningTracking.fetch_event_data(web_id, date_utc8_start, date_utc8_end, 'addCart', keys_collect)
        df_removeCart = MiningTracking.fetch_event_data(web_id, date_utc8_start, date_utc8_end, 'removeCart', keys_collect)

        df_loaded, df_purchased, df_addCart, df_removeCart = clean_df_uuid(df_loaded, df_purchased, df_addCart,
                                                                           df_removeCart)
        keys_collect += ['is_addCart', 'is_removeCart', 'is_purchased']
        df_clean = append_3actions_by_uuid_session(df_loaded, df_purchased, df_addCart, df_removeCart, keys_collect)

        return df_clean

    ## if web_id==None, fetch all web_id
    @staticmethod
    @timing
    def fetch_event_data(web_id, date_utc8_start, date_utc8_end, event_type, keys_collect):
        table_dict = {'load':'clean_event_load', 'addCart':'clean_event_addCart', 'removeCart':'clean_event_removeCart',
                      'leave':'clean_event_leave', 'purchase':'clean_event_purchase', 'timeout':'clean_event_timeout'}

        if event_type in table_dict.keys():
            table = table_dict[event_type]
            if web_id==None or web_id=='default':
                query = f"""
                        SELECT {','.join(keys_collect)} FROM {table}
                        WHERE date_time BETWEEN '{date_utc8_start}' and '{date_utc8_end}'
                        """
            else:
                query = f"""
                        SELECT {','.join(keys_collect)} FROM {table}
                        WHERE date_time BETWEEN '{date_utc8_start}' and '{date_utc8_end}' and web_id='{web_id}'
                        """
            print(query)
            data = MySqlHelper('tracker').ExecuteSelect(query)
            df = pd.DataFrame(data, columns=keys_collect)
            return df
        else:
            print('not valid even_type')
            return pd.DataFrame()

    def train_logistic(self, df, features_select, power_features=None, is_train_test_split=True, test_size=0.3, random_state=25):
        # features_select = ['pageviews', 'time_pageview_total', 'click_count_total', 'is_addCart', 'is_removeCart']
        # features_select = ['pageviews', 'time_pageview_total', 'click_count_total']
        features_select = ['uuid'] + features_select
        ## add uuid
        X = np.array(df[features_select])
        X[:,1:] = X[:,1:].astype('float')
        if power_features==None or len(power_features)!=len(features_select):
            print(f"power_features are not fit with features_select or power_features are not assigned, do nothing")
        else:
            for i,power in enumerate(power_features, 1):
                X[:, i] = pow(X[:, i], power)
        y = np.array(df['is_purchased']).astype('int')
        if is_train_test_split:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
            ## ignore uuid column
            model = LogisticRegression(random_state=0).fit(X_train[:,1:], y_train)
            self.df_train, self.df_test = pd.DataFrame(data=X_train, columns=features_select), pd.DataFrame(data=X_test, columns=features_select)
            self.df_train['is_purchased'], self.df_test['is_purchased'] = y_train, y_test
        else:
            X_train, y_train = X, y
            # X_test, y_test = X, y
            ## ignore uuid column
            model = LogisticRegression(random_state=0).fit(X_train[:, 1:], y_train)
            self.df_train = pd.DataFrame(data=X_train, columns=features_select)
            self.df_train['is_purchased'] = y_train

        ## ignore uuid column
        # model = LogisticRegression(random_state=0).fit(X_train[:,1:], y_train)
        # self.df_train, self.df_test = pd.DataFrame(data=X_train, columns=features_select), pd.DataFrame(data=X_test, columns=features_select)
        # self.df_train['is_purchased'], self.df_test['is_purchased'] = y_train, y_test

        self.features_select = features_select[1:]
        self.coeff = model.coef_
        self.intercept = model.intercept_

    @staticmethod
    def predict_prob(df, features_select, coeff, intercept, inplace=False):
        if inplace:
            df['prob'] = MiningTracking.cal_prob(np.array(df[features_select]).astype('float'), coeff, intercept)
        else:
            df_prob = df.copy()
            df_prob['prob'] = MiningTracking.cal_prob(np.array(df_prob[features_select]).astype('float'), coeff, intercept)
            return df_prob

    @staticmethod
    def predict(df_prob, lower=0.5, upper=1.0, inplace=False):
        if inplace:
            df_prob['predict'] = [1 if row['prob']>=lower and row['prob']<=upper else 0 for i,row in df_prob.iterrows()]
        else:
            df_predict = df_prob.copy()
            df_predict['predict'] = [1 if row['prob']>=lower and row['prob']<=upper else 0 for i,row in df_prob.iterrows()]
            return df_predict

    @staticmethod
    def get_confusion_matrix(y, y_predict):
        confusion = confusion_matrix(y, y_predict)
        print(confusion)

    @staticmethod
    def get_ROC_curve(y_test, y_prob):
        fpr, tpr, _ = metrics.roc_curve(y_test, y_prob)
        # create ROC curve
        plt.figure()
        plt.plot(fpr, tpr)
        plt.ylabel('True Positive Rate')
        plt.xlabel('False Positive Rate')
        plt.xlim([0,1.01])
        plt.ylim([0,1.01])
        plt.show()
        return fpr, tpr

    @staticmethod
    def cal_prob(X, coeff, intercept):
        """

        Parameters
        ----------
        X: shape (n,m), n is sample size and m is number of features
        coeff: shape (1,m), coefficient of logistic regression
        intercept: shape (1,), intercept of logistic regression
        Returns
        prob: shape (n,1), positive probability
        -------

        """
        prob = 1 / (1 + np.exp(-(np.sum(X * coeff, axis=1) + intercept)))
        return prob

    @staticmethod
    def check_correlation(df, feature_list=None, corr_with='is_purchased'):
        if feature_list==None:
            return df.corr()[corr_with][:]
        else:
            return df[feature_list].corr()[corr_with][:]

    @staticmethod
    def get_precision(df):
        return df.query("is_purchased==1&predict==1").shape[0] / df.query("predict==1").shape[0]



@timing
def fetch_91app_web_id():
    query = f"""SELECT web_id
                        FROM cdp_tracking_settings where platform='91'"""
    print(query)
    data = MySqlHelper("rheacache-db0").ExecuteSelect(query)
    web_id_list = [d[0] for d in data]
    return web_id_list


def collect_from_web_id_list(web_id_list, date_utc8_start, date_utc8_end):
    df_loaded_all, df_purchased_all = pd.DataFrame(), pd.DataFrame()
    df_addCart_all, df_removeCart_all = pd.DataFrame(), pd.DataFrame()

    len_loaded = {}
    for i,web_id in enumerate(web_id_list):
        tracking = TrackingParser(web_id, date_utc8_start, date_utc8_end)

        df_loaded, df_purchased = tracking.df_loaded, tracking.df_purchased
        df_addCart, df_removeCart = tracking.df_addCart, tracking.df_removeCart

        l = df_loaded.shape[0]
        print(f"{web_id} with loaded event length: {l}")
        len_loaded.update({web_id:l})
        if l==0: ## no available data
            continue

        # if check_df_shape_is_zero(df_loaded_all, df_purchased_all, df_addCart_all, df_removeCart_all):
        if df_loaded_all.shape[0]==0:
            df_loaded_all = df_loaded
        else:
            df_loaded_all = df_loaded_all.append(df_loaded)

        if df_purchased_all.shape[0]==0:
            df_purchased_all = df_purchased
        else:
            df_purchased_all = df_purchased_all.append(df_purchased)

        if df_addCart_all.shape[0]==0:
            df_addCart_all = df_addCart
        else:
            df_addCart_all = df_addCart_all.append(df_addCart)

        if df_removeCart_all.shape[0]==0:
            df_removeCart_all = df_removeCart
        else:
            df_removeCart_all = df_removeCart_all.append(df_removeCart)
        #
        # else:
        #     df_loaded_all, df_purchased_all, df_addCart_all, df_removeCart_all = append_multi(
        #         [df_loaded, df_loaded_all], [df_purchased, df_purchased_all],
        #         [df_addCart, df_addCart_all], [df_removeCart, df_removeCart_all])

    return df_loaded_all, df_purchased_all, df_addCart_all, df_removeCart_all, len_loaded



# def collect_features(df_loaded, df_purchased, keys_collect=['uuid', 'pageviews', 'time_pageview_total']):
#     uuid_load = list(set(df_loaded['uuid']))
#     uuid_purchased = list(set(df_purchased['uuid']))
#     dict_collect_list = []
#     # keys_collect = ['uuid', 'device', 'pageviews', 'time_pageview_total', 'landing_count',
#     #                 'click_count_total', 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth']
#     # keys_collect = ['uuid', 'pageviews', 'time_pageview_total']
#     for i,uuid in enumerate(uuid_load):
#         dict_collect = {}
#         df_query = df_loaded.query(f"uuid=='{uuid}'").iloc[-1]
#         for key in keys_collect:
#             vlaue = df_query[key]
#             dict_collect.update({key: vlaue})
#             if key=='uuid':
#                 if vlaue in uuid_purchased:
#                     dict_append = {'is_purchased': 1}
#                 else:
#                     dict_append = {'is_purchased': 0}
#                 dict_collect.update(dict_append)
#
#         dict_collect_list += [dict_collect]
#         if i%100==0:
#             print(f"finish {i}")
#     df_collect = pd.DataFrame(dict_collect_list)
#     return df_collect

def binning2(data, binwidth, start=None, end=None, xlabel='value', ylabel='probability density', show=True, density=True):
    if start==None:
        start = min(data)
    if end == None:
        end = max(data)
    bin_edge = np.arange(start, end+1, binwidth)
    center = np.arange(start+binwidth/2, end-binwidth/2+1, binwidth)
    fig, ax = plt.subplots(figsize=(10, 8))
    pd, edges, patches = plt.hist(data, bins=bin_edge, density=density)
    ax.bar(center, pd, width=binwidth, color="silver", edgecolor="white")
    ax.set_xlabel(f'{xlabel}', fontsize=22)
    ax.set_ylabel(f'{ylabel}', fontsize=22)
    if show==False:
        plt.close(fig)
    return pd, center, fig, ax

def visualization_feature(df_collect, feature='pageviews', binwidth=1, bin_start=0, bin_end=50, y_low=None, y_high=None):
    feature_values_purchased = np.array(df_collect[df_collect['is_purchased']==1][feature]).astype('int')
    pd1, center1, fig, ax = binning2(feature_values_purchased, binwidth=binwidth, start=bin_start, end=bin_end, show=False)

    feature_values_not_purchased = np.array(df_collect[df_collect['is_purchased']==0][feature]).astype('int')
    pd0, center0, fig, ax = binning2(feature_values_not_purchased, binwidth=binwidth, start=bin_start, end=bin_end, show=False)

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.bar(center0, pd0, width=binwidth, color="blue", edgecolor="white", alpha=0.5)
    ax.bar(center1, pd1, width=binwidth, color="red", edgecolor="white", alpha=0.5)
    ax.set_xlabel(f'{feature}', fontsize=22)
    ax.set_ylabel(f'probability density', fontsize=22)
    plt.xlim([bin_start, bin_end])
    if y_low!=None and y_high!=None:
        plt.ylim([y_low, y_high])
    plt.show()


### normalize to mean = 0, std = 1
def normalize_data(data):
    data = np.array(data)
    data_nor = []
    for datum in data.T:
        mean = np.mean(datum)
        std = np.std(datum, ddof=1)
        datum_nor = (datum-mean)/std
        data_nor += [datum_nor]
    return np.nan_to_num(np.array(data_nor).T)

def append_purchased_column(df, uuid_purchased):
    df_collect = df.copy()
    is_purchased_list = [1 if row['uuid'] in uuid_purchased else 0 for i,row in df_collect.iterrows()]
    df_collect['is_purchased'] = is_purchased_list
    return df_collect

def append_action_column(df, uuid_event, col_name='is_purchased'):
    df_collect = df.copy()
    is_acted_list = [1 if row['uuid'] in uuid_event else 0 for i,row in df_collect.iterrows()]
    df_collect[col_name] = is_acted_list
    return df_collect

def append_action_session_column(df, dict_uuid_session, col_name='is_purchased'):
    df_collect = df.copy()
    uuid_list = list(dict_uuid_session.keys())
    is_acted_list = [1 if row['uuid'] in uuid_list and row['session_id'] in dict_uuid_session[row['uuid']] else 0 for i,row in df_collect.iterrows()]
    df_collect[col_name] = is_acted_list
    return df_collect


def clean_df_uuid(*args):
    results_clean = []
    for arg in args:
        arg['uuid'] = arg['uuid'].astype(str)
        arg = arg[arg['uuid'].map(len) == 36]
        results_clean += [arg]
    return results_clean


def check_correlation(df, feature_list, corr_with='is_purchased'):
    return df[feature_list].corr()[corr_with][:]


def get_unique(key, *args):
    results = []
    for arg in args:
        results += [list(set(arg[key]))]
    return results



# def append_multi(*args):
#     """
#
#     Parameters
#     ----------
#     args: put [[df1, df1_to_be_append],[df2, df2_to_be_append], ...]
#
#     Returns: [df1_to_be_append, df2_to_be_append, df3_to_be_append,...]
#     -------
#
#     """
#     results = []
#     for arg in args:
#         arg[1] = arg[1].append(arg[0])
#         results += [arg[1]]
#     return results

## collect uuid with session_list
def get_session_dict(df):
    dict_uuid_session = {}
    for i, row in df.iterrows():
        uuid, session_id = row['uuid'], row['session_id']
        if uuid in dict_uuid_session.keys(): ## old uuid, append
            dict_uuid_session[uuid] += [session_id]
        else: ## new uuid, add
            dict_uuid_session[uuid] = [session_id]
    return dict_uuid_session


def append_3actions_by_uuid_session(df_loaded, df_purchased, df_addCart, df_removeCart, keys_collect=None):
    dict_uuid_session_purchased = {row['uuid']: row['session_id'] for i, row in df_purchased.iterrows()}
    dict_uuid_session_addCart = {row['uuid']: row['session_id'] for i, row in df_addCart.iterrows()}
    dict_uuid_session_removeCart = {row['uuid']: row['session_id'] for i, row in df_removeCart.iterrows()}
    if keys_collect==None:
        keys_collect = ['uuid', 'session_id', 'url_last', 'url_now', 'timestamp', 'device', 'pageviews',
                        'time_pageview_total', 'landing_count',
                        'click_count_total', 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total',
                        'max_scroll_depth', 'is_purchased', 'is_addCart', 'is_removeCart']
    ## add is_purchased, is_addCart, is_removeCart actions to df
    df_collect2 = append_action_session_column(df_loaded, dict_uuid_session_purchased, col_name='is_purchased')
    df_collect2 = append_action_session_column(df_collect2, dict_uuid_session_addCart, col_name='is_addCart')
    df_collect2 = append_action_session_column(df_collect2, dict_uuid_session_removeCart, col_name='is_removeCart')

    # df_collect_clean2 = df_collect2[keys_collect][df_collect2['landing_count']>0].sort_values(by=['uuid', 'timestamp'])
    df_collect_clean2 = df_collect2[keys_collect].sort_values(by=['uuid', 'timestamp'])
    df_collect_group2 = df_collect_clean2.groupby(["uuid", "session_id"]).last().reset_index()
    return df_collect_group2

def append_3actions_by_uuid(df_loaded, df_purchased, df_addCart, df_removeCart, keys_collect=None):
    uuid_load, uuid_purchased, uuid_addCart, uuid_removeCart = get_unique('uuid', df_loaded, df_purchased, df_addCart, df_removeCart)

    if keys_collect==None:
        keys_collect = ['uuid', 'session_id', 'url_last', 'url_now', 'timestamp', 'device', 'pageviews',
                        'time_pageview_total', 'landing_count',
                        'click_count_total', 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total',
                        'max_scroll_depth', 'is_purchased', 'is_addCart', 'is_removeCart']

    df_collect = append_action_column(df_loaded, uuid_purchased, col_name='is_purchased')
    df_collect = append_action_column(df_collect, uuid_addCart, col_name='is_addCart')
    df_collect = append_action_column(df_collect, uuid_removeCart, col_name='is_removeCart')

    df_collect_clean = df_collect[keys_collect][df_collect['landing_count']>0].sort_values(by=['uuid', 'timestamp'])
    df_collect_group = df_collect_clean.groupby("uuid").last().reset_index()
    return df_collect_group





@timing
def fetch_mining_settings(web_id):
    query = f"SELECT test_size,n_day_mining,lower_bound,model_key,n_weight FROM cdp_tracking_settings where web_id='{web_id}'"
    data = MySqlHelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
    test_size, n_day, lower_bound, features_join, n_weight = data[0]
    features_select = features_join.split(',')
    return test_size, n_day, lower_bound, features_select, n_weight



## main for mining tracker data
if __name__ == "__main__":
    ## settings
    web_id = "nineyi11"
    ## settings
    test_size, n_day, lower_bound, features_select, n_weight = fetch_mining_settings(web_id)
    ## left yesterday as testing set, use yesterday-1-n_day ~ yesterday-1
    datetime_utc8_yesterday = datetime.datetime.utcnow()+datetime.timedelta(hours=8)-datetime.timedelta(days=1)
    date_utc8_start = datetime_to_str((datetime_utc8_yesterday-datetime.timedelta(days=n_day-1+1)).date())
    date_utc8_end = datetime_to_str((datetime_utc8_yesterday - datetime.timedelta(days=1)).date())
    # get number of coupons
    n_coupon_per_day = 50
    # n_coupon_per_day *= n_weight
    ## get clean data
    keys_collect = ['uuid', 'session_id', 'timestamp', 'pageviews', 'time_pageview_total',
                    'click_count_total']
    ## use db
    # datetime_utc8_yesterday = datetime.datetime.utcnow()+datetime.timedelta(hours=8)-datetime.timedelta(days=1)
    # date_utc8_start = datetime_to_str((datetime_utc8_yesterday-datetime.timedelta(days=n_day)).date())
    # date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
    # df_collect_group = MiningTracking.fetch_training_data(web_id, date_utc8_start, date_utc8_end, keys_collect)
    ## use local
    datetime_utc8_yesterday = datetime.datetime.utcnow()+datetime.timedelta(hours=8)-datetime.timedelta(days=1)
    date_utc8_start = datetime_to_str((datetime_utc8_yesterday-datetime.timedelta(days=n_day-1)).date())
    date_utc8_end = datetime_to_str(datetime_utc8_yesterday.date())
    df_collect_group = MiningTracking.get_clean_data(web_id, date_utc8_start, date_utc8_end)

    # df_collect_group = MiningTracking.get_clean_data(web_id, date_utc8_start, date_utc8_end)

    ## 1.train
    model = MiningTracking()
    # model.train_logistic(df_collect_group, features_select, [1, 0.5, 1, 1, 1], test_size=test_size)
    model.train_logistic(df_collect_group, features_select, [1, 1, 1, 1, 1], test_size=test_size, is_train_test_split=False)

    ## 2.compute probability
    model.predict_prob(model.df_test, features_select, model.coeff, model.intercept, inplace=True)
    model.predict_prob(model.df_train, features_select, model.coeff, model.intercept, inplace=True)


    ## 3.compute upper bound at given lower bound
    n_cum, prob = np.histogram(np.array(model.df_test.query(f"prob>{lower_bound}")['prob']), bins=50)
    n_cum_weight = n_cum/test_size/n_day ## normalize to traffic of a day
    cumulative = np.cumsum(n_cum_weight)

    poly_coeff = np.polyfit(x=cumulative, y=prob[:-1], deg=4)
    equation = np.poly1d(poly_coeff)
    upper_bound = equation(n_coupon_per_day*n_weight)
    print(f"result of selecting probability between {lower_bound:.3f} and {upper_bound:.3f}")

    plt.figure(figsize=(10,8))
    plt.plot(cumulative, prob[:-1], 'bo')
    plt.plot(cumulative, equation(cumulative), 'r--')
    plt.xlabel('number of coupons per day')
    plt.ylabel('set upper bound at given lower bound')
    plt.show()

    ## 4-1.predict 0 or 1
    model.predict(model.df_test, lower=lower_bound, upper=upper_bound, inplace=True)
    model.predict(model.df_train, lower=lower_bound, upper=upper_bound, inplace=True)
    ## 4-2.precision and estimate n_coupon at a day
    precision = model.df_test.query("is_purchased==1&predict==1").shape[0] / model.df_test.query("predict==1").shape[0]
    n_coupon_estimate = int(len(set(model.df_test.query(f"predict==1")['uuid']))/test_size/n_day)

    ## 5.confusion matrix
    model.get_confusion_matrix(np.array(model.df_test['is_purchased']), np.array(model.df_test['predict']))
    model.get_confusion_matrix(np.array(model.df_train['is_purchased']), np.array(model.df_train['predict']))
    ## 6.ROC curve
    model.get_ROC_curve(np.array(model.df_test['is_purchased']), np.array(model.df_test['prob']))
    model.get_ROC_curve(np.array(model.df_train['is_purchased']), np.array(model.df_train['prob']))
    ## 7.build model to be saved
    features = model.features_select
    coeff = model.coeff
    intercept = model.intercept

    df_model = pd.DataFrame()
    df_model['web_id'] = [web_id]
    # df_model['model_type'] = ['logistic']
    # df_model['model_key'] = [','.join(features)]
    # df_model['model_key_js'] = [','.join(features_js)]
    df_model['model_value'] = [','.join([f"{coeff[0,i]:.5f}" for i in range(coeff.shape[1])])]
    df_model['model_intercept'] = [float(intercept)]
    df_model['upper_bound'] = [upper_bound]
    df_model['model_precision'] = [precision]
    df_model['n_coupon_estimate'] = [n_coupon_estimate]
    # ## update model parameters
    # query = MySqlHelper.generate_insertDup_SQLquery(df_model, 'cdp_tracking_settings', list(df_model.columns)[1:])
    # MySqlHelper("rheacache-db0", is_ssh=True).ExecuteUpdate(query, df_model.to_dict('records'))


    # ## PCA, data visualization
    # data = normalize_data(np.array(df_collect_predict[features_select]))
    # label = np.array(df_collect_predict['is_purchased']).astype(int)
    # pca = PCA(n_components=2)
    # result = pca.fit(data)
    # transform_data = result.transform(data)
    #
    # plt.figure()
    # plt.plot(transform_data[label==0,0], transform_data[label==0,1], 'bo')
    # plt.plot(transform_data[label==1,0], transform_data[label==1,1], 'r*')
    # plt.xlabel('PC1')
    # plt.ylabel('PC2')
    # plt.show()


    # session_purchased = {row['uuid']:[row['session_id_last'],row['session_id']] for i,row in df_purchased.iterrows()}
    # dict_collect_list = []
    # keys_collect = ['uuid', 'device', 'pageviews', 'time_pageview_total', 'landing_count',
    #                 'click_count_total', 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth']
    #
    # df_loaded_purchased = df_loaded.query(f"uuid in {uuid_purchased}").sort_values(by=['uuid', 'timestamp'])
    # keys_collect = ['uuid', 'max_pageviews', 'max_time_pageview_total', 'click_count_total']
    # for i,uuid in enumerate(uuid_load):
    #     dict_collect = {}
    #     df_query = df_loaded.query(f"uuid=='{uuid}'").iloc[-1]
    #     for key in keys_collect:
    #         vlaue = df_query[key]
    #         dict_collect.update({key: vlaue})
    #         if key=='uuid':
    #             if vlaue in uuid_purchased:
    #                 dict_append = {'is_purchased': 1}
    #             else:
    #                 dict_append = {'is_purchased': 0}
    #             dict_collect.update(dict_append)
    #
    #     dict_collect_list += [dict_collect]
    #     if i%100==0:
    #         print(f"finish {i}")
    # df_collect = pd.DataFrame(dict_collect_list)
    # df_collect = append_purchased_column(df_loaded, uuid_purchased)[keys_collect+['is_purchased']]

    #
    # from sklearn.decomposition import PCA
    # # data_purchased = normalize_data(np.array(df_collect.query(f"is_purchased==1"))[:,2:])
    # data_purchased = np.array(df_collect.query(f"is_purchased==1"))[:,2:]
    #
    # pca = PCA(n_components=2)
    # result = pca.fit(data_purchased)
    # transform_purchased = result.transform(data_purchased)
    #
    # # data_not_purchased = normalize_data(np.array(df_collect.query(f"is_purchased==0"))[:,2:])
    # data_not_purchased = np.array(df_collect.query(f"is_purchased==0"))[:,2:]
    #
    # pca = PCA(n_components=2)
    # result = pca.fit(data_not_purchased)
    # transform_not_purchased = result.transform(data_not_purchased)
    # plt.figure()
    # plt.plot(transform_not_purchased[:,0], transform_not_purchased[:,1], 'bo')
    # plt.plot(transform_purchased[:,0], transform_purchased[:,1], 'ro')
    # plt.show()
    #
    #
    #
    # data = normalize_data(np.array(df_collect)[:,6:])
    # label = np.array(df_collect)[:,1]
    # pca = PCA(n_components=2)
    # result = pca.fit(data)
    # transform_data = result.transform(data)
    #
    # plt.figure()
    # plt.plot(transform_data[label==0,0], transform_data[label==0,1], 'bo')
    # plt.plot(transform_data[label==1,0], transform_data[label==1,1], 'r*')
    # plt.show()
    #
    #
    # ## clustering
    # from sklearn.mixture import GaussianMixture
    # data = np.array(df_collect)[:,6:]
    # data_nor = normalize_data(data)
    # label = np.array(df_collect)[:,1]
    # pca = PCA(n_components=2)
    # result = pca.fit(data_nor)
    # transform_data = result.transform(data_nor)
    #
    # gmm = GaussianMixture(n_components=2, tol=1e-5, init_params='random')
    # # model = Birch(threshold=0.05, n_clusters=5)
    # ##  fit the model
    # gmm.fit(transform_data)
    # ## assign a cluster to each example
    # label = gmm.predict(transform_data)
    # plt.figure()
    # data_passive, data_active = [], []
    # for i, (row,l) in enumerate(zip(transform_data,label)):
    #     if row[1]>0 and row[0]>0: ## class 1
    #         plt.plot(row[0], row[1], 'r*')
    #         data_passive += [data[i]]
    #     else:
    #         plt.plot(row[0], row[1], 'bo')
    #         data_active += [data[i]]
    # plt.show()
    # data_passive, data_active = np.array(data_passive).astype(int), np.array(data_active).astype(int)
    #
    # plt.figure()
    # for i, (row,l) in enumerate(zip(transform_data,label)):
    #     if l==1: ## class 1
    #         plt.plot(row[0], row[1], 'r*')
    #     else:
    #         plt.plot(row[0], row[1], 'bo')
    # plt.show()




    #
    #
    # web_id_list = fetch_91app_web_id()
    # # web_id_list = ['nineyi1105', 'nineyi11185', 'nineyi123', 'nineyi14267', 'nineyi1849']
    # date_utc8_start = "2022-01-19"
    # date_utc8_end = "2022-01-19"
    # df_loaded_all, df_purchased_all, len_loaded = collect_from_web_id_list(web_id_list, date_utc8_start, date_utc8_end)
    # keys_collect = ['uuid', 'device', 'pageviews', 'time_pageview_total', 'landing_count', 'click_count_total',
    #                 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth']
    # df_collect = collect_features(df_loaded_all, df_purchased_all, keys_collect=keys_collect)
    # df_collect['mean_max_time_per_page'] = df_collect['max_time_pageview_total']/df_collect['max_pageviews']
    # keys_select = ['max_pageviews', 'landing_count', 'click_count_total', 'max_time_pageview_total', 'max_scroll_depth']
    # X = np.array(df_collect[keys_select]).astype('int')
    # y = np.array(df_collect['is_purchased']).astype('int')
    #
    # model = LogisticRegression(random_state=0).fit(X, y)
    # prob = model.predict_proba(X)
    # predict = model.predict(X)
    # df_collect_predict = df_collect.copy()
    # df_collect_predict['prob'] = prob[:,1]
    # df_collect_predict['predict'] = predict
    #
    # model.score(X, y)
    # visualization_feature(df_collect, feature='device', binwidth=1, bin_start=0, bin_end=5, y_low=0, y_high=0.5)
    # visualization_feature(df_collect, feature='pageviews', binwidth=1, bin_start=0, bin_end=50, y_low=0, y_high=0.2)
    # visualization_feature(df_collect, feature='time_pageview_total', binwidth=50, bin_start=0, bin_end=1000, y_low=0, y_high=0.01)
    # visualization_feature(df_collect, feature='landing_count', binwidth=1, bin_start=0, bin_end=30, y_low=0, y_high=0.6)
    # visualization_feature(df_collect, feature='click_count_total', binwidth=2, bin_start=0, bin_end=150, y_low=0, y_high=0.1)
    # visualization_feature(df_collect, feature='max_pageviews', binwidth=1, bin_start=0, bin_end=50, y_low=0, y_high=0.2)
    # visualization_feature(df_collect, feature='max_time_pageview', binwidth=50, bin_start=0, bin_end=2000, y_low=0, y_high=0.015)
    # visualization_feature(df_collect, feature='max_time_pageview_total', binwidth=100, bin_start=0, bin_end=5000, y_low=0, y_high=0.008)
    # visualization_feature(df_collect, feature='max_scroll_depth', binwidth=1, bin_start=0, bin_end=100, y_low=0, y_high=0.8)
    #
    # visualization_feature(df_collect, feature='mean_max_time_per_page', binwidth=5, bin_start=0, bin_end=200, y_low=0, y_high=0.1)
