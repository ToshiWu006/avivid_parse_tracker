from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict, to_datetime
from definitions import ROOT_DIR
from db import DBhelper
import datetime, os, pickle, json, collections
import pandas as pd
import re
import numpy as np

class TrackingParser:
    def __init__(self, web_id=None, date_utc8_start=None, date_utc8_end=None):
        self.web_id = web_id
        self.event_type_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart',
                                'purchase', 'sendCoupon', 'acceptCoupon', 'discardCoupon',
                                'enterCoupon', 'acceptAf']
        self.dict_object_key = {"load":"load", "leave":"", "timeout":"",
                                "addCart":"cart", "removeCart":"remove_cart", "purchase":"purchase",
                                "sendCoupon":"coupon_info", "acceptCoupon":"coupon_info",
                                "discardCoupon":"coupon_info", "enterCoupon":"coupon_info",
                                "acceptAf":"adaf_info"}
        self.dict_settings = self.fetch_parse_key_settings(web_id)
        self.dict_settings_all = None
        # self.dict_settings_all = self.fetch_parse_key_all_settings()
        self.date_utc8_start = date_utc8_start
        self.date_utc8_end = date_utc8_end
        self.data_list = self.get_data_by_daterange(date_utc8_start, date_utc8_end)

    def __str__(self):
        return "TrackingParser"

    @classmethod
    def get_multiple_df(cls, event_type_list, date_utc8_start='2022-01-01', date_utc8_end='2022-01-01',
                        web_id=None, data_list=None, use_db=False,
                        dict_settings_all=None, columns=None):
        """
        To get multiple events at a time.

        Parameters
        ----------
        date_utc8_start   : str, only use in data_list=None, '2022-01-01' for use_db=False, '2022-01-01 02:00:00' for use_db=True
        date_utc8_end     : str, only use in data_list=None, '2022-01-01' for use_db=False, '2022-01-01 02:00:00' for use_db=True
        event_type_list   : list, available event_type: 'load', 'leave', 'timeout',
                                                        'addCart', 'removeCart', 'purchase',
                                                        'sendCoupon', 'acceptCoupon', 'discardCoupon',
                                                        'enterCoupon', 'acceptAf'
        web_id            : str, None for all web_id
        data_list         : list, None for get data_list by date_utc8_start and date_utc8_end
        use_db            : bool, fetch from db(True) or from local(False)
        dict_settings_all : dict, None, fetch from setting db
        columns           : list, select for columns in db when use_db=True

        Returns
        -------
        list of df ordered by event_type_list
        """

        df_list = []
        for event_type in event_type_list:
            if use_db:
                df = cls.get_df_from_db(date_utc8_start, date_utc8_end, web_id, event_type, columns)
            else:
                df = cls.get_df(date_utc8_start, date_utc8_end,
                                web_id=web_id,
                                data_list=data_list,
                                event_type=event_type,
                                dict_settings_all=dict_settings_all)
            df_list.append(df)
        return df_list

    @classmethod
    @logging_channels(['clare_test'])
    def get_df(cls, date_utc8_start='2022-01-01', date_utc8_end='2022-01-01',
                    web_id=None, data_list=None,
                    event_type='load', dict_settings_all=None):
        """

        Parameters
        ----------
        date_utc8_start   : str, only use in data_list=None
        date_utc8_end     : str, only use in data_list=None
        web_id
        data_list         : list of dict which to be appended
        event_type        : load, leave, addCart, removeCart, purchase,
                            sendCoupon, acceptCoupon, discardCoupon, enterCoupon, acceptAf
        dict_settings_all : settings of all web_id for addCar, removeCar and purchase events

        Returns
        -------
        DataFrame, df of all web_id
        """

        if data_list==None:
            data_list = cls.get_data_by_daterange(date_utc8_start, date_utc8_end)

        if web_id:
            data_list_filter = filterListofDictByDict(data_list,
                                                      dict_criteria={"web_id":web_id, "event_type": event_type})
        else:
            data_list_filter = filterListofDictByDict(data_list, dict_criteria={"event_type": event_type})

        dict_list = []
        if event_type=='load':
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_loaded(data_dict)
        elif event_type=='leave' or event_type=='timeout':
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_leaved_timeout(data_dict)
        ## addCart, removeCart, purchase
        # else:
        elif event_type=='addCart' or event_type=='removeCart' or event_type=='purchase':
            if dict_settings_all == None:
                dict_settings_all = cls.fetch_parse_key_all_settings()
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_object_all(data_dict, event_type, dict_settings_all)
        ## sendCoupon, acceptCoupon, discardCoupon
        elif event_type in ['sendCoupon', 'acceptCoupon', 'discardCoupon', 'enterCoupon']:
        # elif event_type=='sendCoupon' or event_type=='acceptCoupon' or event_type=='discardCoupon':
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_coupon(data_dict, event_type)
        elif event_type=='acceptAf':
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_afad(data_dict, event_type)
        else:
            print("not a valid event")
            return pd.DataFrame()  ## early return
        # self.data_list_clean = dict_list
        if dict_list == []:
            return pd.DataFrame() ## early return
        else:
            df = pd.DataFrame(dict_list)
        df['date_time'] = [datetime.datetime.utcfromtimestamp(ts/1000)+datetime.timedelta(hours=8) for ts in df['timestamp']]
        ## modify data case by case
        if event_type=='purchase': ## unique key in table
            df.drop(columns=cls._get_drop_col('purchase'), inplace=True)
            cls.reformat_remove_str2num(df, col='product_price', inplace=True, reformat_value=-1)
            cls.reformat_remove_str2num(df, col='total_price', inplace=True, reformat_value=-1)
            cls.reformat_remove_str2num(df, col='shipping_price', inplace=True, reformat_value=-1)
        elif event_type=='addCart' or event_type=='removeCart' or event_type=='leave' or event_type=='timeout' or event_type=='acceptCoupon' or event_type=='discardCoupon':
            # print([type(data) for data in df['max_time_no_scroll_array']])
            df['max_time_no_scroll_array'] = [','.join([str(i) for i in data]) if type(data)==list else str(data) for data in df['max_time_no_scroll_array']]
            df['max_time_no_scroll_depth_array'] = [','.join([str(i) for i in data]) if type(data)==list else str(data) for data in df['max_time_no_scroll_depth_array']]
        elif event_type=='sendCoupon':
            df.drop(columns=cls._get_drop_col('sendCoupon'), inplace=True)
            df['model_keys'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_keys']]
            df['model_parameters'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_parameters']]
            df['model_X'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_X']]
        elif event_type=='enterCoupon':
            df = df.query(f"coupon_code!=''")
        # df.drop_duplicates(subset=self._get_unique_col(event_type), inplace=True)
        df.dropna(inplace=True)
        df = cls.clean_before_sql(df, criteria_len={'web_id': 45, 'uuid': 36, 'ga_id': 45, 'fb_id': 45, 'timestamp': 16})
        return df


    @classmethod
    @timing
    def get_df_from_db(cls, date_utc8_start, date_utc8_end, web_id=None, event_type='load', columns=None):
        """

        Parameters
        ----------
        date_utc8_start: use for SQL query, '2022-05-19 00:00:00'
        date_utc8_end: use for SQL query, '2022-05-19 02:00:00'
        web_id: None for all web_id
        event_type: 9 events, 'load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase', 'sendCoupon', 'acceptCoupon', 'discardCoupon'
        columns: SQL columns to be chose

        Returns: DataFrame
        -------

        """
        if columns==None:
            columns = cls._get_df_event_col(event_type)
        table = cls._get_event_table(event_type)
        if web_id==None:
            query = f"""SELECT {','.join(columns)} FROM {table} WHERE date_time BETWEEN '{date_utc8_start}' and '{date_utc8_end}' 
                     """
        else:
            query = f"""SELECT {','.join(columns)} FROM {table} WHERE date_time BETWEEN '{date_utc8_start}' and '{date_utc8_end}' 
                        and web_id='{web_id}'"""
        print(query)
        data = DBhelper('tracker').ExecuteSelect(query)
        df = pd.DataFrame(data, columns=columns)
        return df

    @classmethod
    def get_df_click(cls, date_utc8_start='2022-01-01', date_utc8_end='2022-01-01',
                    web_id=None, data_list=None, event_type=None):
        if data_list==None:
            data_list = cls.get_data_by_daterange(date_utc8_start, date_utc8_end)
        data_list = filterListofDictByDict(data_list, dict_criteria={"click_info":None})
        if web_id:
            data_list = filterListofDictByDict(data_list, dict_criteria={"web_id":web_id})
        if event_type:
            data_list = filterListofDictByDict(data_list, dict_criteria={"event_type": event_type})
        res = []
        for data_dict in data_list:
            res.extend(cls.fully_parse_clickInfo(data_dict))
        df = pd.DataFrame(res)
        ### error ### with case -> df == []
        df['date_time'] = [datetime.datetime.utcfromtimestamp(ts / 1000) + datetime.timedelta(hours=8) for ts in
                           df['timestamp']]
        return df

    @staticmethod
    def fully_parse_clickInfo(data_dict:dict) -> list:
        type_dict = {"keyword_side_hot": 0, "keyword_side_footprint": 1, "keyword_search": 2, "keyword_word_hot": 3,
                     "keyword_word_other": 4, "sliding_click": 5, "guess_click": 7, "footprint_click": 8,
                     "otherlike_click": 9, "onpage": 10}
        # general keys
        keys_general = ["web_id", "uuid", "ga_id", "fb_id", "ip", "timestamp"]  # all sql default is string
        # in click_info
        keys_clickInfo = {"s_id": "_", "s_idl": "_", "dv": -1, "ul": "_", "un": "_", "type": 0,
                          "eventLabel": "_"}  # key:default
        keys_rename = ["session_id", "session_id_last", "device", "url_last", "url_now", "recommend_type", "label"]

        event_type = data_dict.get("event_type")  # default is None
        if not event_type or not data_dict or event_type not in type_dict:
            return []
        result_dict = {}
        # update key and value
        result_dict.update({k: data_dict.get(k, "_") for k in keys_general})
        clickInfo = data_dict.get("click_info", keys_clickInfo) # default
        # update key and value
        for (k, d), rk in zip(keys_clickInfo.items(), keys_rename):
            if k == "type":
                result_dict[rk] = type_dict.get(event_type, -1) + (clickInfo.get(k, d))
            else:
                result_dict[rk] = clickInfo.get(k, d)
        return [result_dict] # list



    @staticmethod
    def clean_before_sql(df, criteria_len={'web_id': 45, 'uuid': 36, 'ga_id': 45, 'fb_id': 45, 'timestamp': 16,
                                           'event_type': 16}):
        """

        Parameters
        ----------
        df: DataFrame to enter sql table
        criteria_len: convert to str and map(len) <= criteria

        Returns
        -------

        """
        cols = criteria_len.keys()
        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df = df[df[col].map(len) <= criteria_len[col]]
        return df

    ## addCart,removeCart,purchased events
    @classmethod
    def fully_parse_object_all(cls, data_dict, event_type, dict_settings_all):
        ## 1. parse common terms
        universial_dict = cls.parse_rename_universial(data_dict)
        ## 2. parse record_user terms
        record_dict = cls.parse_rename_record_user(data_dict, event_type)
        ## 3. parse cart, remove_cart or purchase terms
        object_dict_list = cls.parse_rename_object(data_dict, dict_settings_all, event_type)
        result_dict_list = []
        for object_dict in object_dict_list:
            if 'product_id' not in object_dict or str(object_dict['product_id']).startswith('hitobp:product'):
                # remove useless in hito
                continue
            object_dict['product_id'] = str(object_dict['product_id'])
            if object_dict['product_id'].startswith('pufii:product'):
                object_dict['product_id'] = object_dict['product_id'][14:]
                if object_dict['product_id'][0] == 'p': object_dict['product_id'] = object_dict['product_id'].replace('p', 'P', 1)
            elif object_dict['product_id'].startswith('wstyle:product'):
                object_dict['product_id'] = object_dict['product_id'][15:]
            object_dict.update(universial_dict)
            object_dict.update(record_dict)
            result_dict_list += [object_dict]
        return result_dict_list

    ## loaded event
    @classmethod
    def fully_parse_loaded(cls, data_dict):
        universial_dict = cls.parse_rename_universial(data_dict)
        key_list = ['dv', 'ul', 'un', 'm_t', 'i_l',
                    'ps', 't_p_t', 's_id', 's_idl', 'l_c',
                    's_h','w_ih','c_c_t', 'mt_nm', 'mt_ns',
                    'mt_nc', 'mps', 'mt_p', 'mt_p_t', 'ms_d',
                    'i_ac', 'i_rc', 'i_pb'] ## remove mt_nd(max_time_no_scroll_depth)
        key_rename_list = ['device', 'url_last', 'url_now', 'meta_title', 'is_landing',
                           'pageviews', 'time_pageview_total', 'session_id', 'session_id_last', 'landing_count',
                           'scroll_height', 'window_innerHeight', 'click_count_total', 'max_time_no_move_last', 'max_time_no_scroll_last',
                           'max_time_no_click_last', 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth',
                           'is_addCart', 'is_removeCart', 'is_purchased_before']

        object_dict = data_dict['load']
        loaded_dict = {}
        for key, key_rename in zip(key_list, key_rename_list):
            if key not in object_dict.keys():
                loaded_dict.update({key_rename: -1})
            else:
                loaded_dict.update({key_rename: object_dict[key]})
        universial_dict.update(loaded_dict)
        return [universial_dict]

    ## leaved and timeout event
    @classmethod
    def fully_parse_leaved_timeout(cls, data_dict):
        universial_dict = cls.parse_rename_universial(data_dict)
        record_dict = cls.parse_rename_record_user(data_dict)
        universial_dict.update(record_dict)
        return [universial_dict]

    ## coupon related events
    @classmethod
    def fully_parse_coupon(cls, data_dict, event_type):
        universial_dict = cls.parse_rename_universial(data_dict)
        coupon_info_dict = cls.parse_rename_coupon_info(data_dict, event_type)
        ## simple event to check enter coupon
        if event_type=='enterCoupon':
            universial_dict.update(coupon_info_dict)
        else:
            record_dict = cls.parse_rename_record_user(data_dict)
            universial_dict.update(coupon_info_dict)
            universial_dict.update(record_dict)
        return [universial_dict]

    ## addfan_ad related events
    @classmethod
    def fully_parse_afad(cls, data_dict, event_type):
        universial_dict = cls.parse_rename_universial(data_dict)
        afad_info_dict = cls.parse_rename_afad_info(data_dict, event_type)
        record_dict = cls.parse_rename_record_user(data_dict)
        universial_dict.update(afad_info_dict)
        universial_dict.update(record_dict)
        return [universial_dict]

    @staticmethod
    def parse_rename_universial(data_dict):
        key_list = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'coupon']
        key_rename_list = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon']
        universial_dict = {}
        for key,key_rename in zip(key_list,key_rename_list):
            if key in data_dict.keys():
                universial_dict.update({key_rename: data_dict[key]})
            else:
                universial_dict.update({key_rename: '_'})
        return universial_dict

    @staticmethod
    def parse_rename_record_user(data_dict, event_type = None):
        key_list = ['dv', 'ul', 'un', 'm_t', 's_h',
                    'w_ih', 't_p', 's_d', 's_d_', 'c_c',
                    'c_c_t', 't_nm', 't_ns', 't_nc', 'mt_nm',
                    'mt_ns', 'mt_nsa', 'mt_nda', 'mt_nd', 'mt_nd_',
                    'mt_nc', 'i_l', 's_idl', 's_id', 'ps',
                    't_p_t', 't_p_tl', 'mps', 'mt_p', 'mt_p_t',
                    'ms_d', 'ms_d_p', 'ms_d_pl', 'mt_nml', 'mt_nsl',
                    'mt_ncl', 'ms_dl', 'l_c',
                    'i_ac', 'i_rc', 'i_pb']
        key_rename_list = ['device', 'url_last', 'url_now', 'meta_title', 'scroll_height',
                           'window_innerHeight', 'time_pageview', 'scroll_depth', 'scroll_depth_px', 'click_count',
                           'click_count_total', 'time_no_move', 'time_no_scroll', 'time_no_click', 'max_time_no_move',
                           'max_time_no_scroll', 'max_time_no_scroll_array', 'max_time_no_scroll_depth_array',
                           'max_time_no_scroll_depth', 'max_time_no_scroll_depth_px',
                           'max_time_no_click', 'is_landing', 'session_id_last', 'session_id', 'pageviews',
                           'time_pageview_total', 'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                           'max_time_pageview_total',
                           'max_scroll_depth', 'max_scroll_depth_page', 'max_scroll_depth_page_last',
                           'max_time_no_move_last', 'max_time_no_scroll_last',
                           'max_time_no_click_last', 'max_scroll_depth_last', 'landing_count',
                           'is_addCart', 'is_removeCart', 'is_purchased_before']
        record_dict = {}
        if event_type in ['purchase']:
            key_list += ['l_u', 'l_p_u']
            key_rename_list += ['landing_url', 'landing_url_last']
        if 'record_user' not in data_dict.keys():
            record_dict = {key:-1 for key in key_rename_list}
            return record_dict
        record_user_dict = data_dict['record_user']
        if type(record_user_dict)==str:
            record_user_dict = json.loads(record_user_dict)
        for key, key_rename in zip(key_list, key_rename_list):
            if key in record_user_dict.keys():
                record_dict.update({key_rename: record_user_dict[key]})
            else:
                record_dict.update({key_rename: -1})
        return record_dict


    ## main for parse and rename 'addcart', 'removeCart', 'purchase' event
    @staticmethod
    def parse_rename_object(data_dict, dict_settings_all, event_type):
        dict_object_key = {'addCart': 'cart', 'removeCart': 'remove_cart', 'purchase': 'purchase'}
        object_key = dict_object_key[event_type]
        if 'web_id' not in data_dict.keys():
            print(f"web_id not in dict (not a valid format), return []")
            return []
        web_id = data_dict['web_id']
        if web_id not in dict_settings_all.keys():
            print(f"{web_id} not in dict_settings_all( do not analyze), return []")
            return []
        key_join_list, key_rename_list = dict_settings_all[web_id][event_type]
        if object_key not in data_dict.keys():
            print(f"{object_key} not in {data_dict}, return []")
            return []
        collection_dict, dict_object = {}, json.loads(data_dict[object_key])
        if len(key_join_list[0].split('.')) > 1:
            ## for dealing with adding 'purchase' key in purchase (91app, lovingfamily)
            if key_join_list[0].split('.')[0] not in dict_object.keys():
                dict_object = list(dict_object.values())[0]
        else:
            if key_join_list[0].split('.')[0] not in dict_object.keys():
                print(f"{key_join_list[0].split('.')[0]} not in {dict_object}, return []")
                return []
        ## skip dict_object is not dict
        if type(dict_object)!=dict:
            return []
        value_list = []
        n_list = 0
        # print(dict_object)
        ## parse dict type key and store list type key
        for key, key_rename in zip(key_join_list, key_rename_list):
            key_list = key.split('.')
            value = ''
            if len(key_list) == 1:  ##directly access dict
                for k in key_list:  ## append -1 if key not found
                    temp = dict_object[k] if k in dict_object.keys() else -1
                    if type(temp) == str or type(temp) == int:
                        collection_dict.update({key_rename: temp})
                    elif temp is None:
                        collection_dict.update({key_rename: -1})
                    elif type(temp) == list:
                        value_list = temp
                        n_list = len(temp)
                # for k in key_list: ## append -1 if key not found
                #     collection_dict.update({key_rename: dict_object[k]}) if k in dict_object.keys() else collection_dict.update({key_rename: -1})
            else:  ## parse multiple layer
                for key_2nd in key_list:
                    if value == '':  ## 1st level
                        value = '_' if key_2nd == 'empty' or key_2nd not in dict_object.keys() or not dict_object[key_2nd] else dict_object[key_2nd]
                    elif key_2nd=='json': ## use json.loads() => i3fresh case
                        value = json.loads(value)
                    elif type(value) == dict:  ## 2nd, 3rd... level
                        value = '_' if key_2nd == 'empty' or not value[key_2nd] else value[key_2nd]
                        collection_dict.update({key_rename: value})
                    elif type(value) == list:  ## 2nd, 3rd... level(parse list)
                        n_list = len(value)
                        for v in value: ## value: list [{k21:v21, k22:v22, k23:v23,...}]
                            if key_2nd in v.keys():
                                value = '_' if key == 'empty' or not v[key_2nd] else v[key_2nd]
                            else: ## not in k21,k22,k23...
                                value = '_'
                            value_list += [value]
                    else:
                        print(f'do nothing in {dict_object}')
        ## for parse multiple objects in a main_object
        if value_list == []:
            collection_purchase_dict_list = [collection_dict]
        else:
            # create multiple purchase record
            n_dict_key = len(collection_dict.keys())
            n_dict_list_key = int(len(value_list) / n_list)
            collection_purchase_dict_list = []
            if n_list != 0:
                for i in range(n_list):
                    temp_dict = {}
                    for j in range(n_dict_list_key):
                        temp_dict.update({key_rename_list[n_dict_key + j]: value_list[n_list * j + i]})
                    temp_dict.update(collection_dict)
                    collection_purchase_dict_list += [temp_dict]
            else:
                collection_purchase_dict_list = [collection_dict]
        return collection_purchase_dict_list


    @staticmethod
    def parse_rename_coupon_info(data_dict, event_type):
        if 'coupon_info' not in data_dict.keys():
            print(f"'coupon_info' not in {data_dict}, return []")
            return []
        dict_object = data_dict['coupon_info']
        if event_type=='sendCoupon':
            key_list = ['l_b', 'u_b', 'm_k', 'm_p', 'm_i',
                        'm_X', 'c_i', 'c_c_t', 'p_p']
            key_rename_list = ['lower_bound', 'upper_bound', 'model_keys', 'model_parameters', 'model_intercept',
                               'model_X', 'coupon_id', 'coupon_customer_type', 'prob_purchase']
        elif event_type=='enterCoupon':
            key_list = ['s_id', 'code']
            key_rename_list = ['session_id', 'coupon_code']
        else:
            key_list = ['p_p', 'c_t', 'c_d', 'c_c', 'c_st',
                        'c_ty', 'c_a', 'c_c_t', 'c_c_m', 'l_c',
                        'c_i']
            key_rename_list = ['prob_purchase', 'coupon_title', 'coupon_description', 'coupon_code', 'coupon_setTimer',
                               'coupon_type', 'coupon_amount', 'coupon_customer_type', 'coupon_code_mode', 'link_code',
                               'coupon_id']
        coupon_info_dict = {}
        for key,key_rename in zip(key_list,key_rename_list):
            if key in dict_object.keys():
                coupon_info_dict.update({key_rename: dict_object[key]})
            else:
                coupon_info_dict.update({key_rename: -1})
        return coupon_info_dict

    @staticmethod
    def parse_rename_afad_info(data_dict, event_type):
        if 'afad_info' not in data_dict.keys():
            print(f"'afad_info' not in {data_dict}, return []")
            return []
        dict_object = data_dict['afad_info']
        if event_type=='acceptAf':
            key_list = ['l_b', 'p_p', 'a_i', 'w_t']
            key_rename_list = ['lower_bound', 'prob_purchase', 'ad_id', 'website_type']
        afad_info_dict = {}
        for key,key_rename in zip(key_list,key_rename_list):
            if key in dict_object.keys():
                afad_info_dict.update({key_rename: dict_object[key]})
            else:
                afad_info_dict.update({key_rename: -1})
        return afad_info_dict

    @staticmethod
    @timing
    def fetch_parse_key_settings(web_id):
        if web_id==None:
            print("no settings to fetch")
            return {}
        query = f"""SELECT parsed_purchase_key, parsed_purchase_key_rename, parsed_addCart_key, parsed_addCart_key_rename,
                            parsed_removeCart_key, parsed_removeCart_key_rename
                            FROM cdp_tracking_settings where web_id='{web_id}'"""
        print(query)
        data = DBhelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
        settings = [x.split(',') for x in data[0]]
        dict_settings = {}
        for i,event_type in enumerate(['purchase', 'addCart', 'removeCart']):
            dict_settings.update({event_type: settings[i*2:(i+1)*2]})
        return dict_settings
    @staticmethod
    @timing
    def fetch_parse_key_all_settings():
        query = f"""SELECT web_id, parsed_purchase_key, parsed_purchase_key_rename, 
                            parsed_addCart_key, parsed_addCart_key_rename,
                            parsed_removeCart_key, parsed_removeCart_key_rename
                            FROM cdp_tracking_settings where enable_analysis=1"""
        print(query)
        data = DBhelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)
        dict_settings_all = {}
        for d in data:
            web_id = d[0]
            settings = [x.split(',') for x in d[1:]]
            dict_settings = {}
            for i, event_type in enumerate(['purchase', 'addCart', 'removeCart']):
                dict_settings.update({event_type: settings[i * 2:(i + 1) * 2]})
            dict_settings_all.update({web_id:dict_settings})
        return dict_settings_all

    ################################# get data using local storage #################################
    @staticmethod
    def get_file_byDatetime(datetime_utc0):
        """

        Parameters
        ----------
        datetime_utc0: with format, str:'2022-01-01 10:00:00' or datetime.datetime: 2022-01-01 10:00:00

        Returns: data_list
        -------

        """
        ## convert to datetime.datetime
        if type(datetime_utc0)==str:
            datetime_utc0 = datetime.datetime.strptime(datetime_utc0, "%Y-%m-%d %H:%M:%S")
        MID_DIR = datetime.datetime.strftime(datetime_utc0, format="%Y/%m/%d/%H")
        path = os.path.join(ROOT_DIR, "s3data", MID_DIR, "rawData.pickle")
        if os.path.isfile(path):
            with open(path, 'rb') as f:
                data_list = pickle.load(f)
        return data_list

    @classmethod
    def get_file_byHour(cls, date, hour, utc=0, pattern='%Y-%m-%d'):
        """

        Parameters
        ----------
        date: with format, str:'2022-01-01'(default pattern) or str:'2022/01/01'
        hour: with format, int:0-23
        pattern: parse date, str: '%Y-%m-%d' or '%Y/%m/%d'

        Returns: data_list
        -------

        """
        path = cls.get_date_hour_filepath(date, hour, utc, pattern)

        if os.path.isfile(path):
            with open(path, 'rb') as f:
                data_list = pickle.load(f)
        else:
            data_list = []
        return data_list

    @classmethod
    def get_data_by_daterange(cls, date_utc8_start='2022-01-01', date_utc8_end='2022-01-11'):
        if date_utc8_start==None or date_utc8_end==None:
            print("input date_utc8 range is None")
            return []
        num_days = (to_datetime(date_utc8_end) - to_datetime(date_utc8_start)).days+1
        date_utc8_list = [to_datetime(date_utc8_start) + datetime.timedelta(days=x) for x in range(num_days)]
        data_list = []
        for date_utc8 in date_utc8_list:
            data_list += cls.get_data_by_date(date_utc8)
        return data_list

    @classmethod
    def get_data_by_date(cls, date_utc8):
        file_list = cls.get_a_day_file_list(date_utc8)
        data_list = []
        for file in file_list:
            if os.path.isfile(file):
                with open(file, 'rb') as f:
                    data_list += pickle.load(f)
        return data_list

    @staticmethod
    def reformat_remove_str2num(df, col='shipping_price', inplace=False, reformat_value=0):
        # all integers and floats
        regex = "^-?\d+$|[\d\.\d]+"
        if col in df.columns:
            if inplace:
                # df[col] = [0 if re.findall("[0-9]", str(x)) == [] else int(float(str(x).replace(',',''))) for x in df[col]]
                df[col] = [reformat_value if re.findall(regex, str(x)) == [] else int(float(''.join(re.findall(regex, str(x))))) for x in df[col]]

            else:
                df_copy = df.copy()
                # df_copy[col] = [0 if re.findall("[0-9]", str(x)) == [] else int(float(str(x).replace(',',''))) for x in df[col]]
                df_copy[col] = [reformat_value if re.findall(regex, str(x)) == [] else int(float(''.join(re.findall(regex, str(x))))) for x in df[col]]

                return df_copy
        else:
            return df


    @staticmethod
    def get_a_day_file_list(date_utc8):
        if type(date_utc8) == datetime.datetime:
            datetime_utc0 = date_utc8 + datetime.timedelta(hours=-8)
        else:
            datetime_utc0 = datetime.datetime.strptime(date_utc8, "%Y-%m-%d") + datetime.timedelta(hours=-8)
        datetime_list = [datetime_utc0 + datetime.timedelta(hours=x) for x in range(24)]
        file_list = [
            os.path.join(ROOT_DIR, "s3data", datetime_to_str(root_folder, pattern="%Y/%m/%d/%H"), "rawData.pickle") for
            root_folder in datetime_list]
        return file_list

    @staticmethod
    def get_date_hour_filepath(date, hour, utc=8, pattern="%Y-%m-%d"):
        if type(date) == datetime.datetime:
            datetime_utc0 = date + datetime.timedelta(hours=-utc+hour)
        else:
            datetime_utc0 = datetime.datetime.strptime(date, pattern) + datetime.timedelta(hours=-utc+hour)
        path = os.path.join(ROOT_DIR, "s3data", datetime_to_str(datetime_utc0, pattern="%Y/%m/%d/%H"), "rawData.pickle")
        return path

    @staticmethod
    def _get_object_key():
        dict_object_key = {"load":"load", "leave":"", "timeout":"",
                           "addCart":"cart", "removeCart":"remove_cart", "purchase":"purchase",
                           "sendCoupon":"coupon_info", "acceptCoupon":"coupon_info", "discardCoupon":"coupon_info"}
        return dict_object_key

    @staticmethod
    def _get_event_table(event_type):
        if event_type=='load':
            table = 'clean_event_load'
        elif event_type == 'leave':
            table = 'clean_event_leave'
        elif event_type=='timeout':
            table = 'clean_event_timeout'
        elif event_type=='addCart':
            table = 'clean_event_addCart'
        elif event_type=='removeCart':
            table = 'clean_event_removeCart'
        elif event_type=='purchase':
            table = 'clean_event_purchase'
        elif event_type=='sendCoupon':
            table = 'clean_event_sendCoupon'
        elif event_type=='acceptCoupon':
            table = 'clean_event_acceptCoupon'
        elif event_type=='discardCoupon':
            table = 'clean_event_discardCoupon'
        else:
            table = ''
        return table

    @staticmethod
    def _get_df_event_col(event_type):
        if event_type=='load':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'device', 'url_last', 'url_now', 'meta_title', 'is_landing',
                       'pageviews', 'time_pageview_total', 'session_id', 'session_id_last',
                       'landing_count', 'scroll_height', 'window_innerHeight',
                       'click_count_total', 'max_time_no_move_last', 'max_time_no_scroll_last',
                       'max_time_no_click_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'date_time']
        elif event_type=='leave':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'device', 'url_last', 'url_now', 'meta_title', 'scroll_height',
                       'window_innerHeight', 'time_pageview', 'scroll_depth',
                       'scroll_depth_px', 'click_count', 'click_count_total', 'time_no_move',
                       'time_no_scroll', 'time_no_click', 'max_time_no_move',
                       'max_time_no_scroll', 'max_time_no_scroll_array',
                       'max_time_no_scroll_depth_array', 'max_time_no_scroll_depth',
                       'max_time_no_scroll_depth_px', 'max_time_no_click', 'is_landing',
                       'session_id_last', 'session_id', 'pageviews', 'time_pageview_total',
                       'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'max_scroll_depth_page',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='timeout':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'device', 'url_last', 'url_now', 'meta_title', 'scroll_height',
                       'window_innerHeight', 'time_pageview', 'scroll_depth',
                       'scroll_depth_px', 'click_count', 'click_count_total', 'time_no_move',
                       'time_no_scroll', 'time_no_click', 'max_time_no_move',
                       'max_time_no_scroll', 'max_time_no_scroll_array',
                       'max_time_no_scroll_depth_array', 'max_time_no_scroll_depth',
                       'max_time_no_scroll_depth_px', 'max_time_no_click', 'is_landing',
                       'session_id_last', 'session_id', 'pageviews', 'time_pageview_total',
                       'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'max_scroll_depth_page',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='addCart':
            columns = ['product_id', 'product_name', 'product_price', 'product_quantity',
                       'product_category', 'product_category_name', 'product_variant',
                       'sku_id', 'sku_name', 'currency',
                       'web_id', 'uuid', 'ga_id', 'fb_id',
                       'timestamp', 'avivid_coupon', 'device', 'url_last',
                       'url_now', 'meta_title', 'scroll_height', 'window_innerHeight',
                       'time_pageview', 'scroll_depth', 'scroll_depth_px', 'click_count',
                       'click_count_total', 'time_no_move', 'time_no_scroll', 'time_no_click',
                       'max_time_no_move', 'max_time_no_scroll', 'max_time_no_scroll_array',
                       'max_time_no_scroll_depth_array', 'max_time_no_scroll_depth',
                       'max_time_no_scroll_depth_px', 'max_time_no_click', 'is_landing',
                       'session_id_last', 'session_id', 'pageviews', 'time_pageview_total',
                       'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'max_scroll_depth_page',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='removeCart':
            columns = ['product_id', 'product_name', 'product_price', 'product_quantity',
                       'product_category', 'product_category_name', 'product_variant',
                       'sku_id', 'sku_name', 'currency',
                       'web_id', 'uuid', 'ga_id', 'fb_id',
                       'timestamp', 'avivid_coupon', 'device', 'url_last', 'url_now',
                       'meta_title', 'scroll_height', 'window_innerHeight', 'time_pageview',
                       'scroll_depth', 'scroll_depth_px', 'click_count', 'click_count_total',
                       'time_no_move', 'time_no_scroll', 'time_no_click', 'max_time_no_move',
                       'max_time_no_scroll', 'max_time_no_scroll_array',
                       'max_time_no_scroll_depth_array', 'max_time_no_scroll_depth',
                       'max_time_no_scroll_depth_px', 'max_time_no_click', 'is_landing',
                       'session_id_last', 'session_id', 'pageviews', 'time_pageview_total',
                       'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'max_scroll_depth_page',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='purchase':
            columns = ['product_id', 'product_name', 'product_price', 'product_quantity',
                       'product_category', 'product_category_name', 'product_variant',
                       'coupon', 'currency', 'order_id', 'total_price', 'shipping_price',
                       'web_id', 'uuid', 'ga_id', 'fb_id',
                       'timestamp', 'avivid_coupon', 'device', 'url_last', 'url_now',
                       'scroll_height', 'window_innerHeight', 'click_count_total',
                       'is_landing', 'session_id_last', 'session_id', 'pageviews',
                       'time_pageview_total', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='sendCoupon' or event_type=='acceptCoupon' or event_type=='discardCoupon':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'prob_purchase', 'coupon_title', 'coupon_description', 'coupon_code', 'coupon_setTimer',
                       'coupon_type', 'coupon_amount', 'coupon_customer_type', 'coupon_code_mode', 'link_code',
                       'device', 'url_last', 'url_now', 'meta_title', 'is_landing',
                       'pageviews', 'time_pageview_total', 'session_id', 'session_id_last',
                       'landing_count', 'scroll_height', 'window_innerHeight',
                       'click_count_total', 'max_time_no_move_last', 'max_time_no_scroll_last',
                       'max_time_no_click_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'date_time']
        elif event_type == 'enterCoupon':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'coupon_code', 'session_id', 'date_time']
        else:
            columns = []
        return columns

    @staticmethod
    def _get_drop_col(event_type):
        if event_type=='purchase':
            drop_col_list = ['meta_title','time_pageview','scroll_depth','scroll_depth_px','click_count',
                            'time_no_move','time_no_scroll','time_no_click','max_time_no_move','max_time_no_scroll',
                            'max_time_no_scroll_array','max_time_no_scroll_depth_array','max_time_no_scroll_depth','max_time_no_scroll_depth_px',
                            'max_time_no_click','max_scroll_depth_page','time_pageview_total_last']
        elif event_type=='sendCoupon':
            drop_col_list = ['time_pageview','scroll_depth','scroll_depth_px','click_count', 'time_no_move',
                             'time_no_scroll','time_no_click','max_time_no_move','max_time_no_scroll', 'max_time_no_scroll_array',
                             'max_time_no_scroll_depth_array','max_time_no_scroll_depth','max_time_no_scroll_depth_px', 'max_time_no_click',
                             'max_scroll_depth_page','time_pageview_total_last']
        return drop_col_list

    ## for df.drop_duplicates(subset), can be more than unique key in sql table
    @staticmethod
    def _get_unique_col(event_type):
        if event_type=='load':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        elif event_type=='purchase':
            subset = ['date_time', 'web_id', 'uuid', 'product_id',
                      'product_variant', 'product_category', 'product_price', 'product_name']
            # subset = ['date_time', 'web_id', 'uuid', 'product_id', 'product_name']
        elif event_type == 'addCart':
            # subset = ['date_time', 'web_id', 'uuid', 'product_id', 'product_price',
            #           'product_quantity', 'product_category']
            subset = ['date_time', 'web_id', 'uuid', 'product_id']
        elif event_type=='removeCart':
            # subset = ['date_time', 'web_id', 'uuid', 'product_id', 'sku_id']
            subset = ['date_time', 'web_id', 'uuid', 'product_id']
        elif event_type=='leave':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        elif event_type == 'timeout':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        else:
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        return subset


if __name__ == "__main__":
    # web_id = "wstyle" # chingtse, kava, draimior, magiplanet, i3fresh, wstyle, blueseeds, menustudy
    # # # lovingfamily, millerpopcorn, blueseeds, hidesan, washcan, hito, fmshoes, lzl, ego, up2you
    # # # fuigo, deliverfresh
    # date_utc8_start = "2022-10-13"
    # date_utc8_end = "2022-10-13"
    # tracking = TrackingParser(web_id, date_utc8_start, date_utc8_end)
    # data_list = tracking.data_list
    # # # order,amount,ship,order_coupon.json.total,bitem.json.itemid,bitem.json.empty,bitem.json.price,bitem.json.count,bitem.json.empty,bitem.json.empty,bitem.json.empty,bitem.json.empty
    # # # # # event_type = "acceptCoupon"
    # data_list_filter = filterListofDictByDict(data_list,
    #                                           dict_criteria={"click_info":None}) # sendCoupon, acceptCoupon, discardCoupon
    # x = filterListofDictByDict(data_list_filter, dict_criteria={"ip": "_", "uuid":"80aab48c-f089-4288-9117-b1eacd7db523"})
    # data_list_filter = filterListofDictByDict(data_list, dict_criteria={"event_type": "onpage"})  # sendCoupon, acceptCoupon, discardCoupon
    #

    TP = TrackingParser(web_id='inparents')
    TP.dict_settings['purchase']
    df = TrackingParser.get_df_click(date_utc8_start='2022-11-16', date_utc8_end='2022-11-16',
                                    web_id='inparents', data_list=None, event_type='purchase')
    data_list = filterListofDictByDict(data_list, dict_criteria={"event_type": event_type})
    data_list = TrackingParser.get_data_by_date('2022-11-16')
    data_list = filterListofDictByDict(data_list, dict_criteria={"web_id": 'inparents'})
    data_list = filterListofDictByDict(data_list, dict_criteria={"event_type": 'purchase'})

    res = []
    data_dict = data_list[0]
    for data_dict in data_list:
        print(data_dict.get('event_type'))
        res.extend(TrackingParser.fully_parse_clickInfo(data_dict))

    a = TrackingParser.get_df_click(data_list=data_list)
    TrackingParser.get_df(web_id=web_id, event_type=event_type)

    query = DBhelper.generate_insertDup_SQLquery(df, 'clean_event_clickInfo', df.columns)



    DBhelper('tracker').ExecuteUpdate(query, df.to_dict('records'))
    # res = []
    # for data_dict in data_list_filter:
    #     type_dict = {"keyword_side_hot": 0, "keyword_side_footprint": 1, "keyword_search": 2, "keyword_word_hot": 3,
    #                  "keyword_word_other": 4, "sliding_click": 5, "guess_click": 7, "footprint_click": 8,
    #                  "otherlike_click": 9, "onpage": 10
    #                  }
    #     event_type = data_dict.get("event_type")  # default is None
    #     if not event_type or not data_dict or event_type not in type_dict:
    #         continue
    #         # return []
    #     result_dict = {}
    #     # general keys
    #     keys_general = ["web_id", "uuid", "ga_id", "fb_id", "ip", "timestamp"]  # all sql default is string
    #     # update key and value
    #     result_dict.update({k: data_dict.get(k, "_") for k in keys_general})
    #     # in click_info
    #     keys_clickInfo = {"s_id": "_", "s_idl": "_", "dv": -1, "ul": "_", "un": "_", "type": 0,
    #                       "eventLabel": "_"}  # key:default
    #     keys_rename = ["session_id", "session_id_last", "device", "url_last", "url_now", "recommend_type", "label"]
    #     clickInfo = data_dict.get("click_info",
    #                               {"s_id": "_", "s_idl": "_", "dv": -1, "ul": "_", "un": "_", "type": 0, "eventLabel": "_"})
    #     # update key and value
    #     for (k, d), rk in zip(keys_clickInfo.items(), keys_rename):
    #         if k == "type":
    #             result_dict[rk] = type_dict.get(event_type, -1) + (clickInfo.get(k, d))
    #         else:
    #             result_dict[rk] = clickInfo.get(k, d)
    #     res.append(result_dict)
    # df = pd.DataFrame(res)
    # data_list_filter = filterListofDictByDict(data_list, dict_criteria={"event_type": "acceptCoupon"})
    # df2 = TrackingParser.get_df(date_utc8_start, date_utc8_end, 'bamboo', data_list_filter, 'acceptAf')
    # query = DBhelper.generate_insertDup_SQLquery(df2, 'clean_event_acceptAf', ['ad_id'])
    # DBhelper('tracker').ExecuteUpdate(query, df2.to_dict('records'))
    # df_sendCoupon = tracking.get_df(web_id, data_list_filter, 'acceptCoupon')
    # df2 = TrackingParser.get_df(date_utc8_start, date_utc8_end, 'wstyle', data_list, 'purchase')
    # df3 = TrackingParser.get_df_from_db('2022-05-19 00:00:00', '2022-05-19 02:00:00',
    #                                     web_id=None, event_type='purchase')
    #
    # data_dict = data_list_filter[0]
    # dict_settings_all = TrackingParser.fetch_parse_key_all_settings()
    # a = TrackingParser.fully_parse_object_all(data_dict, 'purchase', tracking.dict_settings)
    #
    # ## 1. parse common terms
    # universial_dict = TrackingParser.parse_rename_universial(data_dict)
    # ## 2. parse record_user terms
    # record_dict = TrackingParser.parse_rename_record_user(data_dict)
    # ## 3. parse cart, remove_cart or purchase terms
    # object_dict_list = TrackingParser.parse_rename_object(data_list_filter[1], dict_settings_all, 'purchase')
    # result_dict_list = []
    # for object_dict in object_dict_list:
    #     object_dict.update(universial_dict)
    #     object_dict.update(record_dict)
    #     result_dict_list += [object_dict]
    # df_list = TrackingParser.get_multiple_df(['acceptAf'], "2022-06-06", "2022-06-06")
    # df_all = TrackingParser().get_df_all(filterListofDictByDict(data_list, dict_criteria={"web_id": web_id, "event_type":'purchase'}),
    #                                      'purchase')
    #
    #
    def get_df(cls, date_utc8_start='2022-01-01', date_utc8_end='2022-01-01',
                    web_id=None, data_list=None,
                    event_type='load', dict_settings_all=None):
        """

        Parameters
        ----------
        date_utc8_start   : str, only use in data_list=None
        date_utc8_end     : str, only use in data_list=None
        web_id
        data_list         : list of dict which to be appended
        event_type        : load, leave, addCart, removeCart, purchase,
                            sendCoupon, acceptCoupon, discardCoupon, enterCoupon, acceptAf
        dict_settings_all : settings of all web_id for addCar, removeCar and purchase events

        Returns
        -------
        DataFrame, df of all web_id
        """
        date_utc8_start, date_utc8_end = '2022-11-16', '2022-11-16'
        if data_list==None:
            data_list = TrackingParser.get_data_by_daterange(date_utc8_start, date_utc8_end)

        if web_id:
            data_list_filter = filterListofDictByDict(data_list,
                                                      dict_criteria={"web_id":web_id, "event_type": event_type})
        else:
            data_list_filter = filterListofDictByDict(data_list, dict_criteria={"event_type": event_type})

        dict_list = []
        if event_type=='load':
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_loaded(data_dict)
        elif event_type=='leave' or event_type=='timeout':
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_leaved_timeout(data_dict)
        ## addCart, removeCart, purchase
        # else:
        elif event_type=='addCart' or event_type=='removeCart' or event_type=='purchase':
            if dict_settings_all == None:
                dict_settings_all = TrackingParser.fetch_parse_key_all_settings()
            for data_dict in data_list_filter:
                dict_list += TrackingParser.fully_parse_object_all(data_dict, event_type, dict_settings_all)
        ## sendCoupon, acceptCoupon, discardCoupon
        elif event_type in ['sendCoupon', 'acceptCoupon', 'discardCoupon', 'enterCoupon']:
        # elif event_type=='sendCoupon' or event_type=='acceptCoupon' or event_type=='discardCoupon':
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_coupon(data_dict, event_type)
        elif event_type=='acceptAf':
            for data_dict in data_list_filter:
                dict_list += cls.fully_parse_afad(data_dict, event_type)
        else:
            print("not a valid event")
            return pd.DataFrame()  ## early return
        # self.data_list_clean = dict_list
        if dict_list == []:
            return pd.DataFrame() ## early return
        else:
            df = pd.DataFrame(dict_list)
        df['date_time'] = [datetime.datetime.utcfromtimestamp(ts/1000)+datetime.timedelta(hours=8) for ts in df['timestamp']]
        ## modify data case by case
        if event_type=='purchase': ## unique key in table
            df.drop(columns=cls._get_drop_col('purchase'), inplace=True)
            cls.reformat_remove_str2num(df, col='product_price', inplace=True, reformat_value=-1)
            cls.reformat_remove_str2num(df, col='total_price', inplace=True, reformat_value=-1)
            cls.reformat_remove_str2num(df, col='shipping_price', inplace=True, reformat_value=-1)
        elif event_type=='addCart' or event_type=='removeCart' or event_type=='leave' or event_type=='timeout' or event_type=='acceptCoupon' or event_type=='discardCoupon':
            # print([type(data) for data in df['max_time_no_scroll_array']])
            df['max_time_no_scroll_array'] = [','.join([str(i) for i in data]) if type(data)==list else str(data) for data in df['max_time_no_scroll_array']]
            df['max_time_no_scroll_depth_array'] = [','.join([str(i) for i in data]) if type(data)==list else str(data) for data in df['max_time_no_scroll_depth_array']]
        elif event_type=='sendCoupon':
            df.drop(columns=cls._get_drop_col('sendCoupon'), inplace=True)
            df['model_keys'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_keys']]
            df['model_parameters'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_parameters']]
            df['model_X'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_X']]
        elif event_type=='enterCoupon':
            df = df.query(f"coupon_code!=''")
        # df.drop_duplicates(subset=self._get_unique_col(event_type), inplace=True)
        df.dropna(inplace=True)
        df = cls.clean_before_sql(df, criteria_len={'web_id': 45, 'uuid': 36, 'ga_id': 45, 'fb_id': 45, 'timestamp': 16})
        return df

        data_dict = data_list_filter[0]
        universial_dict = TrackingParser.parse_rename_universial(data_dict)
        ## 2. parse record_user terms
        record_dict = TrackingParser.parse_rename_record_user(data_dict)
        ## 3. parse cart, remove_cart or purchase terms
        d = dict_settings_all['inparents']['purchase']
        for a, b in zip(d[0], d[1]):
            print(f'{b:>22s} : {a}')
        object_dict_list = TrackingParser.parse_rename_object(data_dict, dict_settings_all, event_type)
        result_dict_list = []
        for object_dict in object_dict_list:
            if 'product_id' not in object_dict or str(object_dict['product_id']).startswith('hitobp:product'):
                # remove useless in hito
                continue
            object_dict['product_id'] = str(object_dict['product_id'])
            if object_dict['product_id'].startswith('pufii:product'):
                object_dict['product_id'] = object_dict['product_id'][14:]
                if object_dict['product_id'][0] == 'p': object_dict['product_id'] = object_dict['product_id'].replace(
                    'p', 'P', 1)
            elif object_dict['product_id'].startswith('wstyle:product'):
                object_dict['product_id'] = object_dict['product_id'][15:]
            object_dict.update(universial_dict)
            object_dict.update(record_dict)
            result_dict_list += [object_dict]
        return result_dict_list


        dict_object_key = {'addCart': 'cart', 'removeCart': 'remove_cart', 'purchase': 'purchase'}

        object_key = dict_object_key[event_type]
        if 'web_id' not in data_dict.keys():
            print(f"web_id not in dict (not a valid format), return []")
            return []
        web_id = data_dict['web_id']
        if web_id not in dict_settings_all.keys():
            print(f"{web_id} not in dict_settings_all( do not analyze), return []")
            return []
        key_join_list, key_rename_list = dict_settings_all[web_id][event_type]
        if object_key not in data_dict.keys():
            print(f"{object_key} not in {data_dict}, return []")
            return []
        collection_dict, dict_object = {}, json.loads(data_dict[object_key])
        if len(key_join_list[0].split('.')) > 1:
            ## for dealing with adding 'purchase' key in purchase (91app, lovingfamily)
            if key_join_list[0].split('.')[0] not in dict_object.keys():
                dict_object = list(dict_object.values())[0]
        else:
            if key_join_list[0].split('.')[0] not in dict_object.keys():
                print(f"{key_join_list[0].split('.')[0]} not in {dict_object}, return []")
                return []
        ## skip dict_object is not dict
        if type(dict_object) != dict:
            return []
        value_list = []
        n_list = 0
        # print(dict_object)
        ## parse dict type key and store list type key
        a = list(zip(key_join_list, key_rename_list))
        key, key_rename = a[5]
        for key, key_rename in zip(key_join_list, key_rename_list):
            key_list = key.split('.')
            value = ''
            if len(key_list) == 1:  ##directly access dict
                for k in key_list:  ## append -1 if key not found
                    temp = dict_object[k] if k in dict_object.keys() else -1
                    if type(temp) == str or type(temp) == int:
                        collection_dict.update({key_rename: temp})
                    elif temp is None:
                        collection_dict.update({key_rename: -1})
                    elif type(temp) == list:
                        value_list = temp
                        n_list = len(temp)
                # for k in key_list: ## append -1 if key not found
                #     collection_dict.update({key_rename: dict_object[k]}) if k in dict_object.keys() else collection_dict.update({key_rename: -1})
            else:  ## parse multiple layer
                for key_2nd in key_list:
                    if value == '':  ## 1st level
                        value = '_' if key_2nd == 'empty' or key_2nd not in dict_object.keys() or not dict_object[
                            key_2nd] else dict_object[key_2nd]
                    elif key_2nd == 'json':  ## use json.loads() => i3fresh case
                        value = json.loads(value)
                    elif type(value) == dict:  ## 2nd, 3rd... level
                        value = '_' if key_2nd == 'empty' or not value[key_2nd] else value[key_2nd]
                        collection_dict.update({key_rename: value})
                    elif type(value) == list:  ## 2nd, 3rd... level(parse list)
                        n_list = len(value)
                        for v in value:  ## value: list [{k21:v21, k22:v22, k23:v23,...}]
                            if key_2nd in v.keys():
                                value = '_' if key == 'empty' or not v[key_2nd] else v[key_2nd]
                            else:  ## not in k21,k22,k23...
                                value = '_'
                            value_list += [value]
                    else:
                        print(f'do nothing in {dict_object}')
        ## for parse multiple objects in a main_object
        if value_list == []:
            collection_purchase_dict_list = [collection_dict]
        else:
            # create multiple purchase record
            n_dict_key = len(collection_dict.keys())
            n_dict_list_key = int(len(value_list) / n_list)
            collection_purchase_dict_list = []
            if n_list != 0:
                for i in range(n_list):
                    temp_dict = {}
                    for j in range(n_dict_list_key):
                        temp_dict.update({key_rename_list[n_dict_key + j]: value_list[n_list * j + i]})
                    temp_dict.update(collection_dict)
                    collection_purchase_dict_list += [temp_dict]
            else:
                collection_purchase_dict_list = [collection_dict]