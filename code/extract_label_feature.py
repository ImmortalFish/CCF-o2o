# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 22:44:54 2017

@author: Administrator
"""

'''
提取标签特征
'''

import pandas as pd
import numpy as np
import datetime
pd.options.mode.chained_assignment = None

def ExtraceLabelFeature(dataset):
    '''提取标签特征'''
    label_user = dataset[['User_id']].drop_duplicates()
    label_merchant = dataset[['Merchant_id']].drop_duplicates()
    label_coupon = dataset[['Coupon_id']].drop_duplicates()
    label_user_merchant = dataset[['User_id', 'Merchant_id']].drop_duplicates()
    label_user_coupon = dataset[['User_id', 'Coupon_id']].drop_duplicates()
    label_user_discount = dataset[['User_id', 'Discount']].drop_duplicates()
    label_discount = dataset[['Discount']].drop_duplicates()
    temp = dataset.copy()
    
    ###############################################################################
    #用户的领券数量
    label_user['label_user_get_count'] = dataset.groupby('User_id', sort = False)['Date_received'].count().values
    
    #用户领取了多少种类的券
    temp_label = temp[['User_id']].drop_duplicates()
    temp_label_1 = temp[['User_id', 'Coupon_id']].drop_duplicates()
    temp_label_1['temp'] = temp.groupby(['User_id', 'Coupon_id'], sort = False)['Coupon_id'].count().values
    temp_label['label_user_get_kind_count'] = temp_label_1.groupby('User_id', sort = False)['temp'].count().values
    label_user = label_user.merge(temp_label, how = 'left', on = 'User_id')
    
    #用户领取了多少种折扣率的券
    temp_label = temp[['User_id']].drop_duplicates()
    temp_label_1 = temp[['User_id', 'Discount']].drop_duplicates()
    temp_label_1['temp'] = temp.groupby(['User_id', 'Discount'], sort = False)['Discount'].count().values
    temp_label['label_user_get_kind_discount_count'] = temp_label_1.groupby('User_id', sort = False)['temp'].count().values
    label_user = label_user.merge(temp_label, how = 'left', on = 'User_id')
    
    #用户领取了多少个不同商家的券
    temp_label = temp[['User_id']].drop_duplicates()
    temp_label_1 = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_label_1['temp'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Merchant_id'].count().values
    temp_label['label_user_get_kind_merchant_id_count'] = temp_label_1.groupby('User_id', sort = False)['temp'].count().values
    label_user = label_user.merge(temp_label, how = 'left', on = 'User_id')
    
    #用户领取的券的商家的平均距离
    temp['Distance'].replace(-1, np.nan, inplace = True)
    temp_label = temp[['User_id']].drop_duplicates()
    temp_label['label_user_get_distance_mean'] = temp.groupby('User_id', sort = False)['Distance'].mean().values
    label_user = label_user.merge(temp_label, how = 'left', on = 'User_id')
    label_user['label_user_get_distance_mean'].replace(np.nan, -1, inplace = True)
    temp['Distance'].replace(np.nan, -1, inplace = True)
    
    #用户领取的券的平均折扣率
    temp['Discount_rate'] = temp['Discount_rate'].astype('float')
    temp_label = temp[['User_id']].drop_duplicates()
    temp_label['label_user_get_discount_rate_mean'] = temp.groupby('User_id', sort = False)['Discount_rate'].mean().values
    label_user = label_user.merge(temp_label, how = 'left', on = 'User_id')
    
    #用户第一次领券与最后一次领券的天数差
    temp = dataset.copy()
    temp_label = temp[['User_id']].drop_duplicates()
    temp_label['Date_last_get'] = temp.groupby('User_id', sort = False)['Date_received_datetime'].max().values
    temp_label['Date_first_get'] = temp.groupby('User_id', sort = False)['Date_received_datetime'].min().values
    temp_label['label_user_first_get_last_get_day'] = [int(x.days) for x in pd.to_datetime(temp_label['Date_last_get']) - pd.to_datetime(temp_label['Date_first_get'])]
    temp_label.drop(['Date_last_get', 'Date_first_get'], axis = 1, inplace = True)
    label_user = label_user.merge(temp_label, how = 'left', on = 'User_id')
    
    #用户在这个月的前10天的领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = temp['Date_received'].min() + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['User_id']].drop_duplicates()
    label_temp['label_user_get_on_shangxun_count'] = temp.groupby('User_id', sort = False)['Date_received'].count().values
    label_user = label_user.merge(label_temp, how = 'left', on = 'User_id')
    label_user['label_user_get_on_shangxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户在这个月的中间10天的领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    min_date = max_date
    max_date = min_date + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] >= min_date]
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['User_id']].drop_duplicates()
    label_temp['label_user_get_on_zhongxun_count'] = temp.groupby('User_id', sort = False)['Date_received'].count().values
    label_user = label_user.merge(label_temp, how = 'left', on = 'User_id')
    label_user['label_user_get_on_zhongxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户在这个月的后10天的领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = max_date
    temp = temp[temp['Date_received'] >= max_date]
    label_temp = temp[['User_id']].drop_duplicates()
    label_temp['label_user_get_on_xiaxun_count'] = temp.groupby('User_id', sort = False)['Date_received'].count().values
    label_user = label_user.merge(label_temp, how = 'left', on = 'User_id')
    label_user['label_user_get_on_xiaxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户在上旬的领券次数与总的领券次数的占比
    label_user['label_user_get_on_shangxun_get_count'] = [x for x in label_user['label_user_get_on_shangxun_count'] / label_user['label_user_get_count']]
    
    #用户在中旬的领券次数与总的领券次数的占比
    label_user['label_user_get_on_zhongxun_get_count'] = [x for x in label_user['label_user_get_on_zhongxun_count'] / label_user['label_user_get_count']]
    
    #用户在下旬的领券次数与总的领券次数的占比
    label_user['label_user_get_on_xiaxun_get_count'] = [x for x in label_user['label_user_get_on_xiaxun_count'] / label_user['label_user_get_count']]
    
    #用户的领券日期是周末的次数
    temp = dataset[dataset['Date_received_is_weekend'] == 1]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['label_user_get_is_weekend_count'] = temp.groupby('User_id', sort = False)['Date_received_is_weekend'].count().values
    label_user = label_user.merge(temp_user, how = 'left', on = 'User_id')
    label_user['label_user_get_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #用户的领券日期是周末的次数 / 用户的领券数量
    label_user['label_user_get_is_weekend_get_per'] = [x for x in label_user['label_user_get_is_weekend_count'] / label_user['label_user_get_count']]
   
    return_dataset = dataset.merge(label_user, on = 'User_id', how = 'left')
    
    #用户是否是最后一次领券
    temp = dataset.copy()
    temp_label = temp[['User_id']].drop_duplicates()
    temp_label['Date_last_get'] = temp.groupby('User_id', sort = False)['Date_received_datetime'].max().values
    temp = temp.merge(temp_label, how = 'left', on = 'User_id')
    return_dataset['Label_user_is_last_get'] = [1 if x['Date_received_datetime'] == x['Date_last_get'] else 0 for i, x in temp.iterrows()]
    
    #用户是否是第一次领券
    temp = dataset.copy()
    temp_label = temp[['User_id']].drop_duplicates()
    temp_label['Date_first_get'] = temp.groupby('User_id', sort = False)['Date_received_datetime'].min().values
    temp = temp.merge(temp_label, how = 'left', on = 'User_id')
    return_dataset['Label_user_is_first_get'] = [1 if x['Date_received_datetime'] == x['Date_first_get'] else 0 for i, x in temp.iterrows()]
    ###############################################################################
    temp = dataset.copy()
    
    #商户被领取了多少券
    temp_label = temp[['Merchant_id']].drop_duplicates()
    temp_label['label_merchant_get_count'] = temp.groupby('Merchant_id', sort = False)['Date_received'].count().values
    label_merchant = label_merchant.merge(temp_label, how = 'left', on = 'Merchant_id')
    
    #商户在这个月的前10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = temp['Date_received'].min() + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['Merchant_id']].drop_duplicates()
    label_temp['label_merchant_get_on_shangxun_count'] = temp.groupby('Merchant_id', sort = False)['Date_received'].count().values
    label_merchant = label_merchant.merge(label_temp, how = 'left', on = 'Merchant_id')
    label_merchant['label_merchant_get_on_shangxun_count'].replace(np.nan, 0, inplace = True)
    
    #商户在这个月的中间10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    min_date = max_date
    max_date = min_date + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] >= min_date]
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['Merchant_id']].drop_duplicates()
    label_temp['label_merchant_get_on_zhongxun_count'] = temp.groupby('Merchant_id', sort = False)['Date_received'].count().values
    label_merchant = label_merchant.merge(label_temp, how = 'left', on = 'Merchant_id')
    label_merchant['label_merchant_get_on_zhongxun_count'].replace(np.nan, 0, inplace = True)
    
    #商户在这个月的后10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = max_date
    temp = temp[temp['Date_received'] >= max_date]
    label_temp = temp[['Merchant_id']].drop_duplicates()
    label_temp['label_merchant_get_on_xiaxun_count'] = temp.groupby('Merchant_id', sort = False)['Date_received'].count().values
    label_merchant = label_merchant.merge(label_temp, how = 'left', on = 'Merchant_id')
    label_merchant['label_merchant_get_on_xiaxun_count'].replace(np.nan, 0, inplace = True)
    
    #商户在上旬的领券次数与总的领券次数的占比
    label_merchant['label_merchant_get_on_shangxun_get_count'] = [x for x in label_merchant['label_merchant_get_on_shangxun_count'] / label_merchant['label_merchant_get_count']]
    
    #商户在中旬的领券次数与总的领券次数的占比
    label_merchant['label_merchant_get_on_zhongxun_get_count'] = [x for x in label_merchant['label_merchant_get_on_zhongxun_count'] / label_merchant['label_merchant_get_count']]
    
    #商户在下旬的领券次数与总的领券次数的占比
    label_merchant['label_merchant_get_on_xiaxun_get_count'] = [x for x in label_merchant['label_merchant_get_on_xiaxun_count'] / label_merchant['label_merchant_get_count']]
    
    #商户被领取的券的平均折扣率
    temp = dataset.copy()
    temp['Discount_rate'] = temp['Discount_rate'].astype('float')
    temp_label = temp[['Merchant_id']].drop_duplicates()
    temp_label['label_merchant_coupon_discount_rate_mean'] = temp.groupby('Merchant_id', sort = False)['Discount_rate'].mean().values
    label_merchant = label_merchant.merge(temp_label, how = 'left', on = 'Merchant_id')
    
    #商家被领取的所有优惠券种类数目
    temp_label = temp[['Merchant_id']].drop_duplicates()
    temp_label_1 = temp[['Merchant_id', 'Coupon_id']].drop_duplicates()
    temp_label_1['temp'] = temp.groupby(['Merchant_id', 'Coupon_id'], sort = False)['Coupon_id'].count().values
    temp_label['label_merchant_coupon_kinds_count'] = temp_label_1.groupby('Merchant_id', sort = False)['temp'].count().values
    label_merchant = label_merchant.merge(temp_label, how = 'left', on = 'Merchant_id')
    
    #商家被领券的平均距离
    temp = dataset.copy()
    temp['Distance'].replace(-1, np.nan, inplace = True)
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant['label_merchant_coupon_distance_mean'] = temp.groupby('Merchant_id', sort = False)['Distance'].mean().values
    label_merchant = label_merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    
    #商户的领券日期是周末的次数
    temp = dataset[dataset['Date_received_is_weekend'] == 1]
    temp_user = temp[['Merchant_id']].drop_duplicates()
    temp_user['label_merchant_get_is_weekend_count'] = temp.groupby('Merchant_id', sort = False)['Date_received_is_weekend'].count().values
    label_merchant = label_merchant.merge(temp_user, how = 'left', on = 'Merchant_id')
    label_merchant['label_merchant_get_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #商户的领券日期是周末的次数 / 商户的领券数量
    label_merchant['label_merchant_get_is_weekend_get_per'] = [x for x in label_merchant['label_merchant_get_is_weekend_count'] / label_merchant['label_merchant_get_count']]
    
    return_dataset = return_dataset.merge(label_merchant, on = 'Merchant_id', how = 'left')
    
    #商家是否最后一次被领券
    temp = dataset.copy()
    temp_label = temp[['Merchant_id']].drop_duplicates()
    temp_label['Date_last_get'] = temp.groupby('Merchant_id', sort = False)['Date_received_datetime'].max().values
    temp = temp.merge(temp_label, how = 'left', on = 'Merchant_id')
    return_dataset['Label_merchant_is_last_get'] = [1 if x['Date_received_datetime'] == x['Date_last_get'] else 0 for i, x in temp.iterrows()]   
    
    #商家是否第一次被领券
    temp = dataset.copy()
    temp_label = temp[['Merchant_id']].drop_duplicates()
    temp_label['Date_first_get'] = temp.groupby('Merchant_id', sort = False)['Date_received_datetime'].min().values
    temp = temp.merge(temp_label, how = 'left', on = 'Merchant_id')
    return_dataset['Label_merchant_is_first_get'] = [1 if x['Date_received_datetime'] == x['Date_first_get'] else 0 for i, x in temp.iterrows()]   
    ###############################################################################
    temp = dataset.copy()
    
    #优惠券被领取的次数
    temp = dataset.copy()
    temp_coupon = temp[['Coupon_id']].drop_duplicates()
    temp_coupon['label_coupon_get_count'] = temp.groupby('Coupon_id', sort = False)['Date_received'].count().values
    label_coupon = label_coupon.merge(temp_coupon, how = 'left', on = 'Coupon_id')
    
    #优惠券被多少不用的用户领取
    temp_coupon = temp[['Coupon_id']].drop_duplicates()
    temp_coupon_1 = temp[['Coupon_id', 'User_id']].drop_duplicates()
    temp_coupon_1['temp'] = temp_coupon_1.groupby(['Coupon_id', 'User_id'], sort = False)['User_id'].count().values
    temp_coupon['label_coupon_get_diff_user_count'] = temp_coupon_1.groupby('Coupon_id', sort = False)['temp'].count().values
    label_coupon = label_coupon.merge(temp_coupon, how = 'left', on = 'Coupon_id')
    
    #优惠券被用户领取的平均距离
    temp = dataset.copy()
    temp['Distance'].replace(-1, np.nan, inplace = True)
    temp_coupon = temp[['Coupon_id']].drop_duplicates()
    temp_coupon['label_coupon_distance_mean'] = temp.groupby('Coupon_id', sort = False)['Distance'].mean().values
    label_coupon = label_coupon.merge(temp_coupon, how = 'left', on = 'Coupon_id')
    label_coupon['label_coupon_distance_mean'].replace(np.nan, -1, inplace = True)
    
    #优惠券在这个月的前10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = temp['Date_received'].min() + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['Coupon_id']].drop_duplicates()
    label_temp['label_coupon_get_on_shangxun_count'] = temp.groupby('Coupon_id', sort = False)['Date_received'].count().values
    label_coupon = label_coupon.merge(label_temp, how = 'left', on = 'Coupon_id')
    label_coupon['label_coupon_get_on_shangxun_count'].replace(np.nan, 0, inplace = True)
    
    #优惠券在这个月的中间10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    min_date = max_date
    max_date = min_date + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] >= min_date]
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['Coupon_id']].drop_duplicates()
    label_temp['label_coupon_get_on_zhongxun_count'] = temp.groupby('Coupon_id', sort = False)['Date_received'].count().values
    label_coupon = label_coupon.merge(label_temp, how = 'left', on = 'Coupon_id')
    label_coupon['label_coupon_get_on_zhongxun_count'].replace(np.nan, 0, inplace = True)
    
    #优惠券在这个月的后10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = max_date
    temp = temp[temp['Date_received'] >= max_date]
    label_temp = temp[['Coupon_id']].drop_duplicates()
    label_temp['label_coupon_get_on_xiaxun_count'] = temp.groupby('Coupon_id', sort = False)['Date_received'].count().values
    label_coupon = label_coupon.merge(label_temp, how = 'left', on = 'Coupon_id')
    label_coupon['label_coupon_get_on_xiaxun_count'].replace(np.nan, 0, inplace = True) 
    
    #优惠券在上旬的领券次数与总的领券次数的占比
    label_coupon['label_coupon_get_on_shangxun_get_count'] = [x for x in label_coupon['label_coupon_get_on_shangxun_count'] / label_coupon['label_coupon_get_count']]
    
    #优惠券在中旬的领券次数与总的领券次数的占比
    label_coupon['label_coupon_get_on_zhongxun_get_count'] = [x for x in label_coupon['label_coupon_get_on_zhongxun_count'] / label_coupon['label_coupon_get_count']]
    
    #优惠券在下旬的领券次数与总的领券次数的占比
    label_coupon['label_coupon_get_on_xiaxun_get_count'] = [x for x in label_coupon['label_coupon_get_on_xiaxun_count'] / label_coupon['label_coupon_get_count']]
    
    #优惠券的领券日期是周末的次数
    temp = dataset[dataset['Date_received_is_weekend'] == 1]
    temp_user = temp[['Coupon_id']].drop_duplicates()
    temp_user['label_coupon_get_is_weekend_count'] = temp.groupby('Coupon_id', sort = False)['Date_received_is_weekend'].count().values
    label_coupon = label_coupon.merge(temp_user, how = 'left', on = 'Coupon_id')
    label_coupon['label_coupon_get_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #优惠券的领券日期是周末的次数 / 优惠券的领券数量
    label_coupon['label_coupon_get_is_weekend_get_per'] = [x for x in label_coupon['label_coupon_get_is_weekend_count'] / label_coupon['label_coupon_get_count']]
    
    return_dataset = return_dataset.merge(label_coupon, how = 'left', on = 'Coupon_id')
    
    #优惠券是否最后一次被领
    temp = dataset.copy()
    temp_label = temp[['Coupon_id']].drop_duplicates()
    temp_label['Date_last_get'] = temp.groupby('Coupon_id', sort = False)['Date_received_datetime'].max().values
    temp = temp.merge(temp_label, how = 'left', on = 'Coupon_id')
    return_dataset['Label_coupon_is_last_get'] = [1 if x['Date_received_datetime'] == x['Date_last_get'] else 0 for i, x in temp.iterrows()]    
    
    #优惠券是否第一次被领
    temp = dataset.copy()
    temp_label = temp[['Coupon_id']].drop_duplicates()
    temp_label['Date_first_get'] = temp.groupby('Coupon_id', sort = False)['Date_received_datetime'].min().values
    temp = temp.merge(temp_label, how = 'left', on = 'Coupon_id')
    return_dataset['Label_coupon_is_first_get'] = [1 if x['Date_received_datetime'] == x['Date_first_get'] else 0 for i, x in temp.iterrows()]  
    ###############################################################################
    temp = dataset.copy()
    
    #该折扣率的券被领取了多少次
    temp = dataset.copy()
    label_discount['label_discount_get_count'] = temp.groupby('Discount', sort = False)['Date_received'].count().values
    
    #该折扣率的券被多少不同用户领取
    temp_discount = dataset[['Discount']].drop_duplicates()
    temp_discount_1 = dataset[['Discount', 'User_id']].drop_duplicates()
    temp_discount_1['temp'] = dataset.groupby(['Discount', 'User_id'], sort = False)['User_id'].count().values
    temp_discount['Discount_get_kind_user_count'] = temp_discount_1.groupby('Discount', sort = False)['User_id'].count().values
    label_discount = label_discount.merge(temp_discount, how = 'left', on = 'Discount')
    
    #该折扣率的券被多少不同商家发放
    temp_discount = dataset[['Discount']].drop_duplicates()
    temp_discount_1 = dataset[['Discount', 'Merchant_id']].drop_duplicates()
    temp_discount_1['temp'] = dataset.groupby(['Discount', 'Merchant_id'], sort = False)['Merchant_id'].count().values
    temp_discount['Discount_get_kind_merchant_count'] = temp_discount_1.groupby('Discount', sort = False)['Merchant_id'].count().values
    label_discount = label_discount.merge(temp_discount, how = 'left', on = 'Discount')
    
    #该折扣率的券在这个月的前10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = temp['Date_received'].min() + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['Discount']].drop_duplicates()
    label_temp['label_discount_get_on_shangxun_count'] = temp.groupby('Discount', sort = False)['Date_received'].count().values
    label_discount = label_discount.merge(label_temp, how = 'left', on = 'Discount')
    label_discount['label_discount_get_on_shangxun_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券在这个月的中间10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    min_date = max_date
    max_date = min_date + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] >= min_date]
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['Discount']].drop_duplicates()
    label_temp['label_discount_get_on_zhongxun_count'] = temp.groupby('Discount', sort = False)['Date_received'].count().values
    label_discount = label_discount.merge(label_temp, how = 'left', on = 'Discount')
    label_discount['label_discount_get_on_zhongxun_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券在这个月的后10天的被领券次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = max_date
    temp = temp[temp['Date_received'] >= max_date]
    label_temp = temp[['Discount']].drop_duplicates()
    label_temp['label_discount_get_on_xiaxun_count'] = temp.groupby('Discount', sort = False)['Date_received'].count().values
    label_discount = label_discount.merge(label_temp, how = 'left', on = 'Discount')
    label_discount['label_discount_get_on_xiaxun_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券在上旬的领券次数与总的领券次数的占比
    label_discount['label_discount_get_on_shangxun_get_count'] = [x for x in label_discount['label_discount_get_on_shangxun_count'] / label_discount['label_discount_get_count']]
    
    #该折扣率的券在中旬的领券次数与总的领券次数的占比
    label_discount['label_discount_get_on_zhongxun_get_count'] = [x for x in label_discount['label_discount_get_on_zhongxun_count'] / label_discount['label_discount_get_count']]
    
    #该折扣率的券在下旬的领券次数与总的领券次数的占比
    label_discount['label_discount_get_on_xiaxun_get_count'] = [x for x in label_discount['label_discount_get_on_xiaxun_count'] / label_discount['label_discount_get_count']]
    
    #优惠券的领券日期是周末的次数
    temp = dataset[dataset['Date_received_is_weekend'] == 1]
    temp_user = temp[['Discount']].drop_duplicates()
    temp_user['label_discount_get_is_weekend_count'] = temp.groupby('Discount', sort = False)['Date_received_is_weekend'].count().values
    label_discount = label_discount.merge(temp_user, how = 'left', on = 'Discount')
    label_discount['label_discount_get_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #优惠券的领券日期是周末的次数 / 优惠券的领券数量
    label_discount['label_discount_get_is_weekend_get_per'] = [x for x in label_discount['label_discount_get_is_weekend_count'] / label_discount['label_discount_get_count']]
    
    return_dataset = return_dataset.merge(label_discount, how = 'left', on = 'Discount')
    
    #该折扣率的券是否最后一次被领取
    temp = dataset.copy()
    temp_label = temp[['Discount']].drop_duplicates()
    temp_label['Date_last_get'] = temp.groupby('Discount', sort = False)['Date_received_datetime'].max().values
    temp = temp.merge(temp_label, how = 'left', on = 'Discount')
    return_dataset['Label_discount_is_last_get'] = [1 if x['Date_received_datetime'] == x['Date_last_get'] else 0 for i, x in temp.iterrows()] 
    
    #该折扣率的券是否第一次被领取
    temp = dataset.copy()
    temp_label = temp[['Discount']].drop_duplicates()
    temp_label['Date_first_get'] = temp.groupby('Discount', sort = False)['Date_received_datetime'].min().values
    temp = temp.merge(temp_label, how = 'left', on = 'Discount')
    return_dataset['Label_discount_is_first_get'] = [1 if x['Date_received_datetime'] == x['Date_first_get'] else 0 for i, x in temp.iterrows()] 
    ###############################################################################
    temp = dataset.copy()
    
    #用户-商家:用户领取商家优惠券的次数
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['label_user_merchant_get_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Coupon_id'].count().values
    label_user_merchant = label_user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
     
    #用户-商家：用户领取商家优惠券的平均折扣率
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['label_user_merchant_get_discount_rate_mean'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Discount_rate'].mean().values
    label_user_merchant = label_user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
     
    #用户-商家：用户在这个商家的领券次数占用户总的领券次数的比例
    temp_user_merchant_1 = dataset[['User_id']].drop_duplicates()
    temp_user_merchant_1['label_user_get_count'] = dataset.groupby('User_id', sort = False)['Date_received'].count().values
    label_user_merchant = label_user_merchant.merge(temp_user_merchant_1, how = 'left', on = 'User_id')
    label_user_merchant['label_user_merchant_get_this_merchant_get_per'] = [x for x in label_user_merchant['label_user_merchant_get_count'] / label_user_merchant['label_user_get_count']]
    label_user_merchant.drop('label_user_get_count', axis = 1, inplace = True)
    
    #用户-商家：用户在这个月的前10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = temp['Date_received'].min() + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['User_id', 'Merchant_id']].drop_duplicates()
    label_temp['label_user_merchant_get_on_shangxun_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date_received'].count().values
    label_user_merchant = label_user_merchant.merge(label_temp, how = 'left', on = ['User_id', 'Merchant_id'])
    label_user_merchant['label_user_merchant_get_on_shangxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-商家：用户在这个月的中间10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    min_date = max_date
    max_date = min_date + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] >= min_date]
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['User_id', 'Merchant_id']].drop_duplicates()
    label_temp['label_user_merchant_get_on_zhongxun_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date_received'].count().values
    label_user_merchant = label_user_merchant.merge(label_temp, how = 'left', on = ['User_id', 'Merchant_id'])
    label_user_merchant['label_user_merchant_get_on_zhongxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-商家：用户在这个月的后10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = max_date
    temp = temp[temp['Date_received'] >= max_date]
    label_temp = temp[['User_id', 'Merchant_id']].drop_duplicates()
    label_temp['label_user_merchant_get_on_xiaxun_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date_received'].count().values
    label_user_merchant = label_user_merchant.merge(label_temp, how = 'left', on = ['User_id', 'Merchant_id'])
    label_user_merchant['label_user_merchant_get_on_xiaxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-商家：在上旬的领券次数与总的领券次数的占比
    label_user_merchant['label_user_merchant_get_on_shangxun_get_count'] = [x for x in label_user_merchant['label_user_merchant_get_on_shangxun_count'] / label_user_merchant['label_user_merchant_get_count']]
    
    #用户-商家：在中旬的领券次数与总的领券次数的占比
    label_user_merchant['label_user_merchant_get_on_zhongxun_get_count'] = [x for x in label_user_merchant['label_user_merchant_get_on_zhongxun_count'] / label_user_merchant['label_user_merchant_get_count']]
    
    #用户-商家：在下旬的领券次数与总的领券次数的占比
    label_user_merchant['label_user_merchant_get_on_xiaxun_get_count'] = [x for x in label_user_merchant['label_user_merchant_get_on_xiaxun_count'] / label_user_merchant['label_user_merchant_get_count']]
    
    return_dataset = return_dataset.merge(label_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    
    #用户-商家：用户是否是最后一次领这家店的券
    temp = dataset.copy()
    temp_label = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_label['Date_last_get'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date_received_datetime'].max().values
    temp = temp.merge(temp_label, how = 'left', on = ['User_id', 'Merchant_id'])
    return_dataset['Label_user_merchant_is_last_get'] = [1 if x['Date_received_datetime'] == x['Date_last_get'] else 0 for i, x in temp.iterrows()] 
    
    #用户-商家：用户是否是第一次领这家店的券
    temp = dataset.copy()
    temp_label = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_label['Date_first_get'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date_received_datetime'].min().values
    temp = temp.merge(temp_label, how = 'left', on = ['User_id', 'Merchant_id'])
    return_dataset['Label_user_merchant_is_first_get'] = [1 if x['Date_received_datetime'] == x['Date_first_get'] else 0 for i, x in temp.iterrows()] 
    ##############################################################################
    temp = dataset.copy()
     
    #用户-优惠券：用户领取该优惠券的次数
    temp_user_coupon = temp[['User_id', 'Coupon_id']].drop_duplicates()
    temp_user_coupon['label_user_coupon_get_count'] = temp.groupby(['User_id', 'Coupon_id'], sort = False)['Date_received'].count().values
    label_user_coupon = label_user_coupon.merge(temp_user_coupon, how = 'left', on = ['User_id', 'Coupon_id'])
     
    #用户-优惠券：用户领取这个优惠券的次数占用户总的领取次数的比例
    temp_user_coupon_1 = dataset[['User_id']].drop_duplicates()
    temp_user_coupon_1['label_user_get_count'] = dataset.groupby('User_id', sort = False)['Date_received'].count().values
    label_user_coupon = label_user_coupon.merge(temp_user_coupon_1, how = 'left', on = 'User_id')
    label_user_coupon['label_user_coupon_get_this_coupon_get_per'] = [x for x in label_user_coupon['label_user_coupon_get_count'] / label_user_coupon['label_user_get_count']]
    label_user_coupon.drop('label_user_get_count', axis = 1, inplace = True)
    
    #用户-优惠券：用户在这个月的前10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = temp['Date_received'].min() + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['User_id', 'Coupon_id']].drop_duplicates()
    label_temp['label_user_coupon_get_on_shangxun_count'] = temp.groupby(['User_id', 'Coupon_id'], sort = False)['Date_received'].count().values
    label_user_coupon = label_user_coupon.merge(label_temp, how = 'left', on = ['User_id', 'Coupon_id'])
    label_user_coupon['label_user_coupon_get_on_shangxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-优惠券：用户在这个月的中间10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    min_date = max_date
    max_date = min_date + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] >= min_date]
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['User_id', 'Coupon_id']].drop_duplicates()
    label_temp['label_user_coupon_get_on_zhongxun_count'] = temp.groupby(['User_id', 'Coupon_id'], sort = False)['Date_received'].count().values
    label_user_coupon = label_user_coupon.merge(label_temp, how = 'left', on = ['User_id', 'Coupon_id'])
    label_user_coupon['label_user_coupon_get_on_zhongxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-优惠券：用户在这个月的后10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = max_date
    temp = temp[temp['Date_received'] >= max_date]
    label_temp = temp[['User_id', 'Coupon_id']].drop_duplicates()
    label_temp['label_user_coupon_get_on_xiaxun_count'] = temp.groupby(['User_id', 'Coupon_id'], sort = False)['Date_received'].count().values
    label_user_coupon = label_user_coupon.merge(label_temp, how = 'left', on = ['User_id', 'Coupon_id'])
    label_user_coupon['label_user_coupon_get_on_xiaxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-优惠券：在上旬的领券次数与总的领券次数的占比
    label_user_coupon['label_user_coupon_get_on_shangxun_get_count'] = [x for x in label_user_coupon['label_user_coupon_get_on_shangxun_count'] / label_user_coupon['label_user_coupon_get_count']]
    
    #用户-优惠券：在中旬的领券次数与总的领券次数的占比
    label_user_coupon['label_user_coupon_get_on_zhongxun_get_count'] = [x for x in label_user_coupon['label_user_coupon_get_on_zhongxun_count'] / label_user_coupon['label_user_coupon_get_count']]
    
    #用户-优惠券：在下旬的领券次数与总的领券次数的占比
    label_user_coupon['label_user_coupon_get_on_xiaxun_get_count'] = [x for x in label_user_coupon['label_user_coupon_get_on_xiaxun_count'] / label_user_coupon['label_user_coupon_get_count']]
    
    return_dataset = return_dataset.merge(label_user_coupon, how = 'left', on = ['User_id', 'Coupon_id'])
    
    #用户-优惠券：用户是否是最后一次领券
    temp = dataset.copy()
    temp_label = temp[['User_id', 'Coupon_id']].drop_duplicates()
    temp_label['Date_last_get'] = temp.groupby(['User_id', 'Coupon_id'], sort = False)['Date_received_datetime'].max().values
    temp = temp.merge(temp_label, how = 'left', on = ['User_id', 'Coupon_id'])
    return_dataset['Label_user_coupon_is_last_get'] = [1 if x['Date_received_datetime'] == x['Date_last_get'] else 0 for i, x in temp.iterrows()] 
    
    #用户-优惠券：用户是否是第一次领券
    temp = dataset.copy()
    temp_label = temp[['User_id', 'Coupon_id']].drop_duplicates()
    temp_label['Date_first_get'] = temp.groupby(['User_id', 'Coupon_id'], sort = False)['Date_received_datetime'].min().values
    temp = temp.merge(temp_label, how = 'left', on = ['User_id', 'Coupon_id'])
    return_dataset['Label_user_coupon_is_first_get'] = [1 if x['Date_received_datetime'] == x['Date_first_get'] else 0 for i, x in temp.iterrows()] 
    ##############################################################################
    temp = dataset.copy()
    
    #用户-折扣：用户领取该折扣率的券的次数
    temp_user_discount = temp[['User_id', 'Discount']].drop_duplicates()
    temp_user_discount['label_user_discount_get_count'] = temp.groupby(['User_id', 'Discount'], sort = False)['Date_received'].count().values
    label_user_discount = label_user_discount.merge(temp_user_discount, how = 'left', on = ['User_id', 'Discount'])
     
    #用户-折扣：用户领取这个折扣率的券的次数占用户总的领取次数的比例
    temp_user_discount_1 = dataset[['User_id']].drop_duplicates()
    temp_user_discount_1['label_user_get_count'] = dataset.groupby('User_id', sort = False)['Date_received'].count().values
    label_user_discount = label_user_discount.merge(temp_user_discount_1, how = 'left', on = 'User_id')
    label_user_discount['label_user_discount_get_this_coupon_get_per'] = [x for x in label_user_discount['label_user_discount_get_count'] / label_user_discount['label_user_get_count']]
    label_user_discount.drop('label_user_get_count', axis = 1, inplace = True)
    
    #用户-折扣：用户在这个月的前10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = temp['Date_received'].min() + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['User_id', 'Discount']].drop_duplicates()
    label_temp['label_user_discount_get_on_shangxun_count'] = temp.groupby(['User_id', 'Discount'], sort = False)['Date_received'].count().values
    label_user_discount = label_user_discount.merge(label_temp, how = 'left', on = ['User_id', 'Discount'])
    label_user_discount['label_user_discount_get_on_shangxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-折扣：用户在这个月的中间10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    min_date = max_date
    max_date = min_date + datetime.timedelta(days = 10)
    temp = temp[temp['Date_received'] >= min_date]
    temp = temp[temp['Date_received'] < max_date]
    label_temp = temp[['User_id', 'Discount']].drop_duplicates()
    label_temp['label_user_discount_get_on_zhongxun_count'] = temp.groupby(['User_id', 'Discount'], sort = False)['Date_received'].count().values
    label_user_discount = label_user_discount.merge(label_temp, how = 'left', on = ['User_id', 'Discount'])
    label_user_discount['label_user_discount_get_on_zhongxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-折扣：用户在这个月的后10天领取该券的次数
    temp = dataset.copy()
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Date_received'] = pd.to_datetime(temp['Date_received'])
    max_date = max_date
    temp = temp[temp['Date_received'] >= max_date]
    label_temp = temp[['User_id', 'Discount']].drop_duplicates()
    label_temp['label_user_discount_get_on_xiaxun_count'] = temp.groupby(['User_id', 'Discount'], sort = False)['Date_received'].count().values
    label_user_discount = label_user_discount.merge(label_temp, how = 'left', on = ['User_id', 'Discount'])
    label_user_discount['label_user_discount_get_on_xiaxun_count'].replace(np.nan, 0, inplace = True)
    
    #用户-折扣：在上旬的领券次数与总的领券次数的占比
    label_user_discount['label_user_discount_get_on_shangxun_get_count'] = [x for x in label_user_discount['label_user_discount_get_on_shangxun_count'] / label_user_discount['label_user_discount_get_count']]
    
    #用户-折扣：在中旬的领券次数与总的领券次数的占比
    label_user_discount['label_user_discount_get_on_zhongxun_get_count'] = [x for x in label_user_discount['label_user_discount_get_on_zhongxun_count'] / label_user_discount['label_user_discount_get_count']]
    
    #用户-折扣：在下旬的领券次数与总的领券次数的占比
    label_user_discount['label_user_discount_get_on_xiaxun_get_count'] = [x for x in label_user_discount['label_user_discount_get_on_xiaxun_count'] / label_user_discount['label_user_discount_get_count']]
    
    return_dataset = return_dataset.merge(label_user_discount, how = 'left', on = ['User_id', 'Discount'])
    
    #用户-折扣：用户是否是最后一次领券
    temp = dataset.copy()
    temp_label = temp[['User_id', 'Discount']].drop_duplicates()
    temp_label['Date_last_get'] = temp.groupby(['User_id', 'Discount'], sort = False)['Date_received_datetime'].max().values
    temp = temp.merge(temp_label, how = 'left', on = ['User_id', 'Discount'])
    return_dataset['Label_user_discount_is_last_get'] = [1 if x['Date_received_datetime'] == x['Date_last_get'] else 0 for i, x in temp.iterrows()]
    
    #用户-折扣：用户是否是第一次领券
    temp = dataset.copy()
    temp_label = temp[['User_id', 'Discount']].drop_duplicates()
    temp_label['Date_first_get'] = temp.groupby(['User_id', 'Discount'], sort = False)['Date_received_datetime'].min().values
    temp = temp.merge(temp_label, how = 'left', on = ['User_id', 'Discount'])
    return_dataset['Label_user_discount_is_first_get'] = [1 if x['Date_received_datetime'] == x['Date_first_get'] else 0 for i, x in temp.iterrows()]
    
    return_dataset.drop('Date_received_datetime', axis = 1, inplace = True)
    
    return return_dataset

