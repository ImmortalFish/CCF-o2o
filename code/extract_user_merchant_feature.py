# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 21:23:38 2017

@author: Administrator
"""

'''
提取用户-商户特征
'''

import numpy as np

def ExtraceUserMerchantFeature(dataset):
    '''提取用户-商户特征'''
    user_merchant = dataset[['User_id', 'Merchant_id']].drop_duplicates()
    
    #用户领取商家的优惠券次数
    user_merchant['user_merchant_get_count'] = dataset.groupby(['User_id', 'Merchant_id'], sort = False)['Coupon_id'].count().values
    
    #用户在商家领了券并消费的次数
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['user_merchant_get_get_cost_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date'].count().values
    user_merchant = user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    user_merchant['user_merchant_get_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户在商家领了券却未消费的次数
    temp = dataset[(dataset['Date'] == 'null') & (dataset['Date_received'] != 'null')]
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['user_merchant_get_not_cost_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date_received'].count().values
    user_merchant = user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    user_merchant['user_merchant_get_not_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户在商家处核销的次数
    temp = dataset[dataset['Label'] == 1]
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['user_merchant_hexiao_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Label'].count().values
    user_merchant = user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    user_merchant['user_merchant_hexiao_count'].replace(np.nan, 0, inplace = True)
    
    #用户在商家领了券并消费的次数 / 用户领取商家的优惠券次数
    user_merchant['user_merchant_get_cost_get_per'] = [x for x in user_merchant['user_merchant_get_get_cost_count'] / user_merchant['user_merchant_get_count']]
    user_merchant['user_merchant_get_cost_get_per'].replace(np.nan, 0, inplace = True)
    
    #用户在商家处核销的次数 / 用户领取商家的优惠券次数
    user_merchant['user_merchant_hexiao_get_per'] = [x for x in user_merchant['user_merchant_hexiao_count'] / user_merchant['user_merchant_get_count']] 
    user_merchant['user_merchant_hexiao_get_per'].replace(np.nan, 0, inplace = True)
    
    #用户在商家处核销的次数 / 用户在商家领了券并消费的次数
    user_merchant['user_merchant_hexiao_get_cost_per'] = [x for x in user_merchant['user_merchant_hexiao_count'] / user_merchant['user_merchant_get_get_cost_count']]
    user_merchant['user_merchant_hexiao_get_cost_per'].replace(np.nan, 0, inplace = True)
    
    #用户在商家处领取的券的平均折扣率
    user_merchant['user_merchant_get_discount_rate_mean'] = dataset.groupby(['User_id', 'Merchant_id'], sort = False)['Discount_rate'].mean().values
    
    #用户在商家处领取并消费的券的平均折扣率
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['user_merchant_get_cost_discount_rate_mean'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Discount_rate'].mean().values
    user_merchant = user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    user_merchant['user_merchant_get_cost_discount_rate_mean'].replace(np.nan, -1, inplace = True)
    
    #用户在商家处领取了多少种不同的券
    temp_user_merchant =  dataset[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant_1 = dataset[['User_id', 'Merchant_id', 'Coupon_id']].drop_duplicates()
    temp_user_merchant_1['temp'] = dataset.groupby(['User_id', 'Merchant_id', 'Coupon_id'], sort = False)['Coupon_id'].count().values
    temp_user_merchant['user_merchant_get_kind_count'] = temp_user_merchant_1.groupby(['User_id', 'Merchant_id'], sort = False)['temp'].count().values
    user_merchant = user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    user_merchant['user_merchant_get_kind_count'].replace(np.nan, 0, inplace = True)
    
    #用户在商家处领取并消费了多少种不同的券
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_merchant =  temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant_1 = temp[['User_id', 'Merchant_id', 'Coupon_id']].drop_duplicates()
    temp_user_merchant_1['temp'] = temp.groupby(['User_id', 'Merchant_id', 'Coupon_id'], sort = False)['Coupon_id'].count().values
    temp_user_merchant['user_merchant_get_cost_kind_count'] = temp_user_merchant_1.groupby(['User_id', 'Merchant_id'], sort = False)['temp'].count().values
    user_merchant = user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    user_merchant['user_merchant_get_cost_kind_count'].replace(np.nan, 0, inplace = True)
    
    #用户在商家处从领取到消费的平均时间
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['user_merchant_get_get_cost_day_mean'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Day_gap'].mean().values
    user_merchant = user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    user_merchant['user_merchant_get_get_cost_day_mean'].replace(np.nan, -1, inplace = True)
    
    return user_merchant