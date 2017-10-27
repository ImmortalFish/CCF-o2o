# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 17:36:17 2017

@author: Administrator
"""

'''
提取商户特征
'''

import numpy as np

def ExtractMerchantFeature(dataset):
    '''提取商户特征'''
    merchant = dataset[['Merchant_id']].drop_duplicates()
    
    #商户被领取的券数量
    merchant['merchant_get_count'] = dataset.groupby('Merchant_id', sort = False)['Date_received'].count().values
    
    #商户被领取并消费的券的数量
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant['merchant_get_cost_count'] = temp.groupby('Merchant_id', sort = False)['Date'].count().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #商户的券被领取但未消费的数量
    temp = dataset[(dataset['Date'] == 'null') & (dataset['Date_received'] != 'null')]
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant['merchant_get_not_cost_count'] = temp.groupby('Merchant_id', sort = False)['Date'].count().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_not_cost_count'].replace(np.nan, 0, inplace = True)
    
    #商户被核销的券的数量
    temp = dataset[dataset['Label'] == 1]
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant['merchant_hexiao_count'] = temp.groupby('Merchant_id', sort = False)['Label'].count().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    merchant['merchant_hexiao_count'].replace(np.nan, 0, inplace = True)
    
    #商户被领取并消费的券的数量 / 商户被领取的券数量
    merchant['merchant_get_cost_get_per'] = [x for x in merchant['merchant_get_cost_count'] / merchant['merchant_get_count']]
    
    #商户被核销的券的数量 / 商户被领取的券数量
    merchant['merchant_hexiao_get_per'] = [x for x in merchant['merchant_hexiao_count'] / merchant['merchant_get_count']]
    merchant['merchant_hexiao_get_per'].replace(np.nan, 0, inplace = True)
    
    #商户被核销的券的数量 / 商户被领取并消费的券的数量
    merchant['merchant_hexiao_get_cost_per'] = [x for x in merchant['merchant_hexiao_count'] / merchant['merchant_get_cost_count']]
    merchant['merchant_hexiao_get_cost_per'].replace(np.nan, 0, inplace = True)
    
    #商户被领取的券的平均折扣率
    dataset['Discount_rate'] = dataset['Discount_rate'].astype('float')
    merchant['merchant_get_discount_rate_mean'] = dataset.groupby('Merchant_id', sort = False)['Discount_rate'].mean().values
    
    #商家的券从被领取到消费的平均时间
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant['merchant_get_get_cost_day_mean'] = temp.groupby('Merchant_id', sort = False)['Day_gap'].mean().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_get_cost_day_mean'].replace(np.nan, -1, inplace = True)
    
    #商家的券从领取到核销的平均时间
    temp = dataset[dataset['Label'] == 1]
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant['merchant_get_hexiao_day_mean'] = temp.groupby('Merchant_id', sort = False)['Day_gap'].mean().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_hexiao_day_mean'].replace(np.nan, -1, inplace = True)
    
    #商户被领取的券的平均距离
    temp = dataset.copy()
    temp['Distance'].replace(-1, np.nan, inplace = True)
    merchant['merchant_get_distance_mean'] = temp.groupby('Merchant_id', sort = False)['Distance'].mean().values
    merchant['merchant_get_distance_mean'].replace(np.nan, -1, inplace = True)
    
    #商户被领取并消费的券的平均距离
    temp = dataset[(dataset['Date_received'] != 'null') & (dataset['Date'] != 'null')]
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant['merchant_get_cost_distance_mean'] = temp.groupby('Merchant_id', sort = False)['Distance'].mean().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_cost_distance_mean'].replace(np.nan, -1, inplace = True)
    
    #商家的券被多少不同的用户领取
    temp_merchant = dataset[['Merchant_id']].drop_duplicates()
    temp_merchant_1 = dataset[['User_id', 'Merchant_id']].drop_duplicates()
    temp_merchant_1['temp'] = dataset.groupby(['User_id', 'Merchant_id'], sort = False)['User_id'].count().values
    temp_merchant['merchant_get_kind_user_count'] = temp_merchant_1.groupby('Merchant_id', sort = False)['temp'].count().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    
    #商家的券被多少不同的用户消费
    temp = dataset[(dataset['Date_received'] != 'null') & (dataset['Date'] != 'null')]
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant_1 = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_merchant_1['temp'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['User_id'].count().values
    temp_merchant['merchant_get_cost_kind_user_count'] = temp_merchant_1.groupby('Merchant_id', sort = False)['temp'].count().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_cost_kind_user_count'].replace(np.nan, -1, inplace = True)
    
    #商家被领取的满减券的平均最低消费
    temp = dataset[dataset['Discount_type'] == 0]
    temp_merchant = temp[['Merchant_id']].drop_duplicates()
    temp_merchant['merchant_get_man_min_cost_mean'] = temp.groupby('Merchant_id', sort = False)['Min_cost'].mean().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_man_min_cost_mean'].replace(np.nan, -1, inplace = True)
    
    #商家发放了多少种不同的券
    temp_merchant = dataset[['Merchant_id']].drop_duplicates()
    temp_merchant_1 = dataset[['Merchant_id', 'Coupon_id']].drop_duplicates()
    temp_merchant_1['temp'] = dataset.groupby(['Merchant_id', 'Coupon_id'], sort = False)['Coupon_id'].count().values
    temp_merchant['merchant_get_kind_coupon_count'] = temp_merchant_1.groupby('Merchant_id', sort = False)['temp'].count().values
    merchant = merchant.merge(temp_merchant, how = 'left', on = 'Merchant_id')
    
    #商户被领券的日期是周末的次数
    temp = dataset[dataset['Date_received_is_weekend'] == 1]
    temp_user = temp[['Merchant_id']].drop_duplicates()
    temp_user['merchant_get_is_weekend_count'] = temp.groupby('Merchant_id', sort = False)['Date_received_is_weekend'].count().values
    merchant = merchant.merge(temp_user, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #商户被领券并消费的日期是周末的次数
    temp = dataset[(dataset['Date_is_weekend'] == 1) & (dataset['Date_received'] != 'null')]
    temp_user = temp[['Merchant_id']].drop_duplicates()
    temp_user['merchant_get_cost_is_weekend_count'] = temp.groupby('Merchant_id', sort = False)['Date_is_weekend'].count().values
    merchant = merchant.merge(temp_user, how = 'left', on = 'Merchant_id')
    merchant['merchant_get_cost_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #商户被领券并消费的日期是周末的次数 / 商户被领券消费的次数
    merchant['merchant_get_cost_is_weekend_cost_per'] = [x for x in merchant['merchant_get_cost_is_weekend_count'] / merchant['merchant_get_cost_count']]
    
    #商户被领券的日期是周末的次数 / 商户被领取的券数量
    merchant['merchant_get_is_weekend_cost_per'] = [x for x in merchant['merchant_get_is_weekend_count'] / merchant['merchant_get_count']]
    
    return merchant