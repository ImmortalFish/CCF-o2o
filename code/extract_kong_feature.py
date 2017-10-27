# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 10:14:06 2017

@author: Administrator
"""

'''
提取空闲段的特征
'''

import numpy as np

def ExtractKongFeature(dataset):
    '''提取空闲段的特征'''
    kong_user = dataset[['User_id']].drop_duplicates()
    kong_merchant = dataset[['Merchant_id']].drop_duplicates()
    kong_user_merchant = dataset[['User_id', 'Merchant_id']].drop_duplicates()
    
    #用户的消费数量
    temp = dataset.copy()
    temp_kong = temp[temp['Date'] != 'null']
    temp_user = temp_kong[['User_id']].drop_duplicates()
    temp_user['kong_user_cost_count'] = temp_kong.groupby('User_id', sort = False)['Date'].count().values
    kong_user = kong_user.merge(temp_user, on = 'User_id', how = 'left')
    kong_user['kong_user_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户领券消费的数量
    temp = dataset.copy()
    temp_kong = temp[(temp['Date'] != 'null') & (temp['Date_received'] != 'null')]
    temp_user = temp_kong[['User_id']].drop_duplicates()
    temp_user['kong_user_get_cost_count'] = temp_kong.groupby('User_id', sort = False)['Date'].count().values
    kong_user = kong_user.merge(temp_user, on = 'User_id', how = 'left')
    kong_user['kong_user_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户未领券去消费的数量
    temp = dataset.copy()
    temp_kong = temp[(temp['Date'] != 'null') & (temp['Date_received'] == 'null')]
    temp_user = temp_kong[['User_id']].drop_duplicates()
    temp_user['kong_user_not_get_cost_count'] = temp_kong.groupby('User_id', sort = False)['Date'].count().values
    kong_user = kong_user.merge(temp_user, on = 'User_id', how = 'left')
    kong_user['kong_user_not_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户领券就消费的数量 / 用户的消费数量
    kong_user['kong_user_get_cost_cost_per'] = [x for x in kong_user['kong_user_get_cost_count'] / kong_user['kong_user_cost_count']]
    
    #用户未领券消费的数量 / 用户的消费数量
    kong_user['kong_user_not_get_cost_cost_per'] = [x for x in kong_user['kong_user_not_get_cost_count'] / kong_user['kong_user_cost_count']]
    
    #商户的被消费次数
    temp = dataset.copy()
    temp_kong = temp[temp['Date'] != 'null']
    temp_merchant = temp_kong[['Merchant_id']].drop_duplicates()
    temp_merchant['kong_merchant_cost_count'] = temp_kong.groupby('Merchant_id', sort = False)['Date'].count().values
    kong_merchant = kong_merchant.merge(temp_merchant, on = 'Merchant_id', how = 'left')
    kong_merchant['kong_merchant_cost_count'].replace(np.nan, 0, inplace = True)
    
    #商户被领券并消费的次数
    temp = dataset.copy()
    temp_kong = temp[(temp['Date'] != 'null') & (temp['Date_received'] != 'null')]
    temp_merchant = temp_kong[['Merchant_id']].drop_duplicates()
    temp_merchant['kong_merchant_get_cost_count'] = temp_kong.groupby('Merchant_id', sort = False)['Date'].count().values
    kong_merchant = kong_merchant.merge(temp_merchant, on = 'Merchant_id', how = 'left')
    kong_merchant['kong_merchant_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #商户被未领券被费的次数
    temp = dataset.copy()
    temp_kong = temp[(temp['Date'] != 'null') & (temp['Date_received'] == 'null')]
    temp_merchant = temp_kong[['Merchant_id']].drop_duplicates()
    temp_merchant['kong_merchant_not_get_cost_count'] = temp_kong.groupby('Merchant_id', sort = False)['Date'].count().values
    kong_merchant = kong_merchant.merge(temp_merchant, on = 'Merchant_id', how = 'left')
    kong_merchant['kong_merchant_not_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #商户被领券并消费的次数 / 商户的被消费次数
    kong_merchant['kong_merchant_get_cost_cost_per'] = [x for x in kong_merchant['kong_merchant_get_cost_count'] / kong_merchant['kong_merchant_cost_count']]
    
    #商户领券就消费的数量 / 用户的消费数量
    kong_merchant['kong_merchant_not_get_cost_cost_per'] = [x for x in kong_merchant['kong_merchant_not_get_cost_count'] / kong_merchant['kong_merchant_cost_count']]
    
    #用户-商户：用户领取该商家的券的数量
    temp = dataset[dataset['Date_received'] != 'null']
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['kong_user_merchant_get_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date_received'].count().values
    kong_user_merchant = kong_user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    kong_user_merchant['kong_user_merchant_get_count'].replace(np.nan, 0, inplace = True)
    
    #用户-商户：用户在该商家的消费次数
    kong_user_merchant['kong_user_merchant_cost_count'] = dataset.groupby(['User_id', 'Merchant_id'], sort = False)['Date'].count().values
    
    #用户-商户：用户领取该商家的券并消费的数量
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['kong_user_merchant_get_get_cost_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date'].count().values
    kong_user_merchant = kong_user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    kong_user_merchant['kong_user_merchant_get_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户-商户：用户未领取该商户的券还去消费了的次数
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] == 'null')]
    temp_user_merchant = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_merchant['kong_user_merchant_not_get_get_cost_count'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Date'].count().values
    kong_user_merchant = kong_user_merchant.merge(temp_user_merchant, how = 'left', on = ['User_id', 'Merchant_id'])
    kong_user_merchant['kong_user_merchant_not_get_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户-商户：用户在该商家的消费次数 / 用户领取该商家的券的数量
    kong_user_merchant['kong_user_merchant_get_cost_get_per'] = [x for x in kong_user_merchant['kong_user_merchant_cost_count'] / kong_user_merchant['kong_user_merchant_get_count']]
    kong_user_merchant['kong_user_merchant_get_cost_get_per'].replace(np.inf, 0, inplace = True)
    
    #用户-商户：用户领取该商家的券并消费的数量 / 用户在该商家的消费次数
    kong_user_merchant['kong_user_merchant_get_cost_cost_per'] = [x for x in kong_user_merchant['kong_user_merchant_get_get_cost_count'] / kong_user_merchant['kong_user_merchant_cost_count']]
    
    #用户-商户：用户未领取该商户的券还去消费了的次数 / 用户在该商家的消费次数
    kong_user_merchant['kong_user_merchant_not_get_cost_cost_per'] = [x for x in kong_user_merchant['kong_user_merchant_not_get_get_cost_count'] / kong_user_merchant['kong_user_merchant_cost_count']]
    
    return kong_user, kong_merchant, kong_user_merchant
    