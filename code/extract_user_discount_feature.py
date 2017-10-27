# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 21:46:30 2017

@author: Administrator
"""

'''
提取用户折扣特征
'''

import numpy as np

def ExtraceUserDiscountFeature(dataset):
    '''提取用户折扣特征'''
    user_discount = dataset[['User_id', 'Discount']].drop_duplicates()
    
    #用户领取该折扣率的券的数量
    user_discount['user_discount_get_count'] = dataset.groupby(['User_id', 'Discount'], sort = False)['Date_received'].count().values
    
    #用户领取并消费该折扣率的券的数量
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_discount = temp[['User_id', 'Discount']].drop_duplicates()
    temp_user_discount['user_discount_get_cost_count'] = temp.groupby(['User_id', 'Discount'], sort = False)['Date'].count().values
    user_discount = user_discount.merge(temp_user_discount, how = 'left', on = ['User_id', 'Discount'])
    user_discount['user_discount_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户领取未消费该折扣率的券的数量
    user_discount['user_discount_get_not_cost_count'] = [x for x in user_discount['user_discount_get_count'] - user_discount['user_discount_get_cost_count']]
    
    #用户核销该折扣率的券的数量
    temp = dataset[dataset['Label'] == 1]
    temp_user_discount = temp[['User_id', 'Discount']].drop_duplicates()
    temp_user_discount['user_discount_hexiao_count'] = temp.groupby(['User_id', 'Discount'], sort = False)['Label'].count().values
    user_discount = user_discount.merge(temp_user_discount, how = 'left', on = ['User_id', 'Discount'])
    user_discount['user_discount_hexiao_count'].replace(np.nan, 0, inplace = True)
    
    #用户领取并消费该折扣率的券的数量 / 用户领取该折扣率的券的数量
    user_discount['user_discount_get_cost_get_per'] = [x for x in user_discount['user_discount_get_cost_count'] / user_discount['user_discount_get_count']]
    user_discount['user_discount_get_cost_get_per'].replace(np.nan, 0, inplace = True)
    
    #用户核销该折扣率的券的数量 / 用户领取该折扣率的券的数量
    user_discount['user_discount_hexiao_cost_get_per'] = [x for x in user_discount['user_discount_hexiao_count'] / user_discount['user_discount_get_count']]
    user_discount['user_discount_hexiao_cost_get_per'].replace(np.nan, 0, inplace = True)
    
    #用户核销该折扣率的券的数量 / 用户领取并消费该折扣率的券的数量
    user_discount['user_discount_hexiao_cost_per'] = [x for x in user_discount['user_discount_hexiao_count'] / user_discount['user_discount_get_cost_count']]
    user_discount['user_discount_hexiao_cost_per'].replace(np.nan, 0, inplace = True)
    
    #用户领取该折扣率的券的平均折扣率
    user_discount['user_discount_get_discount_rate_mean'] = dataset.groupby(['User_id', 'Discount'], sort = False)['Discount_rate'].mean().values
    
    #用户领取并消费该折扣率的券的平均折扣率
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_discount = temp[['User_id', 'Discount']].drop_duplicates()
    temp_user_discount['user_discount_get_cost_discount_rate_mean'] = temp.groupby(['User_id', 'Discount'], sort = False)['Discount_rate'].mean().values
    user_discount = user_discount.merge(temp_user_discount, how = 'left', on = ['User_id', 'Discount'])
    user_discount['user_discount_get_cost_discount_rate_mean'].replace(np.nan, -1, inplace = True)
    
    #用户从领取该折扣的券到消费该券的平均时间
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_discount = temp[['User_id', 'Discount']].drop_duplicates()
    temp_user_discount['user_discount_get_get_cost_day_mean'] = temp.groupby(['User_id', 'Discount'], sort = False)['Day_gap'].mean().values
    user_discount = user_discount.merge(temp_user_discount, how = 'left', on = ['User_id', 'Discount'])
    user_discount['user_discount_get_get_cost_day_mean'].replace(np.nan, -1, inplace = True)
    
    #用户与领取该折扣率的券的商家的平均距离
    dataset['Distance'].replace(-1, np.nan, inplace = True)
    user_discount['user_discount_get_distance'] = dataset.groupby(['User_id', 'Discount'], sort = False)['Distance'].mean().values
    user_discount['user_discount_get_distance'].replace(np.nan, -1, inplace = True)
    
    #用户与领取并消费该折扣率的券的商家的平均距离
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_discount = temp[['User_id', 'Discount']].drop_duplicates()
    temp_user_discount['user_discount_get_get_cost_distance_mean'] = temp.groupby(['User_id', 'Discount'], sort = False)['Distance'].mean().values
    user_discount = user_discount.merge(temp_user_discount, how = 'left', on = ['User_id', 'Discount'])
    user_discount['user_discount_get_get_cost_distance_mean'].replace(np.nan, -1, inplace = True)
    dataset['Distance'].replace(np.nan, -1, inplace = True)
    
    #用户在多少不同的商家处领取了该折扣率的券
    temp_user_discount =  dataset[['User_id', 'Discount']].drop_duplicates()
    temp_user_discount_1 = dataset[['User_id', 'Discount', 'Merchant_id']].drop_duplicates()
    temp_user_discount_1['temp'] = dataset.groupby(['User_id', 'Discount', 'Merchant_id'], sort = False)['Merchant_id'].count().values
    temp_user_discount['user_discount_get_kind_count'] = temp_user_discount_1.groupby(['User_id', 'Discount'], sort = False)['temp'].count().values
    user_discount = user_discount.merge(temp_user_discount, how = 'left', on = ['User_id', 'Discount'])
    user_discount['user_discount_get_kind_count'].replace(np.nan, 0, inplace = True)
    
    #用户在多少不同的商家处领取并消费了该折扣率的券
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user_discount =  temp[['User_id', 'Discount']].drop_duplicates()
    temp_user_discount_1 = temp[['User_id', 'Discount', 'Merchant_id']].drop_duplicates()
    temp_user_discount_1['temp'] = temp.groupby(['User_id', 'Discount', 'Merchant_id'], sort = False)['Merchant_id'].count().values
    temp_user_discount['user_discount_get_cost_kind_count'] = temp_user_discount_1.groupby(['User_id', 'Discount'], sort = False)['temp'].count().values
    user_discount = user_discount.merge(temp_user_discount, how = 'left', on = ['User_id', 'Discount'])
    user_discount['user_discount_get_cost_kind_count'].replace(np.nan, 0, inplace = True)
    
    return user_discount