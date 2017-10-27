# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 16:17:48 2017

@author: Administrator
"""

'''
提取用户特征
'''

import numpy as np

def ExtractUserFeature(dataset):
    '''提取用户特征'''
    user = dataset[['User_id']].drop_duplicates()
    
    #用户领券次数
    user['user_get_count'] = dataset.groupby('User_id', sort = False)['Date_received'].count().values
    
    #用户领券并消费的次数
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['user_get_cost_count'] = temp.groupby('User_id', sort = False)['Date'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户领券未消费的次数
    temp = dataset[(dataset['Date'] == 'null') & (dataset['Date_received'] != 'null')]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['user_get_not_cost_count'] = temp.groupby('User_id', sort = False)['Date'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_not_cost_count'].replace(np.nan, 0, inplace = True)
    
    #用户核销的次数
    temp = dataset[dataset['Label'] == 1]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['user_hexiao_count'] = temp.groupby('User_id', sort = False)['Label'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_hexiao_count'].replace(np.nan, 0, inplace = True)
    
    #用户领券并消费的次数 / 用户领券次数
    user['user_get_cost_get_per'] = [x for x in user['user_get_cost_count'] / user['user_get_count']]
    
    #用户核销的次数 / 用户领券次数
    user['user_hexiao_get_per'] = [x for x in user['user_hexiao_count'] / user['user_get_count']]
    
    #用户核销的次数 / 用户领券并消费的次数
    user['user_hexiao_get_cost_per'] = [x for x in user['user_hexiao_count'] / user['user_get_cost_count']]
    user['user_hexiao_get_cost_per'].replace(np.nan, 0, inplace = True)
    
    #用户领取券的平均折扣率
    dataset['Discount_rate'] = dataset['Discount_rate'].astype('float')
    user['user_get_discount_rate_mean'] = dataset.groupby('User_id', sort = False)['Discount_rate'].mean().values
    
    #用户领取并消费的券的平均折扣率
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp['Discount_rate'] = temp['Discount_rate'].astype('float')
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['user_get_cost_discount_rate_mean'] = temp.groupby('User_id', sort = False)['Discount_rate'].mean().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_cost_discount_rate_mean'].replace(np.nan, -1, inplace = True)
    
    #用户领取的券的平均距离
    dataset['Distance'].replace(-1, np.nan, inplace = True)
    user['user_get_distance_mean'] = dataset.groupby('User_id', sort = False)['Distance'].mean().values
    
    #用户领取并消费的券的平均距离
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['user_get_cost_distance_mean'] = temp.groupby('User_id', sort = False)['Distance'].mean().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_cost_distance_mean'].replace(np.nan, -1, inplace = True)
    
    #用户从领取到消费的平均时间
    temp = dataset[(dataset['Date'] != 'null') & (dataset['Date_received'] != 'null')]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['user_get_get_cost_day_mean'] = temp.groupby('User_id', sort = False)['Day_gap'].mean().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_get_cost_day_mean'].replace(np.nan, -1, inplace = True)
    
    #用户领取了多少种类的券
    temp_user = dataset[['User_id']].drop_duplicates()
    temp_user_1 = dataset[['User_id', 'Coupon_id']].drop_duplicates()
    temp_user_1['temp'] = dataset.groupby(['User_id', 'Coupon_id'], sort = False)['Coupon_id'].count().values
    temp_user['user_get_kind_count'] = temp_user_1.groupby('User_id', sort = False)['temp'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_kind_count'].replace(np.nan, 0, inplace = True)
    
    #用户领取并消费了多少种类的券
    temp = dataset[(dataset['Date_received'] != 'null') & (dataset['Date'] != 'null')]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user_1 = temp[['User_id', 'Coupon_id']].drop_duplicates()
    temp_user_1['temp'] = temp.groupby(['User_id', 'Coupon_id'], sort = False)['Coupon_id'].count().values
    temp_user['user_get_cost_kind_count'] = temp_user_1.groupby('User_id', sort = False)['temp'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_cost_kind_count'].replace(np.nan, 0, inplace = True)
    
    #用户领取了多少种不同折扣率的券
    temp_user = dataset[['User_id']].drop_duplicates()
    temp_user_1 = dataset[['User_id', 'Discount_rate']].drop_duplicates()
    temp_user_1['temp'] = dataset.groupby(['User_id', 'Discount_rate'], sort = False)['Discount_rate'].count().values
    temp_user['user_get_kind_discount_count'] = temp_user_1.groupby('User_id', sort = False)['temp'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_kind_discount_count'].replace(np.nan, 0, inplace = True)
    
    #用户领取并消费了多少种不同折扣率的券
    temp = dataset[(dataset['Date_received'] != 'null') & (dataset['Date'] != 'null')]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user_1 = temp[['User_id', 'Discount_rate']].drop_duplicates()
    temp_user_1['temp'] = temp.groupby(['User_id', 'Discount_rate'], sort = False)['Discount_rate'].count().values
    temp_user['user_get_cost_kind_discount_count'] = temp_user_1.groupby('User_id', sort = False)['temp'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_cost_kind_discount_count'].replace(np.nan, 0, inplace = True)
    
    #用户领取了多少个不同商家的券
    temp_user = dataset[['User_id']].drop_duplicates()
    temp_user_1 = dataset[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_1['temp'] = dataset.groupby(['User_id', 'Merchant_id'], sort = False)['Merchant_id'].count().values
    temp_user['user_get_kind_merchant_count'] = temp_user_1.groupby('User_id', sort = False)['temp'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_kind_merchant_count'].replace(np.nan, 0, inplace = True)
    
    #用户领取并消费了多少个不同商家的券
    temp = dataset[(dataset['Date_received'] != 'null') & (dataset['Date'] != 'null')]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user_1 = temp[['User_id', 'Merchant_id']].drop_duplicates()
    temp_user_1['temp'] = temp.groupby(['User_id', 'Merchant_id'], sort = False)['Merchant_id'].count().values
    temp_user['user_get_cost_kind_merchant_count'] = temp_user_1.groupby('User_id', sort = False)['temp'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_cost_kind_merchant_count'].replace(np.nan, 0, inplace = True)
    
    #用户领券的日期是周末的次数
    temp = dataset[dataset['Date_received_is_weekend'] == 1]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['user_get_is_weekend_count'] = temp.groupby('User_id', sort = False)['Date_received_is_weekend'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #用户领券并消费的日期是周末的次数
    temp = dataset[(dataset['Date_is_weekend'] == 1) & (dataset['Date_received'] != 'null')]
    temp_user = temp[['User_id']].drop_duplicates()
    temp_user['user_get_cost_is_weekend_count'] = temp.groupby('User_id', sort = False)['Date_is_weekend'].count().values
    user = user.merge(temp_user, how = 'left', on = 'User_id')
    user['user_get_cost_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #用户领券的日期是周末的次数 / 用户领券次数
    user['user_get_is_weekend_get_per'] = [x for x in user['user_get_is_weekend_count'] / user['user_get_count']]
    
    #用户领券并消费的日期是周末的次数 / 用户领券并消费的次数
    user['user_get_cost_is_weekend_cost_per'] = [x for x in user['user_get_cost_is_weekend_count'] / user['user_get_cost_count']]
    
    return user