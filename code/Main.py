# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 21:26:42 2017

@author: Administrator
"""

'''
运行所有程序
'''

import os
import pandas as pd
import xgboost as xgb
import set_label as sl
import split_by_time as sbt
import extract_user_feature as euf
import extract_merchant_feature as emf
import extract_discount_feature as edf
import extract_user_merchant_feature as eumf
import extract_user_discount_feature as eudf
import extract_label_feature as elf
import extract_kong_feature as ekf
import gen_dataset as gd
import data_handle_pre as dhp
from sklearn.ensemble import GradientBoostingClassifier
pd.options.mode.chained_assignment = None
    
def Main(original_train, original_test):
    
    print('开始对 offline_train 进行预处理.....')
    offline_train_handled = dhp.DataHandle(original_train)
    print('开始对 offline_test 进行预处理.....')
    original_test['Date_received'] = original_test['Date_received'].astype('str')
    offline_test_handled = dhp.DataHandle(original_test)
    
    print('开始打标.....')
    offline_train_handled_with_label = sl.SetLabel(offline_train_handled)
    offline_train_handled_with_label['Date_received'] = offline_train_handled_with_label['Date_received'].astype('str')
    
    print('开始划分特征区间.....')
    dataset_1, dataset_2, dataset_3 = sbt.SplitDataset(offline_train_handled_with_label)
    print('开始提取特征区间的特征.....')
    user_feature_1 = euf.ExtractUserFeature(dataset_1)
    user_feature_2 = euf.ExtractUserFeature(dataset_2)
    user_feature_3 = euf.ExtractUserFeature(dataset_3)
    print('用户完成!')
    merchant_feature_1 = emf.ExtractMerchantFeature(dataset_1)
    merchant_feature_2 = emf.ExtractMerchantFeature(dataset_2)
    merchant_feature_3 = emf.ExtractMerchantFeature(dataset_3)
    print('商户完成!')
    discount_feature_1 = edf.ExtraceDiscountFeature(dataset_1)
    discount_feature_2 = edf.ExtraceDiscountFeature(dataset_2)
    discount_feature_3 = edf.ExtraceDiscountFeature(dataset_3)
    print('折扣完成!')
    user_merchant_feature_1 = eumf.ExtraceUserMerchantFeature(dataset_1)
    user_merchant_feature_2 = eumf.ExtraceUserMerchantFeature(dataset_2)
    user_merchant_feature_3 = eumf.ExtraceUserMerchantFeature(dataset_3) 
    print('用户-商户完成!')
    user_discount_feature_1 = eudf.ExtraceUserDiscountFeature(dataset_1)
    user_discount_feature_2 = eudf.ExtraceUserDiscountFeature(dataset_2)
    user_discount_feature_3 = eudf.ExtraceUserDiscountFeature(dataset_3)
    print('用户-折扣完成!')
    
    print('开始划分空闲区间.....')
    kong_1, kong_2, kong_3 = sbt.SplitKong(original_train)
    print('开始提取空闲区间的特征.....')
    kong_user_feature_1, kong_merchant_feature_1, kong_user_merchant_feature_1 = ekf.ExtractKongFeature(kong_1)
    kong_user_feature_2, kong_merchant_feature_2, kong_user_merchant_feature_2 = ekf.ExtractKongFeature(kong_2)
    kong_user_feature_3, kong_merchant_feature_3, kong_user_merchant_feature_3 = ekf.ExtractKongFeature(kong_3)
    
    print('开始划分标签区间.....')
    label_1, label_2, label_3 = sbt.SplitLabel(offline_train_handled_with_label, offline_test_handled)
    print('开始提取标签区间的特征.....')
    label_feature_1 = elf.ExtraceLabelFeature(label_1)
    label_feature_2 = elf.ExtraceLabelFeature(label_2)
    label_feature_3 = elf.ExtraceLabelFeature(label_3)
    
    print('开始生成训练集.....')
    train = gd.GenTrain(user_feature_2, merchant_feature_2, user_merchant_feature_2, discount_feature_2, user_discount_feature_2, label_feature_2, kong_user_feature_2, kong_merchant_feature_2, kong_user_merchant_feature_2)
    print('训练集的大小：', train.shape)
    print('开始生成验证集.....')
    validation = gd.GenValidation(user_feature_1, merchant_feature_1, user_merchant_feature_1, discount_feature_1, user_discount_feature_1, label_feature_1, kong_user_feature_1, kong_merchant_feature_1, kong_user_merchant_feature_1)
    print('验证集的大小：', validation.shape)
    print('开始生成测试集.....')
    test = gd.GenTest(user_feature_3, merchant_feature_3, user_merchant_feature_3, discount_feature_3, user_discount_feature_3, label_feature_3, kong_user_feature_3, kong_merchant_feature_3, kong_user_merchant_feature_3)
    print('测试集的大小：', test.shape)
    
    return train, validation, test

def Model(train, validation, test):
    '''训练模型并进行结果融合'''
    train_y = train['Label']
    train_x = train.drop('Label', axis = 1)
    test_name = test[['User_id', 'Coupon_id', 'Date_received']]
    test_x = test.drop(['User_id', 'Coupon_id', 'Date_received'], axis = 1)
    validation_y = validation['Label']
    validation_x = validation.drop('Label', axis = 1)
    
    print('开始组合训练集和验证集，共同生成训练集.....')
    train_and_vali_x = train_x.append(validation_x)
    train_and_vali_y = train_y.append(validation_y)
    
    train_set = xgb.DMatrix(train_and_vali_x, label = train_and_vali_y)
    test_set = xgb.DMatrix(test_x)
    
    params = {
            'booster':'gbtree',
            'objective': 'binary:logistic',
            'min_child_weight':5,
            'max_depth':5,
            'subsample':0.7,
            'colsample_bytree':0.7,
            'eta': 0.01,
            'eval_metric':'auc'
            }
    num_boost_round = 3500
    
    print('开始训练xgboost，很慢，请耐心等待.....')
    model = xgb.train(params, train_set, num_boost_round = num_boost_round)
    print('训练结束，开始预测.....')
    xgb_result = test_name.copy()
    xgb_result['Probability'] = model.predict(test_set)
    
    print('开始训练gbdt，很慢，请耐心等待.....')
    model = GradientBoostingClassifier()
    model.fit(train_and_vali_x, train_and_vali_y)
    gbdt_result = test_name.copy()
    print('训练结束，开始预测.....')
    gbdt_result['Probability'] = model.predict_proba(test_x)[:,1]
    
    print('两个模型都训练结束，开始进行融合.....')
    result = test_name.copy()
    result['Probability'] = 0.6 * xgb_result['Probability'] + 0.4 * gbdt_result['Probability']
    print('开始保存结果.....')
    if os.path.exists('../result/'):
        result.to_csv('../reslut/submit.csv', index = False)
    else:
        os.path.exists('../result/')
        result.to_csv('../reslut/submit.csv', index = False)
    print('保存结束，去提交吧!!!!!!!!!!')

if __name__ == '__main__':
    
    print('开始读取未处理的训练集offline_train.....')
    offline_train = pd.read_csv('../data_original/ccf_offline_stage1_train.csv')
    print('开始读取未处理的测试集offline_test.....')
    offline_test = pd.read_csv('../data_original/ccf_offline_stage1_test_revised.csv')
    
    train, validation, test = Main(offline_train, offline_test)
    Model(train, validation, test)