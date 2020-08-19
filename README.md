# AHA-Moment-Rev

import pandas as pd
import numpy as np
from google.cloud import bigquery
import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials
from gcloud import storage
import os
import pymysql
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')



def func_unique_action(action1first):
    container1=[]
#     count=0
    for company in action1first['company_id'].unique():
#         count+=1
#         print(count)
        total_action=action1first[action1first['company_id']==company]['action'].nunique()
        container1.append([company, total_action])
    df=pd.DataFrame(container1, columns=['company_id','unique_action'])
    return df

def create_df(action_filtered):
    action_filtered=action_filtered[~((action_filtered['action_type']=='ft Terima Pembayaran') | (action_filtered['action_type']=='ft pembayaran tagihan'))]
    action_filtered=action_filtered[~(action_filtered['action']=='login')]
    df_1=pd.DataFrame()
    for company in (action_filtered['company_id'].unique()):
        each_company={'company_id':[company]}
        for ga in (action_filtered[action_filtered['company_id']==company]['action'].unique()):
            number=action_filtered[(action_filtered['company_id']==company)
                      &(action_filtered['action']==ga)].shape[0]
            each_company[ga]=[number]
        cek=pd.DataFrame(each_company)
        df_1=df_1.append(cek)
    df_1=df_1.fillna(0)
    df_1.loc[:,'total_action'] = df_1.sum(axis=1)

    return df_1

#start_date="'2020-01-01'"
end_date='2020-06-30' #ini untuk bates register date company
business_type='b2b'

# Pull data from bq

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credential_bq_paper-prod.json"
client = bigquery.Client()

sql="""
select rdaa.company_id, count(rdaa.action), mst.business_type
from datascience_public.raw_data_all_action as rdaa
left join
(select company_id, business_type, business_child_segmentation from datascience_public.master_segment_table) as mst on rdaa.company_id=mst.company_id
where 
rdaa.testing_account=0 and 
rdaa.blacklist_account=0 and 
rdaa.created_at >'2020-01-01'
and
rdaa.register_date > '2020-01-01'
and 
mst.business_type='{}'
and
mst.business_child_segmentation='food & beverage' 
and 
rdaa.action!='login'
group by rdaa.company_id, mst.business_type
""".format(business_type)
# Run a Standard SQL query with the project set explicitly
project_id = 'paper-prod'
company_id = client.query(sql, project=project_id).to_dataframe()

company_id

pd.set_option('display.max_rows', 500)

company_id

list_company_id=list(company_id['company_id'])

company=''
for item in list_company_id:
    company=company+("'"+item+"',")

sql="""
select *
from datascience_public.raw_data_all_action
where
company_id in 
("""+str(company[:-1])+")"
# Run a Standard SQL query with the project set explicitly
project_id = 'paper-prod'
action = client.query(sql, project=project_id).to_dataframe()

print(action.shape)
action=action.drop_duplicates()
print(action.shape)

## data cleaning

action_filtered=action[['company_id','action','action_type','created_at','register_date']]

action_filtered['created_at']=pd.to_datetime(action_filtered['created_at'])
action_filtered['register_date']=pd.to_datetime(action_filtered['register_date'])

## create feature

total_action=action_filtered.groupby('company_id').count()['action'].reset_index()

last_action_date=action_filtered.groupby('company_id').max()[['created_at','register_date']].reset_index()

last_action_date['day_diff']=(last_action_date['created_at']-last_action_date['register_date']).dt.days
last_action_date['hour_diff']=(last_action_date['created_at']-last_action_date['register_date']).dt.seconds//3600

company_data=pd.merge(last_action_date, total_action, on='company_id')

company_data['business_type']=business_type

company_data['business_type'].value_counts()

company_data=company_data[company_data['business_type']==business_type]

company_data

# Divide population (retain and not retain)

company_data.info()

# population not retain
population1=company_data[(company_data['day_diff']<56)&(company_data['register_date']<end_date)]
sample_population1=population1
sample_population1['label']=0
print(population1.shape)

# population retain
population2=company_data[(company_data['day_diff']>=56)&(company_data['register_date']<end_date)]
sample_population2=population2
sample_population2['label']=1
print(population2.shape)

total_population=pd.concat([sample_population1, sample_population2])

population2

# Action per day

trans=total_population[['company_id','register_date']]

trans['register_date']=pd.to_datetime(trans['register_date'])

trans['1day']=trans['register_date']+dt.timedelta(days=1)
trans['2day']=trans['register_date']+dt.timedelta(days=2)
trans['3day']=trans['register_date']+dt.timedelta(days=3)
trans['7day']=trans['register_date']+dt.timedelta(days=7)

count=0
first_day=pd.DataFrame()
for company in trans['company_id'].unique():
#     count+=1
#     print(count)
    df1=action_filtered[(action_filtered['company_id']==company)&
           (action_filtered['created_at']<=(trans[trans['company_id']==company]['1day'].iloc[0]))]
    first_day=first_day.append(df1).reset_index().drop('index', axis=1)

count=0
second_day=pd.DataFrame()
for company in trans['company_id'].unique():
#     count+=1
#     print(count)
    df1=action_filtered[(action_filtered['company_id']==company)&
            (action_filtered['created_at']<=(trans[trans['company_id']==company]['2day'].iloc[0]))]
    second_day=second_day.append(df1).reset_index().drop('index', axis=1)

count=0
third_day=pd.DataFrame()
for company in trans['company_id'].unique():
#     count+=1
#     print(count)
    df1=action_filtered[(action_filtered['company_id']==company)&
            (action_filtered['created_at']<=(trans[trans['company_id']==company]['3day'].iloc[0]))]
    third_day=third_day.append(df1).reset_index().drop('index', axis=1)

count=0
seventh_day=pd.DataFrame()
for company in trans['company_id'].unique():
#     count+=1
#     print(count)
    df1=action_filtered[(action_filtered['company_id']==company)&
            (action_filtered['created_at']<=(trans[trans['company_id']==company]['7day'].iloc[0]))]
    seventh_day=seventh_day.append(df1).reset_index().drop('index', axis=1)

action1first=first_day[first_day['company_id'].isin(sample_population1['company_id'])]
action1second=second_day[second_day['company_id'].isin(sample_population1['company_id'])]
action1third=third_day[third_day['company_id'].isin(sample_population1['company_id'])]
action1seventh=seventh_day[seventh_day['company_id'].isin(sample_population1['company_id'])]

action2first=first_day[first_day['company_id'].isin(sample_population2['company_id'])]
action2second=second_day[second_day['company_id'].isin(sample_population2['company_id'])]
action2third=third_day[third_day['company_id'].isin(sample_population2['company_id'])]
action2seventh=seventh_day[seventh_day['company_id'].isin(sample_population2['company_id'])]

## Unique action per day
output: population 1(1) dan 2(2)
df1
df1second
df1third

df1=func_unique_action(action1first)

df2=func_unique_action(action2first)

df1second=func_unique_action(action1second)

df2second=func_unique_action(action2second)

df1third=func_unique_action(action1third)

df2third=func_unique_action(action2third)

df1seventh=func_unique_action(action1seventh)

df2seventh=func_unique_action(action2seventh)

## list_action per day per company

data=pd.concat([action1first, action2first])
main_first=create_df(data)

data=pd.concat([action1second, action2second])
main_second=create_df(data)

data=pd.concat([action1third, action2third])
main_third=create_df(data)

data=pd.concat([action1seventh, action2seventh])
main_seventh=create_df(data)

# merge all data

# Day 1
data_training1=pd.merge(total_population[['company_id','day_diff','label']], main_first, on='company_id', how='left')
unique_action_day1=pd.concat([df1, df2])
data_training1=pd.merge(data_training1, unique_action_day1, on='company_id', how='left')
data_training1=data_training1.fillna(0)
data_training1['ratio']=data_training1['total_action']/data_training1['unique_action']

# Day 2
data_training2=pd.merge(total_population[['company_id','day_diff','label']], main_second, on='company_id', how='left')
unique_action_day2=pd.concat([df1second, df2second])
data_training2=pd.merge(data_training2, unique_action_day2, on='company_id', how='left')
data_training2=data_training2.fillna(0)
data_training2['ratio']=data_training2['total_action']/data_training2['unique_action']

# Day 3
data_training3=pd.merge(total_population[['company_id','day_diff','label']], main_third, on='company_id', how='left')
unique_action_day3=pd.concat([df1third, df2third])
data_training3=pd.merge(data_training3, unique_action_day3, on='company_id', how='left')
data_training3=data_training3.fillna(0)
data_training3['ratio']=data_training3['total_action']/data_training3['unique_action']

# Day 7
data_training7=pd.merge(total_population[['company_id','day_diff','label']], main_seventh, on='company_id', how='left')
unique_action_day7=pd.concat([df1seventh, df2seventh])
data_training7=pd.merge(data_training7, unique_action_day7, on='company_id', how='left')
data_training7=data_training7.fillna(0)
data_training7['ratio']=data_training7['total_action']/data_training7['unique_action']

data_training7

# calculate intersection zone

def aha(data_training2,parameter):
    output=[]
    for i in range (1,20):
        retain_fulfill=data_training2[(data_training2['label']==1)&(data_training2[parameter]>=i)].shape[0]
        retain_not_fulfill=data_training2[(data_training2['label']==1)&(~(data_training2[parameter]>=i))].shape[0]
        fulfill=data_training2[data_training2[parameter]>=i].shape[0]
        output.append([i,retain_not_fulfill,retain_fulfill,fulfill])
    df_aha=pd.DataFrame(output, columns=['parameter','retain_not_fulfill','retain_fulfill','fulfill'])
    df_aha['total_user']=df_aha['retain_not_fulfill']+df_aha['fulfill']
    df_aha['overlap']=df_aha['retain_fulfill']*100/(df_aha['total_user'])
    return df_aha

def aha2(data_training2,parameter1, parameter2):
    output=[]
    for i in range (1,20):
        retain_fulfill=data_training2[(data_training2['label']==1)&((data_training2[parameter2]>=i)&(data_training2[parameter2]>=i))].shape[0]
        retain_not_fulfill=data_training2[(data_training2['label']==1)&(~((data_training2[parameter2]>=i)&(data_training2[parameter2]>=i)))].shape[0]
        fulfill=data_training2[((data_training2[parameter2]>=i)&(data_training2[parameter2]>=i))].shape[0]
        output.append([i,retain_not_fulfill,retain_fulfill,fulfill])
    df_aha=pd.DataFrame(output, columns=['parameter','retain_not_fulfill','retain_fulfill','fulfill'])
    df_aha['total_user']=df_aha['retain_not_fulfill']+df_aha['fulfill']
    df_aha['overlap']=df_aha['retain_fulfill']*100/(df_aha['total_user'])
    return [parameter1, parameter2, df_aha['overlap'].max()]

def aha2fix(data_training2,parameter1, parameter2):
    output=[]
    for i in range (1,20):
        retain_fulfill=data_training2[(data_training2['label']==1)&((data_training2[parameter2]>=i)&(data_training2[parameter2]>=i))].shape[0]
        retain_not_fulfill=data_training2[(data_training2['label']==1)&(~((data_training2[parameter2]>=i)&(data_training2[parameter2]>=i)))].shape[0]
        fulfill=data_training2[((data_training2[parameter2]>=i)&(data_training2[parameter2]>=i))].shape[0]
        output.append([i,retain_not_fulfill,retain_fulfill,fulfill])
    df_aha=pd.DataFrame(output, columns=['parameter','retain_not_fulfill','retain_fulfill','fulfill'])
    df_aha['total_user']=df_aha['retain_not_fulfill']+df_aha['fulfill']
    df_aha['overlap']=df_aha['retain_fulfill']*100/(df_aha['total_user'])
    return df_aha

def aha3(data_training2,parameter1, parameter2, parameter3):
    output=[]
    for i in range (1,20):
        retain_fulfill=data_training2[(data_training2['label']==1)&((data_training2[parameter1]>=i)&(data_training2[parameter2]>=i)&(data_training2[parameter3]>=i))].shape[0]
        retain_not_fulfill=data_training2[(data_training2['label']==1)&(~((data_training2[parameter1]>=i)&(data_training2[parameter2]>=i)&(data_training2[parameter3]>=i)))].shape[0]
        fulfill=data_training2[((data_training2[parameter1]>=i)&(data_training2[parameter2]>=i)&(data_training2[parameter3]>=i))].shape[0]
        output.append([i,retain_not_fulfill,retain_fulfill,fulfill])
    df_aha=pd.DataFrame(output, columns=['parameter','retain_not_fulfill','retain_fulfill','fulfill'])
    df_aha['total_user']=df_aha['retain_not_fulfill']+df_aha['fulfill']
    df_aha['overlap']=df_aha['retain_fulfill']*100/(df_aha['total_user'])
    return [parameter1, parameter2,parameter3, df_aha['overlap'].max()]

def aha3fix(data_training2,parameter1, parameter2, parameter3):
    output=[]
    for i in range (1,20):
        retain_fulfill=data_training2[(data_training2['label']==1)&((data_training2[parameter1]>=i)&(data_training2[parameter2]>=i)&(data_training2[parameter3]>=i))].shape[0]
        retain_not_fulfill=data_training2[(data_training2['label']==1)&(~((data_training2[parameter1]>=i)&(data_training2[parameter2]>=i)&(data_training2[parameter3]>=i)))].shape[0]
        fulfill=data_training2[((data_training2[parameter1]>=i)&(data_training2[parameter2]>=i)&(data_training2[parameter3]>=i))].shape[0]
        output.append([i,retain_not_fulfill,retain_fulfill,fulfill])
    df_aha=pd.DataFrame(output, columns=['parameter','retain_not_fulfill','retain_fulfill','fulfill'])
    df_aha['total_user']=df_aha['retain_not_fulfill']+df_aha['fulfill']
    df_aha['overlap']=df_aha['retain_fulfill']*100/(df_aha['total_user'])
    return df_aha

data_training2.columns

# <day 1
for item in data_training1.columns[3:]:
    print(item)
    print(aha(data_training1, item))
    print('-----------------------------------------------')

# <day 2
for item in data_training2.columns[3:]:
    print(item)
    print(aha(data_training2, item))
    print('-----------------------------------------------')

# <day 3
for item in data_training2.columns[3:]:
    print(item)
    print(aha(data_training3, item))
    print('-----------------------------------------------')





# < day 7
for item in data_training2.columns[3:]:
    print(item)
    print(aha(data_training7, item))
    print('-----------------------------------------------')

aha(data_training7, 'invoice_sales')

aha(data_training7, 'partner')

aha(data_training7, 'total_action')

## Multi parameter *need further improvement

combination2=[]
for item in data_training2.columns[3:-2]:
    for item2 in data_training2.columns[4:-2]:
#         print(item + item2)
        combination2.append(aha2(data_training2, item, item2))
#         print(aha2(data_training2, item, item2))
#         print('-----------------------------------------------')
df_combination2=pd.DataFrame(combination2)
df_combination2[df_combination2[2]>20]

combination2=[]
for item in data_training2.columns[3:-2]:
    for item2 in data_training2.columns[4:-2]:
#         print(item + item2)
        combination2.append(aha2(data_training3, item, item2))
#         print(aha2(data_training2, item, item2))
#         print('-----------------------------------------------')
df_combination2=pd.DataFrame(combination2)
df_combination2[df_combination2[2]>20]

combination2=[]
for item in data_training2.columns[3:-2]:
    for item2 in data_training2.columns[4:-2]:
#         print(item + item2)
        combination2.append(aha2(data_training7, item, item2))
#         print(aha2(data_training2, item, item2))
#         print('-----------------------------------------------')
df_combination2=pd.DataFrame(combination2)
df_combination2[df_combination2[2]>20]



# combination3=[]
# for item in data_training2.columns[4:-2]:
#     for item2 in data_training2.columns[4:-2]:
#         for item3 in data_training2.columns[4:-2]:
# #         print(item + item2)
#             combination3.append(aha3(data_training2, item, item2, item3))
# #         print(aha2(data_training2, item, item2))
# #         print('-----------------------------------------------')
# df_combination3=pd.DataFrame(combination3)
# df_combination3[df_combination3[3]>20]

# combination3=[]
# for item in data_training2.columns[4:-2]:
#     for item2 in data_training2.columns[4:-2]:
#         for item3 in data_training2.columns[4:-2]:
# #         print(item + item2)
#             combination3.append(aha3(data_training3, item, item2, item3))
# #         print(aha2(data_training2, item, item2))
# #         print('-----------------------------------------------')
# df_combination3=pd.DataFrame(combination3)
# df_combination3[df_combination3[3]>20]

# combination3=[]
# for item in data_training2.columns[4:-2]:
#     for item2 in data_training2.columns[4:-2]:
#         for item3 in data_training2.columns[4:-2]:
# #         print(item + item2)
#             combination3.append(aha3(data_training7, item, item2, item3))
# #         print(aha2(data_training2, item, item2))
# #         print('-----------------------------------------------')
# df_combination3=pd.DataFrame(combination3)
# df_combination3[df_combination3[3]>20]

aha3fix(data_training7, 'invoice_sales', 'partner', 'unique_action')



# df_combination3.sort_values([3]).tail(50)

# df_combination3[((df_combination3[0]!=df_combination3[1])&(df_combination3[0]!=df_combination3[2])
#                  &(df_combination3[1]!=df_combination3[2]))].sort_values([3]).tail(50)

# df_combination3[df_combination3[3]>34].sort_values([3]).tail(60)

# df_combination3[3].max()

aha(data_training7, 'unique_action')

aha(data_training7, 'total_action')

aha(data_training7, 'partner')

aha(data_training7, 'invoice_sales')

aha(data_training7, 'stock')

aha(data_training7, 'product')

aha(data_training7, 'coa')



