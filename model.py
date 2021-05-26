import pandas as pd
import datetime
import pmdarima
from pmdarima import auto_arima
from pandas import datetime


# putting the data in a pandas dataframe
df = pd.read_excel('/content/drive/MyDrive/colab_datasets/20xx.xlsx', index_col=None)


# making a copy of the dataset to work with so if things didn't go well, we have a copy
dfcopy = df.copy()


#  converting the Date column's type to datetime
dfcopy['Date'] = pd.to_datetime(dfcopy['Date'])
# seperating the month and the year
dfcopy['month'] = pd.DatetimeIndex(dfcopy['Date']).month.astype(str)
dfcopy['year'] = pd.DatetimeIndex(dfcopy['Date']).year.astype(str)
# specifying the month and the year of each data instance
dfcopy['sortingdate'] = pd.to_datetime([f'{y}-{m}-01' for y, m in zip(dfcopy.year, dfcopy.month)])
dfcopy['sortingdate'] = pd.to_datetime(dfcopy['sortingdate']).dt.date
# revoking the specified day in the data instance so we can gather the data on monthly basis, also revoking the item_category because it already exists in the id_struct
dfcopy = dfcopy.drop(columns=['Date','month','year','item_category'])



# sorting the dataset 
grouped = dfcopy.groupby(['sortingdate','shop_id','item_id'])



# extracting the possible days, shops, and items
days = set (dfcopy['sortingdate'])
shops = set (dfcopy['shop_id'])
items = set (dfcopy['item_id'])



# reshaping the data by gathering them by month,shop_id, and item_id
data = []
for i in days:
  for j in shops:
    for k in items:
      try:
        holderbyitem = grouped.get_group((i,j,k)) # getting the specified group
      except:
        continue
      if not holderbyitem.empty:
        quantity = holderbyitem['item_cnt_day'].sum() # summing the quantity across one month
        price = holderbyitem['Price'].mean() # averaging the price because the price of a given item can change multiple times within one month
        data.append([i,j,k,price,quantity])
# creating a dataframe that has instances sorted by month
redata = pd.DataFrame(data, columns=['month','shop_id','item_id' ,'quantity'])
redata['quantity'] = redata['quantity'].astype('int64')
# the following line describes the possible ['sortingdate','shop_id','item_id'] groupings given the data that we have. uncommet it and run it to get the output
# dfcopy[['sortingdate','shop_id','item_id']].drop_duplicates().describe()




# making copies of the dataframs to work with
redatacopy = redata.copy()
redatacopyonemodel = redatacopy.copy()
redatacopynmodels = redatacopy.copy()





# in case the data given from the industrial partner are missing for some couples of shop_id and item_id, ASSUMING THAT THE DATA ARE SIMILIAR BETWEEN ALL THE GROUPS FOR THAT COUPLE, we will be replacing them with the data of the corresponding item_id of the shop that sold the most among other shops of that particular product
def change (j):
  avshops = list(set(redatacopynmodels[redatacopynmodels['item_id'] == j]['shop_id']))
  other = list(redatacopynmodels.groupby(['shop_id','item_id']).get_group((k,j)) for k in avshops)
  shapes = [i.shape[0] for i in other]
  max = sorted(shapes)[-1]
  indice = [i for i in range(len(shapes)) if shapes[i] == max][0]
  return other[indice]




  # creating 457 dataframes made of couples of shop_id and item_id and placing them in two ddictionaries; one with data that is monovariate and the other with data that is multivariate
datasetswithdelete = {}
structuredgrouped = redatacopynmodels.groupby(['shop_id','item_id'])
for i in shops:
  for j in items:
    try:
      holderbygrp = redatacopynmodels[redatacopynmodels['shop_id'] == i][redatacopynmodels['item_id'] == j] #holderbygrp = structuredgrouped.get_group((i,j)).sort_values(by=['month'])
      1 / holderbygrp.shape[0]
      datasetswithdelete['shop' + str(i) + 'item' + str(j)] = holderbygrp.reset_index().drop(columns=['index','shop_id','item_id','Price'])
    except:
      continue #in case the shop_id , item_id couple doesn't exist, we create for it an empty dataframe
      #datasetswithdelete['shop' + str(i) + 'item' + str(j)] = change(j).drop(columns=['shop_id' , 'item_id' , 'Price'])
      #datasetswithdelete['shop' + str(i) + 'item' + str(j)] = pd.DataFrame(columns=['month','quantity'])




'''      
# creating 475 dataframes made of couples of shop_id and item_id and placing them in two ddictionaries; one with data that is monovariate and the other with data that is multivariate
datasetswithdelete = {}
structuredgrouped = redatacopynmodels.groupby(['shop_id','item_id'])
for i in shops:
  for j in items:
    try:
      holderbygrp = redatacopynmodels[redatacopynmodels['shop_id'] == i][redatacopynmodels['item_id'] == j] #holderbygrp = structuredgrouped.get_group((i,j)).sort_values(by=['month'])
      datasetswithdelete['shop' + str(i) + 'item' + str(j)] = holderbygrp.reset_index().drop(columns=['index','shop_id','item_id','Price'])
    except: #in case the shop_id , item_id couple doesn't exist, we create for it an empty dataframe
      #datasetswithdelete['shop' + str(i) + 'item' + str(j)] = change(j).drop(columns=['shop_id' , 'item_id' , 'Price'])
      datasetswithdelete['shop' + str(i) + 'item' + str(j)] = pd.DataFrame(columns=['month','quantity'])
'''






# appending zeros for the months that did not score any sales of the given item in the given shop
copyofwithdelete = datasetswithdelete.copy()
for i in copyofwithdelete:
  for j in days:
    try:
      1 / list(copyofwithdelete[i]['month'] == j).count(True)
    except:
      copyofwithdelete[i] = copyofwithdelete[i].append(pd.DataFrame([[j,0]], columns=list(copyofwithdelete[i].columns)))
  copyofwithdelete[i] = copyofwithdelete[i].reset_index().sort_values(by=['month']).drop(columns=['index']).reset_index().drop(columns=['index'])




models = dict()
for i in copyofwithdelete:
  data = pd.Series(copyofwithdelete[i].quantity, index=copyofwithdelete[i].month)
  print(i)
  # stepwise_fit = auto_arima(copyofwithdelete['shop7item790380381']['quantity'] , start_p = 0, start_q = 0, max_p = 12, max_q = 12, m=12 , max_order = None , start_P = 0, seasonal = True, d = 1 , D = 1, stepwise = True , error_action ='ignore' , random = False)
  models[i] = auto_arima(copyofwithdelete[i]['quantity'] , start_p = 0, start_q = 0, max_p = 12, max_q = 12, m=12 , max_order = None , start_P = 0, seasonal = True, d = 1 , D = 1, stepwise = True , error_action ='ignore' , random = False)


'''
forecaster = model.predict(n_periods=20, return_conf_int=False)
forecaster = forecaster.astype('int64')
for i in range(len(forecaster)):
  if forecaster[i] < 0:
    forecaster[i] = 0
'''