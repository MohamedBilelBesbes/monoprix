from flask import Flask, render_template , request
from datetime import datetime
from pmdarima import auto_arima
import pickle
import pandas as pd
import os
import json
from gevent.pywsgi import WSGIServer
from dateutil import relativedelta


app = Flask(__name__)

@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/uploading')
def uploading():
    return render_template('uploading.html')

@app.route('/graphed')
def graphed():
    return render_template('graphed.html')

@app.route('/graphed',methods=['POST'])
def prediction():
    x_test = [[x for x in request.form.values()]]
    # ['shop_id', 'item_id' , 'Price' , 'starting date' , 'period']
    try:
        x_test[0][0] = int(x_test[0][0])
        x_test[0][1] = int(x_test[0][1])
        mnth = x_test[0][3]
        x_test[0][2] = float(x_test[0][2])
        numberofmonths = int(x_test[0][4])
    except:
        return render_template('index.html',
        prediction_text=
  'You forgot to log in a certain value')
    try:
        startdate = datetime.strptime(x_test[0][3]+'-01', '%Y-%m-%d').date()
    except:
        return render_template('index.html',
        prediction_text=
  'item {} in shop {} does exist in our data but there seems to be a problem with the date that you putted; please submit a date by this form : yyyy-mm'.format(x_test[0][1],x_test[0][0]))
    d = {
    'shop_id': [x_test[0][0]],
    'item_id': [x_test[0][1]],
    'Price': [x_test[0][2]]}
    try:
        model = pickle.load(open('models/shop'+str(x_test[0][0])+'item'+str(x_test[0][1])+'.pkl', 'rb'))
        df = pd.read_csv('datasets/shop'+str(x_test[0][0])+'item'+str(x_test[0][1])+'.csv', index_col=None)
    except:
        return render_template('index.html', 
        prediction_text=
  'item {} in shop {} does not exist in our data'.format(x_test[0][1],x_test[0][0]))
    lastdateindf = df['month'].sort_values().iloc[-1]
    firstdateindf = df['month'].sort_values().iloc[0]
    lastdateindf = datetime.strptime(lastdateindf, '%Y-%m-%d').date()
    firstdateindf = datetime.strptime(firstdateindf, '%Y-%m-%d').date()
    df["month"] = pd.to_datetime(df["month"])
    startdiffdate = (startdate.year - firstdateindf.year) * 12 + (startdate.month - firstdateindf.month)
    diffdate = (startdate.year - lastdateindf.year) * 12 + (startdate.month - lastdateindf.month)
    if startdiffdate < 0:
        return render_template('index.html',
        prediction_text=
  'item {} in shop {} does exist in our data but the date that you putted is earlier than the first date in the dataset'.format(x_test[0][1],x_test[0][0]))
    if diffdate > 0:
        forecaster = model.predict(n_periods=numberofmonths, return_conf_int=False)
        forecaster = forecaster.astype('int64')
        forecaster = list(forecaster)
        for i in range(len(forecaster)):
            if forecaster[i] < 0:
                forecaster[i] = 0
    elif((diffdate - 1)*(-1) > numberofmonths and diffdate != 0):
        forecaster = df["quantity"].iloc[diffdate - 1:diffdate + numberofmonths - 1]
        forecaster = forecaster.astype('int64')
        forecaster = forecaster.tolist()
    elif ((diffdate - 1)*(-1) == numberofmonths):
        forecaster = df["quantity"].iloc[diffdate - 1:]
        forecaster = forecaster.astype('int64')
        forecaster = forecaster.tolist()
    elif (-3 < diffdate < 1):
        forecaster1 = df["quantity"].iloc[diffdate - 1:]
        forecaster1 = forecaster1.astype('int64')
        forecaster1 = forecaster1.tolist()
        forecaster2 = model.predict(n_periods= numberofmonths + diffdate - 1, return_conf_int=False)
        forecaster2 = forecaster2.astype('int64').tolist()
        print(forecaster1)
        print(forecaster2)
        forecaster = forecaster1 + forecaster2
    else:
        return render_template('index.html',
        prediction_text=
  'item {} in shop {} does exist in our data but there seems to be a problem with the date that you putted; please submit a date by this form : yyyy-mm'.format(x_test[0][1],x_test[0][0]))
    dates = []
    prices = []
    for i in range(numberofmonths):
        prices.append(x_test[0][2])
        dates.append(startdate + relativedelta.relativedelta(months=i))
    forecastdata = pd.DataFrame({'month':dates,'price':prices,'quantity':forecaster})
    dfpred = df[["month","quantity"]]
    dfpredict = forecastdata[["month","quantity"]]
    dfpredict["quantity"] = dfpredict["quantity"].astype(float)
    dfpredict = dfpredict.rename({'month': 'Date', 'quantity': 'item_cnt_month'}, axis='columns')
    dfpred = dfpred.rename({'month': 'Date', 'quantity': 'predictions'}, axis='columns')
    ss = dfpred.copy()
    copyhist = df.copy().rename({'Price':'price'}, axis='columns')
    copypred = forecastdata.copy()
    print(copyhist["price"])
    copyhist["total sales"] = copyhist["quantity"] * copyhist["price"]
    copypred["total sales"] = copypred["quantity"] * copypred["price"]
    copyhist["month"] = pd.to_datetime(copyhist["month"]).dt.date
    copyall = copyhist.append(copypred, ignore_index=True)
    copyall["total sales"]=copyall["total sales"].apply(lambda x:round(x,3))
    copyall["price"]=copyall["price"].apply(lambda x:round(x,3))
    ss["shop_id"] = x_test[0][0]
    ss["item_id"] = x_test[0][1]
    result = dfpred.to_html()
    return render_template('graphed.html',data=dfpredict.to_json(),data2=ss.to_json(),ss=copyall.values)


@app.route('/uploading', methods=['POST'])
def process():
    files = request.files["myfile"]
    print(files.filename)
    files.save(os.path.join('./uploads',files.filename))
    tstexcel = False
    tstcsv = False
    try:
        data = pd.read_excel('uploads/' + files.filename,engine="openpyxl")
    except:
        tstexcel = True
        pass
    try:
        data = pd.read_csv(os.path.join('./uploads',files.filename),index_col=None)
    except:
        tstcsv = True
        pass
    if (tstcsv and tstexcel):
        os.remove('./uploads/' + files.filename)
        return render_template('uploading.html', 
  textofdisplay=
  'the uploaded file format is not excel nor .csv, please put the correct type of file',labeling = 'Red')
    if list(data.columns)==['Date', 'shop_id', 'item_id', 'item_category', 'id_struct', 'Price', 'item_cnt_day']:
        dictofdata = dataprocessing(data)
        print("new data preprocessed")
        storing(dictofdata)
        print("new data stored")
        os.remove('./uploads/' + files.filename)
        return render_template('uploading.html', 
  textofdisplay=
  'Process successful! Would you like to add another dataset to enhance your models ?',labeling = 'Green')
    else:
        print('dataset inompatible with what is described below')
        os.remove('./uploads/' + files.filename)
        return render_template('uploading.html', 
  textofdisplay=
  'dataset inompatible with what is described below, please follow follow the guidelines',labeling = 'Red')


def storing(dictofdata):
    for i in dictofdata:
        print(dictofdata)
        p = './datasets/' + str(i) + '.csv'
        if os.path.isfile(p):
            hld = pd.read_csv('./datasets/' + str(i) + '.csv',index_col=None)
            print(hld["month"].dtype)
            hld["month"] = pd.to_datetime(hld["month"]).dt.date
            hld = hld.append(dictofdata[i]).sort_values(by='month')
            hld = hld.drop_duplicates().reset_index().drop(columns=['index'])
            os.remove('./datasets/' + str(i) + '.csv')
            hld.to_csv('./datasets/' + str(i) + '.csv',index=False)
            newmodel = auto_arima(hld['quantity'] , start_p = 0, start_q = 0, max_p = 12, max_q = 12, m=12 , max_order = None , start_P = 0, seasonal = True, d = 1 , D = 1, stepwise = True , error_action ='ignore' , random = False)
            os.remove('./models/' + str(i) + '.pkl')
            with open('./models/' + str(i) + '.pkl', 'wb') as pkl:
                pickle.dump(newmodel, pkl)
            print('model for {} was updated'.format(str(i)))
        else:
            print(dictofdata[i])
            dictofdata[i].to_csv('./datasets/' + str(i) + '.csv',index=False)
            newmodel = auto_arima(dictofdata[i]['quantity'] , start_p = 0, start_q = 0, max_p = 12, max_q = 12, m=12 , max_order = None , start_P = 0, seasonal = True, d = 1 , D = 1, stepwise = True , error_action ='ignore' , random = False)
            with open('./models/' + str(i) + '.pkl', 'wb') as pkl:
                pickle.dump(newmodel, pkl)
            print('model for {} was created'.format(str(i)))
def dataprocessing(data):
    dfcopy = data.copy()
    dfcopy['Date'] = pd.to_datetime(dfcopy['Date'])
    # seperating the month and the year
    dfcopy['month'] = pd.DatetimeIndex(dfcopy['Date']).month.astype(str)
    dfcopy['year'] = pd.DatetimeIndex(dfcopy['Date']).year.astype(str)
    # specifying the month and the year of each data instance
    dfcopy['sortingdate'] = pd.to_datetime([f'{y}-{m}-01' for y, m in zip(dfcopy.year, dfcopy.month)])
    dfcopy['sortingdate'] = pd.to_datetime(dfcopy['sortingdate']).dt.date
    # revoking the specified day in the data instance so we can gather the data on monthly basis, also revoking the item_category because it already exists in the id_struct
    dfcopy = dfcopy.drop(columns=['Date','month','year','item_category'])
    grouped = dfcopy.groupby(['sortingdate','shop_id','item_id'])
    days = set (dfcopy['sortingdate'])
    shops = set (dfcopy['shop_id'])
    items = set (dfcopy['item_id'])
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
    redata = pd.DataFrame(data, columns=['month','shop_id','item_id','price','quantity'])
    redata['quantity'] = redata['quantity'].astype('int64')
    redatacopynmodels = redata.copy()
    # creating dataframes made of couples of shop_id and item_id and placing them in two ddictionaries; one with data that is monovariate and the other with data that is multivariate
    datasetswithdelete = dict()
    structuredgrouped = redatacopynmodels.groupby(['shop_id','item_id'])
    for i in shops:
        for j in items:
            try:
                # holderbygrp = redatacopynmodels[redatacopynmodels['shop_id'] == i][redatacopynmodels['item_id'] == j]
                holderbygrp = structuredgrouped.get_group((i,j)).sort_values(by=['month'])
                1 / holderbygrp.shape[0]
                datasetswithdelete['shop' + str(i) + 'item' + str(j)] = holderbygrp.reset_index().drop(columns=['index','shop_id','item_id'])
            except:
                continue

    return datasetswithdelete

if __name__ == '__main__':
    #app.run(host='localhost', debug=True, port=5000)
    app.debug = True
    app.config['SESSION_TYPE'] = 'filesystem'
    #sess.init_app(app)
    app.debug = True
    # Serve the app with gevent
    http_server = WSGIServer(('0.0.0.0', 5000), app)
    http_server.serve_forever()