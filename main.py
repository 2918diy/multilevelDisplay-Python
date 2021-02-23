from flask import Flask
from flask import request
from flask import make_response
from flask import redirect
from flask_cors import CORS, cross_origin

import pandas as pd
from functools import reduce
import numpy as np
import uuid

import json
import storage


app = Flask(__name__)
CORS(app)

def get_array_boolean(array_array):
    return list(map(lambda x: reduce(lambda m,n:m and n,x),np.array(array_array).transpose()))

def get_result(data,level_name,level_index,value,parentBoolean,level_setting,field,columnFilter=None):
    parentBoolean = list(parentBoolean)
    columnBoolean = pd.Series([True]*data.shape[0])
    currentBoolean = (data[level_name]==value).to_list()
    typeABoolean = get_array_boolean([parentBoolean,currentBoolean,columnBoolean])
    typeBBoolean = get_array_boolean([parentBoolean,currentBoolean]) 
    
    maxLevel  = len(level_setting.keys())

    if type(columnFilter)!=type(None):
        columnBoolean = reduce(lambda x,y:(data[list(x.keys())[0]]==list(x.values())[0])&(data[list(y.keys())[0]]==list(y.values())[0]),columnFilter)
    if ((level_index+1)== maxLevel):
        if ((data[parentBoolean][level_setting[level_index]]).isna().all()):
            return {}
        else:
            return {'{}{}${}'.format(' '*level_index,value,uuid.uuid4()):data[typeABoolean][field].sum()}
    elif ((level_index+1)> maxLevel):
        pass
    else:
        if ((data[parentBoolean][level_setting[level_index+1]]).isna().all()):
            return {'{}{}${}'.format(' '*level_index,value,uuid.uuid4()):data[typeABoolean][field].sum()}
        else:
            children = data[typeBBoolean][level_setting[level_index+1]].unique()
            result = {}
            for child in children:
                if child !='':
                    result = {**result,**get_result(data,level_setting[level_index+1],level_index+1,child,parentBoolean & (data[level_name]==value),level_setting,'入账金额借减贷',columnFilter)}
            return {**{'{}{}${}'.format(' '*level_index,value,uuid.uuid4()):data[typeABoolean][field].sum()},**result}

@app.route('/')
def Main():
    return 'Welcome to the Test API!'


@app.route("/lemon/get_Multilevel_Result/", methods=["post"])
@cross_origin()
def insert_items():
    args = json.loads(request.get_data().decode("utf-8"))
    # result = test.insert_items(args['tableName'],args['dataCollection'])
    level_setting = {**{0:'tool'},**dict(zip(range(1,len(args['level'])+1),args['level']))}
    data = pd.DataFrame(data=args['data'][1:],columns=args['data'][0])
    data['tool'] = 'result'
    result_1 =  get_result(data,'tool',0,data['tool'].unique()[0],[True]*data.shape[0],level_setting,args['field'])
    final_result = [{k.split('$')[0]:result_1[k]} for k in result_1.keys()]
    return (json.dumps(final_result,ensure_ascii=False)).encode('utf8')

if __name__ == "__main__":
    app.run(debug = True,host='127.0.0.1', port=5000,ssl_context=('localhost.crt','localhost.key'))