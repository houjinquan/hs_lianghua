import os
import numpy as np
import datetime
import time
import warnings
warnings.filterwarnings("ignore")
import math
import multiprocessing
import gc
import config
import random

pwd=config.pwd_dir_all
y_len_array=np.array(config.y_len_list)
x_len_expand=config.x_len_expand
x_len=config.x_len
y_len=config.y_len
not_suspended_days=config.not_suspended_days
compressed_factor=5

x_len2=x_len*compressed_factor
x_data=[]
x_data2=[]
y_data=[]
stock_info_data=[]
file_num=[]

start_date=datetime.datetime.strptime('2014-12-15','%Y-%m-%d')


data_path=config.npy_path+'/data'
    

changed_date=np.loadtxt(pwd+'/changed_date.txt',  dtype=bytes)
all_stock_minute_pwd='/data/qin/stock_all/stock_minute'
# changed_datetime=changed_date[:,1]
changed_date=changed_date[:,0]
changed_date=np.append(changed_date, '2018-06-14'.encode())
begin_date_list=[]
def get_compressed_data(x_price,x_volume,compressed_factor=1):
    
    tmp_x_len=x_volume.shape[0]
    if compressed_factor > 1:
        tmp_x_price=[]
        tmp_x_volume=[]
        i = 0
        while i < x_price.shape[0]:
            this_open=x_price[i,0]
            this_high=np.max(x_price[i:i+compressed_factor,1])
            this_low=np.min(x_price[i:i+compressed_factor,2])
            this_close=x_price[i+compressed_factor-1,3]
            this_volume=np.sum(x_volume[i:i+compressed_factor])
            tmp_x_price.append([this_open,this_high,this_low,this_close])
            tmp_x_volume.append(this_volume)
            i+=compressed_factor
        tmp_x_price=np.array(tmp_x_price)
        tmp_x_volume=np.array(tmp_x_volume)
    else:
        tmp_x_price=x_price
        tmp_x_volume=x_volume
    tmp_x_price=tmp_x_price/np.max(tmp_x_price[:,1])
    tmp_x_volume=tmp_x_volume/np.max(tmp_x_volume)
    return np.float32(np.append(tmp_x_price, tmp_x_volume.reshape(int(tmp_x_len/compressed_factor),1), axis=1))

def clean_data(stock_data):
    i=0
    while i<stock_data.shape[0]:
        if np.sum(stock_data[i:i+242,5])==0:
            stock_data=np.delete(stock_data, np.arange(i,i+242,1), 0)
            continue
        stock_data[i+1,1]=stock_data[i,1]
        stock_data[i+1,5]+=stock_data[i,5]
        
        stock_data[i+240,4]=stock_data[i+241,4]
        stock_data[i+240,5]+=stock_data[i+241,5]
        i+=242
    index=np.tile([0,241],(int(stock_data.shape[0]/242),1))+np.tile(
        (np.arange(0,int(stock_data.shape[0]/242),1)*242).reshape((-1,1)),(1,2))
    stock_data=np.delete(stock_data, index.reshape(-1), 0)
    return stock_data

def add_data(data):
    if data_path_list[-1] != tmp_data_path:
        if(not os.path.exists(data_path_list[-1])):
            os.makedirs(data_path_list[-1])
        np.save(data_path_list[-1]+'/x_data.npy', x_data)
        np.save(data_path_list[-1]+'/y_data.npy', y_data)
        np.save(data_path_list[-1]+'/x_data2.npy', x_data2)
        np.save(data_path_list[-1]+'/stock_info_data.npy', stock_info_data)
        
        x_data.clear()
        x_data2.clear()
        y_data.clear()
        stock_info_data.clear()
        gc.collect()
        data_path_list.append(tmp_data_path)
        
    print(len(data[0]))
    x_data.extend(data[0])
    stock_info_data.extend(data[1])
    x_data2.extend(data[2])
    y_data.extend(data[3])
    data_len=len(x_data)
    print(data_len)

def gen_data(stock_code,begin_date,end_date):
    print('1--'+stock_code)
    tmp_filepath=all_stock_minute_pwd+'/'+stock_code+'.npy'
    tmp_data=np.load(tmp_filepath)
    tmp_data=tmp_data[tmp_data[:,0]<(end_date+datetime.timedelta(days = 1)),:]
    tmp_begin_index = np.argwhere(tmp_data[:,0] < begin_date).shape[0]
    if tmp_begin_index - x_len2 < 0:
        tmp_begin_index=0
    else:
        tmp_begin_index-=math.ceil(x_len2/242)*242
    tmp_data=tmp_data[tmp_begin_index:,:]
    tmp_data=clean_data(tmp_data)
    j=math.ceil(x_len2/240)*240
    tmp_x_data=[]
    tmp_x_data2=[]
    tmp_y_data=[]
    stock_info=[]
    while j < tmp_data.shape[0]-y_len:
        stock_minute_time=tmp_data[j,0]
        tmp_stock_minute=tmp_data[j-x_len2+1:j+y_len+1,1:]
        zeros_index=np.argwhere(tmp_stock_minute[:,3]==0)
        if zeros_index.shape[0]>0:
            j+=zeros_index[-1,0]+1
            residue=240-(j+1)%240
            if residue<y_len:
                j+=residue
            continue
        tmp_last_price=tmp_stock_minute[x_len2-1:,:-1]
        tmp_y=tmp_stock_minute[x_len2:,3]/tmp_stock_minute[x_len2-1,3]-1
        if np.max(tmp_last_price) != np.min(tmp_last_price) or random.randint(0,19) == 0:
            if np.argwhere(tmp_y[y_len_array-1] == 0).shape[0]==0 or random.randint(0,4) == 0:
                tmp_x_price2=tmp_stock_minute[:x_len2,:-1]
                tmp_x_volume2=tmp_stock_minute[:x_len2,-1]
                tmp_x_data2.append(get_compressed_data(tmp_x_price2,tmp_x_volume2,compressed_factor))
                tmp_x_data.append(get_compressed_data(tmp_x_price2[-x_len:,:],tmp_x_volume2[-x_len:]))
                tmp_y_data.append(tmp_stock_minute[x_len2:,3]/tmp_stock_minute[x_len2-1,3]-1)
                stock_info.append([stock_code,stock_minute_time])
        residue=240-(j+1)%240
        if residue<=y_len:
            j+=residue
        else:
            j+=1
    print('2--'+stock_code)
    return tmp_x_data,stock_info,tmp_x_data2,tmp_y_data

data_path_list=[]
i=0

tmp_data_path=data_path+'/'+start_date.strftime("%Y-%m-%d")
data_path_list.append(tmp_data_path)
while i < len(changed_date)-1:
    begin_date=datetime.datetime.strptime(changed_date[i].decode(),'%Y-%m-%d') 
    end_date=datetime.datetime.strptime(changed_date[i+1].decode(),'%Y-%m-%d') 
    if start_date>=end_date:
        i+=1
        continue
    
    stock_codes=np.loadtxt(pwd+'/changed_date/'+changed_date[i].decode()+'.txt', dtype=bytes)
    
    
    if start_date>begin_date:
        begin_date=start_date
    
    tmp_end_date=end_date
    if end_date > start_date+datetime.timedelta(days=60):
        tmp_end_date=start_date+datetime.timedelta(days=60)
    else:
        i+=1
    
    
    pool = multiprocessing.Pool(processes=26)
    
    for j in range(len(stock_codes)):
        stock_code=stock_codes[j].decode()
#         tmp_x_data,stock_info,tmp_x_data2,tmp_y_data=gen_data(stock_code,begin_date,tmp_end_date)
        
        pool.apply_async(gen_data,args=(stock_code,begin_date,tmp_end_date,),callback=add_data)
       
    pool.close()
    pool.join()
    print(start_date)
    
    if end_date > tmp_end_date: 
        start_date=tmp_end_date
        tmp_data_path=data_path+'/'+start_date.strftime("%Y-%m-%d")
        
if(not os.path.exists(tmp_data_path)):
    os.makedirs(tmp_data_path)

np.save(tmp_data_path+'/x_data.npy', x_data)
np.save(tmp_data_path+'/y_data.npy', y_data)
np.save(tmp_data_path+'/x_data2.npy', x_data2)
np.save(tmp_data_path+'/stock_info_data.npy', stock_info_data)

x_data.clear()
x_data2.clear()
y_data.clear()
stock_info_data.clear()
    


