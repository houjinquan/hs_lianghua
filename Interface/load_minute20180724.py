import numpy as np 
import os
import gc
from functools import  partial
import multiprocessing
import config
import random

drop_rate = 0.5
count = 0

def save_stock_return(stock_id, path, Threshold = 0.2, fre = 5, day_num = 242, day_delete_num = 2):
    print('1--'+stock_id)
    ###保存本地所有股票的收益率
    sto_path = path + "/" +stock_id
    x_finally_data = []
    y_finally_data_1min = []
    y_finally_data_2min = []
    y_finally_data_4min = []
    y_finally_data_8min = []
    id_date = []
    ###数据的整合过程
    init_data = np.load(sto_path)
    index1 = [i*day_num for i in np.arange(int(len(init_data)/day_num))] 
    index2 = [i+1 for i in index1]
    index3 = [((i+1)*day_num-1) for i in np.arange(int(len(init_data)/day_num))]
    index4 = [i-1 for i in index3]
    
    init_data[index2, 1] = init_data[index1, 1]
    init_data[index3, 4] = init_data[index1, 4]
    init_data[index2, 5] = init_data[index2, 5] + init_data[index1, 5]
    init_data[index4, 5] = init_data[index4, 5] + init_data[index3, 5]

    data_use = np.delete(init_data, [index1, index3], axis = 0)
    ### 数据的计算

    i = 0
    while i < int(len(data_use)/(day_num - day_delete_num)-fre):
        sub_data_date = data_use[((day_num - day_delete_num)*(i+fre)),0].date()
        sub_data_use = data_use[((day_num - day_delete_num)*i):((day_num - day_delete_num)*(i+fre)),1:]
        index = np.argwhere(sub_data_use[:,3] == 0)

        if len(index) != 0:
            #print("停牌/熔断")
            skip = int(index[-1]/(day_num - day_delete_num)+1)
            i += skip
            continue

        res_data = sub_data_use[((day_num - day_delete_num)*(fre-1)):((day_num - day_delete_num)*fre), 3]
        res_1 = (res_data[1:] - res_data[:-1])/res_data[:-1]
        
        if len(np.argwhere(res_1  == 0)) > (len(res_1)*drop_rate):
            i += 1
            continue
        if len(np.argwhere(res_1  == 0)) < (len(res_1)*Threshold) or random.randint(0,9) == 0:
            res_2 = (res_data[2:] - res_data[:-2])/res_data[:-2]
            res_4 = (res_data[4:] - res_data[:-4])/res_data[:-4]
            res_8 = (res_data[8:] - res_data[:-8])/res_data[:-8]
                
                
            y_finally_data_1min.append(np.float32(res_1[:-7]))
            y_finally_data_2min.append(np.float32(res_2[:-6]))
            y_finally_data_4min.append(np.float32(res_4[:-4]))
            y_finally_data_8min.append(np.float32(res_8))
            x_finally_data.append(np.float32(sub_data_use))
            id_date.append(np.array([stock_id[:-4] ,sub_data_date]))

            
        i += 1 
    print('2--'+stock_id)
    ####返回原始数据和收益率数据 
    return(x_finally_data, y_finally_data_1min, y_finally_data_2min, y_finally_data_4min, y_finally_data_8min, id_date)


def get_filelist(path):
    ####获取文件列表
    for dirpath, dirnames, filenames in os.walk(path):
        print(filenames)
    return filenames
    

def add_data(data):
    print(len(data[0]))
    x_data.extend(data[0])
    y1_data.extend(data[1])
    y2_data.extend(data[2])
    y4_data.extend(data[3])
    y8_data.extend(data[4])
    stock_inf.extend(data[5])
    
    
    print(len(x_data))
    
        

    if len(x_data) > 100000:
        global count
        count+=1
        tmp_path=save_path + '/' + str(count)
        if(not os.path.exists(tmp_path)):
            os.makedirs(tmp_path)
        np.save(tmp_path + "/x_data", x_data)
        np.save(tmp_path + "/y1_data", y1_data)
        np.save(tmp_path + "/y2_data", y2_data)
        np.save(tmp_path + "/y4_data", y4_data)
        np.save(tmp_path + "/y8_data", y8_data)
        np.save(tmp_path + "/stock_inf", stock_inf)
         
        x_data.clear()
        y1_data.clear()
        y2_data.clear()
        y4_data.clear()
        y8_data.clear()
        stock_inf.clear()         
        gc.collect()

     


if __name__ == "__main__":
    
    path = '/data/qin/stock_all/stock_minute'
    save_path = config.all_npy_path
    x_data = []
    y1_data = []
    y2_data = []
    y4_data = []
    y8_data = []
    stock_inf = []
#     slice_len = 50   ##所有股票的每次切片长度
    filelist = get_filelist(path = path)
    

    pool = multiprocessing.Pool(26)
    for i in np.arange(len(filelist)):
        stock_code = filelist[i]
        pool.apply_async(partial(save_stock_return, path = path),
                         (stock_code,), callback=add_data)
    
    pool.close()
    pool.join()
    
    print("计算结束")
    
    count+=1
    tmp_path=save_path + '/' + str(count)
    if(not os.path.exists(tmp_path)):
        os.makedirs(tmp_path)
    np.save(tmp_path + "/x_data", x_data)
    np.save(tmp_path + "/y1_data", y1_data)
    np.save(tmp_path + "/y2_data", y2_data)
    np.save(tmp_path + "/y4_data", y4_data)
    np.save(tmp_path + "/y8_data", y8_data)
    np.save(tmp_path + "/stock_inf", stock_inf)
    
'''    
    for i in np.arange(int(len(filelist)/slice_len)+1):
        pool = multiprocessing.Pool(4)
        data_result = pool.map(partial(save_stock_return, path = "D:/hjq/Python/stock_minute"),
                                filelist[(i*slice_len):((i+1)*slice_len)])
        
        pool.close()
        pool.join()
        print(i)
            
        for j in np.arange(len(filelist[(i*slice_len):((i+1)*slice_len)])):
            x_data.extend(data_result[j][0])
            y1_data.extend(data_result[j][1])
            y2_data.extend(data_result[j][2])
            y4_data.extend(data_result[j][3])
            y8_data.extend(data_result[j][4])
            stock_inf.extend(data_result[j][5])
        
        print(len(x_data))    
        np.save(save_path + str(i) +"-x_data.npy", x_data)
        np.save(save_path + str(i) +"-y1_data.npy", y1_data)
        np.save(save_path + str(i) +"-y2_data.npy", y2_data)
        np.save(save_path + str(i) +"-y4_data.npy", y4_data)
        np.save(save_path + str(i) +"-y8_data.npy", y8_data)
        np.save(save_path + str(i) +"-stock_inf.npy", stock_inf)
        
        x_data.clear()
        y1_data.clear()
        y2_data.clear()
        y4_data.clear()
        y8_data.clear()
        stock_inf.clear()
        
        gc.collect()
       
    print("计算结束")

''' 




    
    











