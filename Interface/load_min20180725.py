import numpy as np 
import os
import datetime
import gc
from functools import  partial
import multiprocessing
from numpy import float32


def save_stock_return(stock_id, path,Threshold = 0.2, fre = 3, day_num = 242, day_delete_num = 2, skip = 2):
    ###保存本地所有股票的收益率
    sto_path = path + "/" +stock_id
    x_finally_data = []
    y_finally_data_1min = []
    y_finally_data_2min = []
    y_finally_data_4min = []
    y_finally_data_8min = []
    id_date = []
    day_len = day_num - day_delete_num
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

    data_use_s = np.delete(init_data, [index1, index3], axis = 0)
    suspension = data_use_s[:, 3]
    suspension_d = np.unique(np.floor(np.argwhere(suspension == 0)/(day_num-day_delete_num)).astype(np.int))
    suspension_date = data_use_s[(suspension_d + 1) * day_len, 0]
    data_use = np.delete(data_use_s, np.array([np.arange(i*day_len, day_len*(i+1)) for i in suspension_d]).reshape(len(suspension_d)*day_len,1), axis = 0)
    ### 数据的计算

    i = 0
    while i < int(len(data_use)/(day_num - day_delete_num)-fre):
        sub_data_date = data_use[((day_num - day_delete_num)*(i+fre)),0].strftime("%Y%m%d")
        sub_data_use = data_use[((day_num - day_delete_num)*i):((day_num - day_delete_num)*(i+fre)),1:]
        sub_time = data_use[((day_num - day_delete_num) * i):((day_num - day_delete_num) * (i + fre)), 0]

        if data_use[(i+fre-1)*day_len, 0] in suspension_date:
            i += skip

        res_data = sub_data_use[((day_num - day_delete_num)*(fre-1)):((day_num - day_delete_num)*fre), 3]
        res_1 = (res_data[1:] - res_data[:-1])/res_data[:-1]
        
        if len(np.argwhere(res_1  == 0)) < (len(res_1)*Threshold):
            res_2 = (res_data[2:] - res_data[:-2])/res_data[:-2]
            res_4 = (res_data[4:] - res_data[:-4])/res_data[:-4]
            res_8 = (res_data[8:] - res_data[:-8])/res_data[:-8]

            time_d = []
            for j in np.arange(len(sub_time)):
                sec = time.mktime(sub_time[j].timetuple())/86400 -int(time.mktime(sub_time[j].timetuple())/86400)
                time_d.append(sec)

            y_finally_data_1min.append(res_1[:-7])
            y_finally_data_2min.append(res_2[:-6])
            y_finally_data_4min.append(res_4[:-4])
            y_finally_data_8min.append(res_8)
            x_finally_data.append(np.hstack((sub_data_use, np.array(time_d).reshape(len(time_d), 1))))
            id_date.append(np.array([stock_id[:-4], sub_data_date]))

            
        i += 1 
    ####返回原始数据和收益率数据 
    return(x_finally_data, y_finally_data_1min, y_finally_data_2min, y_finally_data_4min, y_finally_data_8min, id_date)


def get_filelist(path):
    ####获取文件列表
    for dirpath, dirnames, filenames in os.walk(path):
        print(filenames)
    return filenames
    

def add_data(data):  
    x_data.extend(data[0])
    y1_data.extend(data[1])
    y2_data.extend(data[2])
    y4_data.extend(data[3])
    y8_data.extend(data[4])
    stock_inf.extend(data[5])
    
    print(len(x_data))

    
    if filelist[:30][-1][:-7] == stock_inf[-1][0][:6]:  
        np.save(save_path + str(len(order)+1) + "-x_data", x_data)
        np.save(save_path + str(len(order)+1) + "-y1_data", y1_data)
        np.save(save_path + str(len(order)+1) + "-y2_data", y2_data)
        np.save(save_path + str(len(order)+1) + "-y4_data", y4_data)
        np.save(save_path + str(len(order)+1) + "-y8_data", y8_data)
        np.save(save_path + str(len(order)+1) + "-stock_inf", stock_inf)
  
    if len(x_data) >300:
        order.append("a")
        print(len(order))
        np.save(save_path + str(len(order)) + "-x_data", x_data)
        np.save(save_path + str(len(order)) + "-y1_data", y1_data)
        np.save(save_path + str(len(order)) + "-y2_data", y2_data)
        np.save(save_path + str(len(order)) + "-y4_data", y4_data)
        np.save(save_path + str(len(order)) + "-y8_data", y8_data)
        np.save(save_path + str(len(order)) + "-stock_inf", stock_inf)
         
        x_data.clear()
        y1_data.clear()
        y2_data.clear()
        y4_data.clear()
        y8_data.clear()
        stock_inf.clear()         
        gc.collect()
        
     


if __name__ == "__main__":
    
    path = "D:/hjq/Python/stock_minute"
    save_path = "D:/hjq/Python/stock_min_xy/"
    x_data = []
    y1_data = []
    y2_data = []
    y4_data = []
    y8_data = []
    stock_inf = []
    order = []
    slice_len = 50   ##所有股票的每次切片长度
    filelist = get_filelist(path = path)
    
    ###用30只股票测试 
    pool = multiprocessing.Pool(3)
    for i in np.arange(len(filelist[:30])):
        stock_code = filelist[i]
        pool.apply_async(partial(save_stock_return, path = "D:/hjq/Python/stock_minute"),
                         (stock_code,), callback=add_data)
    
    pool.close()
    pool.join()
    
    print("计算结束")
    


    
    
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




    
    











