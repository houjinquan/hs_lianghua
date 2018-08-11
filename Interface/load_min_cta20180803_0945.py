import numpy as np 
import os
import time
import datetime
import gc
import multiprocessing
import config
import random


def save_stock_return(stock_id, path, return_list = [1,2,4,8], Threshold = 0.2, fre = 5, day_num = 242, day_delete_num = 2, skip = 1):
    # 保存本地所有股票的收益率
    sto_path = path + "/" + stock_id
    x_finally_data = []
    y_finally_data = []
    for _ in return_list:
        y_finally_data.append([])
    #     y_finally_data_1min = []
    #     y_finally_data_2min = []
    #     y_finally_data_4min = []
    #     y_finally_data_8min = []
    id_date = []
    day_len = day_num - day_delete_num

    # 数据的整合过程
    init_data = np.load(sto_path)
    index1 = np.arange(int(len(init_data) / day_num)) * day_num
    index2 = index1 + 1
    index3 = (np.arange(int(len(init_data) / day_num)) + 1) * day_num - 1
    index4 = index3 - 1
    index5 = np.argwhere(init_data[index1, 1] == 0)
    index6 = [i + 1 for i in index5]
    index7 = np.argwhere(init_data[index3, 1] == 0)
    index8 = [i - 1 for i in index7]

    if len(index5) > 0:
        init_data[index5, 1] = init_data[index6, 1]
        init_data[index7, 4] = init_data[index8, 4]
    init_data[index2, 1] = init_data[index1, 1]
    init_data[index3, 4] = init_data[index1, 4]
    init_data[index2, 5] = init_data[index2, 5] + init_data[index1, 5]
    init_data[index4, 5] = init_data[index4, 5] + init_data[index3, 5]

    data_use_sub = np.delete(init_data, [index1, index3], axis=0)
    # 剔除全天停牌数据
    delete_index = []
    for i in np.arange(int(len(data_use_sub) / day_len)):
        sta_data = np.sum(data_use_sub[day_len * i:day_len * (i + 1), 1:])
        if sta_data == 0:
            delete_index.extend(np.arange(day_len * i, day_len * (i + 1)))

    # 保留剔除数据的时间和所在位置
    delete_day = np.unique(np.floor(np.array(delete_index) / day_len).astype(np.int))
    delete_day_date = data_use_sub[delete_day * day_len, 0]
    data_use = np.delete(data_use_sub, delete_index, axis=0)

    # 数据的整合过程
    i = 0
    while i < int(len(data_use) / day_len - fre - 1):
        # 切片数据
        sub_data_date = data_use[(day_len * (i + fre) - 1), 0].strftime("%Y%m%d")
        sub_data_use = data_use[(day_len * i):(day_len * (i + fre) + max(return_list)), 1:]
        sub_time = data_use[(day_len * i):(day_len * (i + fre)), 0]
        index = np.argwhere(np.min(sub_data_use[:, :4], 1) == 0)

        # 剔除x_data不连续的切片数据
        if len(index) != 0:
            no_use = int(index[-1] / day_len + 1)
            i += no_use
            continue

        logit = np.logical_and(data_use[i * day_len, 0] < delete_day_date,
                               data_use[(i + fre - 1) * day_len, 0] > delete_day_date)
        if any(logit):
            skip_date = delete_day_date[np.argwhere(logit > 0)[0]][0]
            data_day = data_use[np.arange(i * day_len, day_len * (i + fre), day_len), 0]
            breakpoint_position = np.array([i.days for i in (skip_date - data_day)])
            skip_mun = np.argwhere(breakpoint_position < 0)[0]

            if skip_mun == 4:
                skip = 2
            elif skip_mun == 3:
                skip = 1

            i += skip
            continue

        res_data = sub_data_use[(day_len * (fre - 1)):(day_len * fre + max(return_list)), 3]
        res_1 = ((res_data[1:] - res_data[:-1]) / res_data[:-1]).astype(np.float32)

        if len(np.argwhere(res_1 == 0)) < (len(res_1) * Threshold):
            for res in np.arange(len(return_list)):
                return_data = ((res_data[return_list[res]:] - res_data[:-return_list[res]]) / res_data[:-return_list[
                    res]]).astype(np.float32)
                if (res + 1) == len(return_list):
                    y_finally_data[res].append(return_data)
                else:
                    y_finally_data[res].append(return_data[:-(return_list[-1] - return_list[res])])

            #             res_2 = ((res_data[2:] - res_data[:-2])/res_data[:-2]).astype(np.float32)
            #             res_4 = ((res_data[4:] - res_data[:-4])/res_data[:-4]).astype(np.float32)
            #             res_8 = ((res_data[8:] - res_data[:-8])/res_data[:-8]).astype(np.float32)

            time_d = []
            for j in np.arange(len(sub_time)):
                sec = time.mktime(sub_time[j].timetuple()) / (60 * 60 * 24) - int(
                    time.mktime(sub_time[j].timetuple()) / (60 * 60 * 24))
                time_d.append(sec)

            #             y_finally_data_1min.append(res_1[:-7])
            #             y_finally_data_2min.append(res_2[:-6])
            #             y_finally_data_4min.append(res_4[:-4])
            #             y_finally_data_8min.append(res_8)
            x_finally_data.append(np.hstack((sub_data_use[:-max(return_list)],
                                             np.array(time_d).reshape(len(time_d), 1))))
            id_date.append(np.array([stock_id[:-4], sub_data_date]))

        i += 1
    ####返回原始数据和收益率数据
    return (x_finally_data, y_finally_data, id_date)


def get_filelist(path):
    ####获取文件列表
    for dirpath, dirnames, filenames in os.walk(path):
        print(filenames)
    return filenames
    

def add_data(data):  
    print(len(data[0]))
    x_data.extend(data[0])
#     y1_data.extend(data[1])
#     y2_data.extend(data[2])
#     y4_data.extend(data[3])
#     y8_data.extend(data[4])
    for list_n in np.arange(len(y_data)):
        y_data[list_n].extend(data[1][list_n])
        
    stock_inf.extend(data[2])
    print(len(x_data))
    
    if len(x_data) >100000:
        order.append(len(x_data))
        print(len(order))
        if(not os.path.exists(save_path + str(len(order)))):
            os.makedirs(save_path + str(len(order)))
        np.save(save_path + str(len(order)) + "/x_data", np.float32(x_data))
#         np.save(save_path + str(len(order)+1) + "-y1_data", np.float32(y1_data))
#         np.save(save_path + str(len(order)+1) + "-y2_data", np.float32(y2_data))
#         np.save(save_path + str(len(order)+1) + "-y4_data", np.float32(y4_data))
#         np.save(save_path + str(len(order)+1) + "-y8_data", np.float32(y8_data))
        for data in np.arange(len(return_list)):
            np.save(save_path + str(len(order)) + "/y" + str(return_list[data])+ "_data", y_data[data])
            
        np.save(save_path + str(len(order)) + "/stock_inf", stock_inf)
         
        x_data.clear()
#         y1_data.clear()
#         y2_data.clear()
#         y4_data.clear()
#         y8_data.clear()
        for mint in np.arange(len(return_list)):
            y_data[mint].clear()

        stock_inf.clear()         
        gc.collect()
        
     


if __name__ == "__main__":
    path = '/data/qin/stock_all/stock_minute'
    save_path = config.all_npy_path+'/'
    return_list = [1,3,7,15]
#     y1_data = []
#     y2_data = []
#     y4_data = []
#     y8_data = []
    stock_inf = []
    order = []
    x_data = []
    y_data = []
    for _ in return_list:
        y_data.append([])
#     slice_len = 50   ##所有股票的每次切片长度
    filelist = get_filelist(path = path)
    
    ###用30只股票测试 
    pool = multiprocessing.Pool(26)
    for i in np.arange(len(filelist)):
        stock_code = filelist[i]
        pool.apply_async(save_stock_return,
                         (stock_code, path, return_list,), callback=add_data)

    pool.close()
    pool.join()

    print("计算结束")
    # test = save_stock_return("000004.SZ.npy", path = path)
    # print(test[2])
    order.append(len(x_data))
    print(len(order))
    if(not os.path.exists(save_path + str(len(order)))):
        os.makedirs(save_path + str(len(order)))
    np.save(save_path + str(len(order)) + "/x_data", np.float32(x_data))
#         np.save(save_path + str(len(order)+1) + "-y1_data", np.float32(y1_data))
#         np.save(save_path + str(len(order)+1) + "-y2_data", np.float32(y2_data))
#         np.save(save_path + str(len(order)+1) + "-y4_data", np.float32(y4_data))
#         np.save(save_path + str(len(order)+1) + "-y8_data", np.float32(y8_data))
    for data in np.arange(len(return_list)):
        np.save(save_path + str(len(order)) + "/y" + str(return_list[data])+ "_data", y_data[data])
        
    np.save(save_path + str(len(order)) + "/stock_inf", stock_inf)





    
    
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




    
    











