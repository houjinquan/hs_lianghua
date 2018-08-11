import numpy as np 
import os
from functools import  partial
import multiprocessing


def save_stock_return(stock_id, path, save_path,Threshold = 0.2, fre = 5, day_num = 242, day_delete_num = 2):
    sto_path = path + "/" +stock_id
    x_finally_data = []
    y_finally_data_1min = []
    y_finally_data_2min = []
    y_finally_data_4min = []
    y_finally_data_8min = []
    
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

    i = 0
    while i < int(len(data_use)/(day_num - day_delete_num)-fre):
        sub_data_use = data_use[((day_num - day_delete_num)*i):((day_num - day_delete_num)*(i+5)),1:]
        index = np.argwhere(sub_data_use[:,3] == 0)
        print(i)

        if len(index) != 0:
            print("停牌/熔断")
            skip = int(index[-1]/(day_num - day_delete_num)+1)
            i += skip
        else:
            res_data = sub_data_use[((day_num - day_delete_num)*4):((day_num - day_delete_num)*5), 3]
            res_1 = (res_data[1:] - res_data[:-1])/res_data[:-1]
        
            if len(np.argwhere(res_1  == 0)) < (len(res_1)*Threshold):
                res_2 = (res_data[2:] - res_data[:-2])/res_data[:-2]
                res_4 = (res_data[4:] - res_data[:-4])/res_data[:-4]
                res_8 = (res_data[8:] - res_data[:-8])/res_data[:-8]
                
                y_finally_data_1min.append(res_1[:-7])
                y_finally_data_2min.append(res_2[:-6])
                y_finally_data_4min.append(res_4[:-4])
                y_finally_data_8min.append(res_8)
                
                x_finally_data.append(sub_data_use)
            
        i += 1 
     
    print(len(x_finally_data))
    print(len(x_finally_data[0]))
    print(np.array(x_finally_data).shape)
    print(len(y_finally_data_1min))
    print(np.array(y_finally_data_1min).shape)
    np.save(save_path + stock_id[:-4] +"-x.npy", x_finally_data) 
    np.save(save_path + stock_id[:-4] +"-y1.npy", y_finally_data_1min) 
    np.save(save_path + stock_id[:-4] +"-y2.npy", y_finally_data_2min) 
    np.save(save_path + stock_id[:-4] +"-y4.npy", y_finally_data_4min) 
    np.save(save_path + stock_id[:-4] +"-y8.npy", y_finally_data_8min)

def get_filelist(path):
    for dirpath, dirnames, filenames in os.walk(path):
        print(filenames)
    return filenames
    
    

if __name__ == "__main__":
    
    path = "D:/hjq/Python/stock_minute"
    save_path = "D:/hjq/Python/stock_min_xy/"
    #stock_id = "000002.SZ.npy"
    #save_stock_return(stock_id = stock_id, path = path, save_path=save_path)
    filelist = get_filelist(path = path)

    pool = multiprocessing.Pool(4)
    pool.map(partial(save_stock_return, path = "D:/hjq/Python/stock_minute",
                     save_path = "D:/hjq/Python/stock_min_xy/"), filelist)
    pool.close()
    pool.join()
    print("计算结束")

    
    











