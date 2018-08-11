import pandas as pd
import numpy as np
import os

path = "D:/hjq/con_test/1min/"
save_path = "D:/hjq/con_save/1min/"


def get_folderandfile(path):
    folder = []
    files = []
    for dir, temp_folder, temp_files in os.walk(path):
        files.append(temp_files)
        if len(temp_folder) == 0:
            pass
        else:
            folder.append(temp_folder)
    return folder, files[1:]
        
    
all_files = get_folderandfile(path)

for i in np.arange(len(all_files[0][0])):
    start_data =[]
    for j in np.arange(len(all_files[1][i])):
        temp_path = path + all_files[0][0][i] + "/" + all_files[1][i][j]
        print(temp_path)
        temp_data = pd.read_csv(temp_path, sep="\t")
        temp_data = temp_data[["dataDate","barTime", "openPrice","highPrice","lowPrice", 
                               "closePrice", "totalVolume"]]
        save_data = temp_data.values
        if len(start_data) == 0:
            start_data.append(save_data)
            continue
        
        if all_files[1][i][j][:6] == all_files[1][i][j-1][:6]:
            start_data.append(save_data)
        else:
            temp_save_path = save_path + all_files[0][0][i]
            if(not os.path.exists(temp_save_path)):
                os.makedirs(temp_save_path)
            np.save(temp_save_path +"/"+ all_files[1][i][j-1][:6] + ".npy", start_data)
            start_data = []
            
        if (j+1) == len(all_files[1][i]):
            np.save(save_path + all_files[0][0][i] +"/"+ all_files[1][i][j-1][:6] + ".npy", start_data)
            
















