import pandas as pd
import numpy as np
import datetime
import os
import uqer


def save_stock_min_data(stock_id, start_date, end_date, min):
    stock_data = uqer.DataAPI.MktBarHistDateRangeGet(stock_id, startDate=start_date,endDate=end_date, unit=min)
    stock_data_need = stock_data[["dataDate", "ticker","exchangeCD", "shortNM", "barTime", "closePrice",
                                  "openPrice", "highPrice", "lowPrice","totalVolume", "totalValue"]]
    dataDate = np.unique(stock_data_need["dataDate"])
    for date in dataDate:
        need_save_data = stock_data_need[stock_data_need["dataDate"] == date]
        if not os.path.exists(stock_data_save_path + str(min) + "min/" + stock_id[:6]):
            os.makedirs(stock_data_save_path + str(min) + "min/" + stock_id[:6])
        need_save_data.to_csv(stock_data_save_path + str(min) + "min/" + stock_id[:6]+"/" + date.replace("-","") + ".csv")


def save_all_stock_min_data(stock_list, start_date, end_date, min):
    for i in stock_list:
        save_stock_min_data(i, start_date=start_date, end_date=end_date, min=min)


def save_stock_tick_data(stock_id, date):
    stock_tick = uqer.DataAPI.SHSZTicksHistOneDayGet(ticker=stock_id[:6], tradeDate=str(date), exchangeCD=stock_id[-4:])
    if not os.path.exists(stock_data_save_path + "ticker/" + stock_id[:6]):
        os.makedirs(stock_data_save_path + "ticker/" + stock_id[:6])
    stock_tick.to_csv(stock_data_save_path + "ticker/" + stock_id[:6] + "/" + str(date) + ".csv")


def save_all_stock_tick_data(stock_list, date):
    for i in stock_list:
        save_stock_tick_data(i, date=date)


def save_future_min_date(future_id, start_date, end_date, min):
    future_data = uqer.DataAPI.MktFutureBarHistDateRangeGet(instrumentID=future_id, startDate=str(start_date),
                                                            endDate=str(end_date), unit=min)
    future_data_need = future_data[["dataDate", "ticker", "barTime", "openPrice", "highPrice", "lowPrice", "closePrice", "totalVolume"]]
    dataDate = np.unique(future_data_need["dataDate"])
    for date in dataDate:
        need_save_data = future_data_need[future_data_need["dataDate"] == date]
        if not os.path.exists(future_data_save_path + str(min) + "min/" + future_id):
            os.makedirs(future_data_save_path + str(min) + "min/" + future_id)
        need_save_data.to_csv(future_data_save_path + str(min) + "min/" + future_id + "/" + date.replace("-","") + ".csv")


def save_all_future_min_data(future_list, start_date, end_date, min):
    for i in future_list:
        save_future_min_date(i, start_date=start_date, end_date=end_date, min=min)


def save_future_tick_data(future_id, date):
    future_tick = uqer.DataAPI.MktFutureTicksHistOneDayGet(instrumentID=future_id, date=str(date))
    if not os.path.exists(future_data_save_path + "ticker/" + future_id):
        os.makedirs(future_data_save_path + "ticker/" + future_id)
    future_tick.to_csv(future_data_save_path + "ticker/" + future_id + "/" + str(date) + ".csv")


def save_all_future_tick_data(future_list, date):
    for i in future_list:
        save_future_tick_data(i, date)


def save_index_min_data(index_id, date, min=1):
    index_data = uqer.DataAPI.MktBarHistDateRangeGet(index_id, startDate=date,endDate=date, unit=min)
    index_data_need = index_data[["ticker", "dataDate", "barTime", "openPrice", "highPrice", "lowPrice", "closePrice", "totalVolume"]]
    if not os.path.exists(index_data_save_path + str(min) + "min/" + index_id[:6]):
        os.makedirs(index_data_save_path + str(min) + "min/" + index_id[:6])
    index_data_need.to_csv(index_data_save_path + str(min) + "min/" + index_id[:6]+"/" + str(date) + ".csv")


def save_all_index_min_data(index_list, date, min):
    for i in index_list:
        save_index_min_data(i, date=date, min=min)


def save_index_tick_data(index_id, date):
    index_tick = uqer.DataAPI.SHSZTicksHistOneDayGet(ticker=index_id[:6], tradeDate=str(date), exchangeCD=index_id[-4:])
    if not os.path.exists(index_data_save_path + "tick/" + index_id[:6]):
        os.makedirs(index_data_save_path + "tick/" + index_id[:6])
    index_tick.to_csv(index_data_save_path + "tick/" + index_id[:6] + "/" + str(date) + ".csv")


def save_all_index_tick_data(index_list, date):
    for i in index_list:
        save_index_tick_data(i, date=date)


if __name__ == "__main__":
    client = uqer.Client(token='5e834b742dd1f7d2822b835fdf91088377b8630152426222813c4929cdccfa69')
    stock_data_save_path = "D:/hjq/data/stocks/"
    future_data_save_path = "D:/hjq/data/futures/"
    index_data_save_path = "D:/hjq/data/indexs/"
    opt_data_save_path = "D:/hjq/data/opts/"
    date = datetime.datetime.now().strftime("%Y%m%d")

    deal_date_df = pd.read_excel("C:/Users/thinkcentre/Desktop/deal_date.xlsx")
    deal_date = deal_date_df.values

    date_use = []
    for i in np.arange(len(deal_date)):
        deal_date_str = str(deal_date[i][0])
        date_use.append(deal_date_str[:4] + deal_date_str[5:7] + deal_date_str[8:10])

    # save stocks data
    stock_id = uqer.DataAPI.EquGet(equTypeCD=u"A", field=["ticker", "exchangeCD", "listStatusCD"], pandas="1")
    stock_id_use = stock_id[stock_id["listStatusCD"] == "L"]
    stock_id_use["stock_id"] = stock_id_use["ticker"] + "." + stock_id_use["exchangeCD"]

    need_min = [1, 5, 30, 60]
    for j in need_min:
        print(j,"s")
        save_all_stock_min_data(stock_id_use["stock_id"], start_date=20180701,
                                end_date=20180731, min=j)
    # for k in date_use:
    #     print(k,"s")
    #     save_all_stock_tick_data(stock_id_use["stock_id"], k)



    # save futures data
    future_id = uqer.DataAPI.FutuGet(exchangeCD=["XZCE", "CCFX", "XSGE", "XDCE"],
                                     field=["ticker", "contractStatus"],
                                     pandas="1")
    future_id_use = future_id[future_id["contractStatus"] == "L"]
    # b = future_id_use["ticker"]
    # save_all_future_min_data(future_id_use["ticker"][:2], start_date=20180701,
    #                          end_date=20180731, min=30)
    # save_all_future_tick_data(b, 20180726)

    for j in need_min:
        print(j,"f")
        save_all_future_min_data(future_id_use["ticker"][:2], start_date=20180701,
                                 end_date=20180731, min=j)
    # for k in date_use:
    #     print(k,"f")
    #     save_all_future_tick_data(future_id_use["ticker"][:2], k)



    '''
    # save indexs data
    save_all_index_min_data(["000001.XSHG", "000300.XSHG"], 20180725, 5)
    save_all_index_tick_data(["000001.XSHG", "000300.XSHG"], 20180725)
    '''














