import os
import numpy as np
import pandas as pd
import re
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import logging
import yfinance as yf

logging.basicConfig(level=logging.DEBUG)
regex = re.compile(pattern=r"\d{4}")

# df_pbr_path = r"C:\Users\Andyfish\.conda\envs\fs_env\codes\data\PBR_201409_202406.xlsx"
# df_pbr_raw = pd.read_excel(df_pbr_path)
# df_fsdata = pd.read_excel(r"C:\Users\Andyfish\.conda\envs\fs_env\codes\data\fscore_data.xlsx")

#刪舊檔
def remove_old_files(prod):
    stock_file_path = "stock_file"
    dt_today = datetime.now().strftime("%Y-%m-%d")

    list_file_paths = os.listdir(f"{stock_file_path}\\")
    regex1 = re.compile(r"{pname}_10y_\d\d\d\d-\d\d-\d\d.csv".format(pname=prod))

    #Data path with TWII treatment
    data_paths = [regex1.findall(i)[0] for i in list_file_paths if regex1.findall(i)!=[]]
    if prod == "TWII":
        prod = r"^TWII"
        for i,n in enumerate(data_paths):
            data_paths[i] = r"^"+n
    if data_paths != [] and data_paths[-1] != f"{stock_file_path}\\{prod}_10y_{dt_today}.csv": 
        data_paths.pop(-1)
        for p in data_paths:
            os.remove(f"{stock_file_path}\\{p}")

#更新10年資料
def updating_10y(prod,interval_="1d",force_update=False):
    prod = prod
    df_10y = pd.DataFrame()
    dt_today = (datetime.now()).strftime("%Y-%m-%d")
    stock_file_path = "stock_file"

    if not os.path.exists(f"{stock_file_path}"):
        os.mkdir(f"{stock_file_path}")

    if force_update:
        df_1y = yf.download(prod,period="1y",interval=interval_,auto_adjust=False,multi_level_index=False).loc[:,["Open","High","Low","Close","Adj Close","Volume"]]
        return df_1y

    #若無目標檔就下載，有則直接讀取
    list_file_paths = os.listdir(f"{stock_file_path}\\")
    regex1 = re.compile(r"{pname}_10y_\d\d\d\d-\d\d-\d\d.csv".format(pname=prod))
    last_prod_data_path = [regex1.findall(i) for i in list_file_paths if regex1.findall(i)!=[]]
    if last_prod_data_path != [] : last_prod_data_path = last_prod_data_path[-1][-1]
    
    if  last_prod_data_path == []:
        if prod == "TWII" : prod = r"^TWII"
        try:
            df_10y = yf.download(prod,period="10y",interval=interval_,auto_adjust=False,multi_level_index=False).loc[:,["Open","High","Low","Close","Adj Close","Volume"]]
            df_10y.to_csv(f"{stock_file_path}\\{prod}_10y_{dt_today}.csv",encoding = 'utf-8')
            df_10y = pd.read_csv(f"{stock_file_path}\\{prod}_10y_{dt_today}.csv",index_col=0,parse_dates=True)
            logging.info(f"{prod}:已嘗試初次下載")
            if len(df_10y) == 0:
                logging.debug(f"{prod}:載入空資料 嘗試 .TWO")
                df_10y = yf.download(prod+"O",period="10y",interval=interval_,auto_adjust=False,multi_level_index=False).loc[:,["Open","High","Low","Close","Adj Close","Volume"]]
                df_10y.to_csv(f"{stock_file_path}\\{prod}_10y_{dt_today}.csv",encoding = 'utf-8')
                df_10y = pd.read_csv(f"{stock_file_path}\\{prod}_10y_{dt_today}.csv",index_col=0,parse_dates=True)
                logging.debug(f"{prod}:已嘗試再次下載")
                if len(df_10y) == 0:
                    logging.debug(f"{prod}:切換finmind源下載---------------------")
                    
        except Exception as err:
            logging.warning("下載錯誤",err)

    #若存在就更新
    else:
        if prod == "TWII" : 
            prod = r"^TWII"
            last_prod_data_path = r"^"+last_prod_data_path
        
        #今日已經更新
        if f"{stock_file_path}\\{last_prod_data_path}" == f"{stock_file_path}\\{prod}_10y_{dt_today}.csv":
            df_10y = pd.read_csv(f"{stock_file_path}\\{last_prod_data_path}",index_col=0,parse_dates=True)
            logging.info(f"{prod}:成功載入價量資料(今日已更新)")
            remove_old_files(prod)
        
        #已存在未更新
        else:
            df_tmp_1 = pd.read_csv(f"{stock_file_path}\\{last_prod_data_path}",index_col=0,parse_dates=True)
            sd = (df_tmp_1.index[-1] + timedelta(days=1)).strftime("%Y-%m-%d")
            ed = dt_today 
            
            #TMP2 DL
            if sd == ed:
                df_tmp_2 = yf.download(prod,interval=interval_,period="1d",auto_adjust=False,multi_level_index=False).loc[:,["Open","High","Low","Close","Adj Close","Volume"]]
                if len(df_tmp_2) == 0:
                    df_tmp_2 = yf.download(prod+"O",interval=interval_,period="1d",auto_adjust=False,multi_level_index=False).loc[:,["Open","High","Low","Close","Adj Close","Volume"]]
            else:
                df_tmp_2 = yf.download(prod,interval=interval_,start=sd,end=ed,auto_adjust=False,multi_level_index=False).loc[:,["Open","High","Low","Close","Adj Close","Volume"]]
                if len(df_tmp_2) == 0:
                    df_tmp_2 = yf.download(prod+"O",interval=interval_,start=sd,end=ed,auto_adjust=False,multi_level_index=False).loc[:,["Open","High","Low","Close","Adj Close","Volume"]]

            #TMP concat and save
            df_tmp_1 = df_tmp_1.drop(index=df_tmp_1[df_tmp_1.index < datetime.strptime(ed,"%Y-%m-%d") - relativedelta(years=10)].index,axis=0)
            df_10y = pd.concat([df_tmp_1,df_tmp_2])
            df_10y.to_csv(f"{stock_file_path}\\{prod}_10y_{dt_today}.csv",encoding = 'utf-8')
            logging.info(f"{prod}:成功載入價量資料(已更新)")

            if prod == "^TWII" : prod = prod[1:]
            remove_old_files(prod)
    return df_10y

def replace_sid(df1):
    global regex
    df1.reset_index(inplace=True,drop=True)
    for i in range(len(df1)):
        df1.loc[i,"sid"] = regex.findall(df1.loc[i,"sid"])[0]
    return df1

def get_pool(ym,pbr_raw,fsdata):
    filter_ym = ym
    global regex
    df_pbr_raw = pbr_raw
    df_fsdata = fsdata

    #篩選依B/M 排名股票
    df_pbr_raw = df_pbr_raw[df_pbr_raw["ym"] == filter_ym]
    df_pbr_raw.reset_index(inplace=True,drop=True)
    df_pbr = replace_sid(df_pbr_raw)

    bm = 1 / df_pbr.loc[:,"pb_s"]
    df_pbr.loc[:,"bm"] = bm
    df_pbr = df_pbr.sort_values(by="bm",ascending=False)
    bm_ranked = df_pbr["bm"].quantile(0.9)
    df_bm_top = df_pbr[df_pbr["bm"] >= bm_ranked]
    df_bm_top = df_bm_top.reset_index()

    #輸出目標股票代號
    df_bm_top = replace_sid(df_bm_top)
    pool = df_bm_top["sid"].sort_values().to_list()

    #載入基本面資料
    df_fsdata = df_fsdata.sort_values(["sid","ym"])

    #當季資料
    df_fsdata_f = df_fsdata[df_fsdata["ym"] == filter_ym]
    df_fsdata_f.reset_index(inplace=True,drop=True)
    df_fsdata_f = replace_sid(df_fsdata_f)
    list_sids = pool or df_fsdata_f["sid"].to_list()
    mask_sids = []
    for i in df_fsdata_f["sid"].to_list():
        if i in pool:
            mask_sids.append(True)
        else:
            mask_sids.append(False)
    df_fsdata_f = df_fsdata_f.loc[mask_sids]
    df_fsdata_f.reset_index(inplace=True,drop=True)

    #前一年同季資料
    df_fsdata_f_prev = df_fsdata[df_fsdata["ym"] == filter_ym - 100]
    df_fsdata_f_prev.reset_index(inplace=True,drop=True)
    df_fsdata_f_prev = replace_sid(df_fsdata_f_prev)
    list_sids = pool or df_fsdata_f_prev["sid"].to_list()
    mask_sids = []
    for i in df_fsdata_f_prev["sid"].to_list():
        if i in pool:
            mask_sids.append(True)
        else:
            mask_sids.append(False)
    df_fsdata_f_prev = df_fsdata_f_prev.loc[mask_sids]
    df_fsdata_f_prev.reset_index(inplace=True,drop=True)

    # #前二年同季資料
    # df_fsdata_f_prev = df_fsdata[df_fsdata["ym"] == filter_ym - 200]
    # df_fsdata_f_prev.reset_index(inplace=True,drop=True)
    # df_fsdata_f_prev = replace_sid(df_fsdata_f_prev)
    # list_sids = pool or df_fsdata_f_prev["sid"].to_list()
    # mask_sids = []
    # for i in df_fsdata_f_prev["sid"].to_list():
    #     if i in pool:
    #         mask_sids.append(True)
    #     else:
    #         mask_sids.append(False)
    # df_fsdata_f_prev = df_fsdata_f_prev.loc[mask_sids]
    # df_fsdata_f_prev.reset_index(inplace=True,drop=True)

    #F score(fsd1=now,fsd0=previous year)-------------------------------------
    df_fsdata_f_e = pd.DataFrame()
    if df_fsdata_f_prev.shape[0] != df_fsdata_f.shape[0]:
        logging.info("Unequal data, adjusting...")
        df_fsdata_f_e=df_fsdata_f.loc[df_fsdata_f["sid"].isin(df_fsdata_f_prev["sid"])].reset_index(drop=True)
        for i in range(len(df_fsdata_f_e)):
            if df_fsdata_f_e.loc[i,"sid"] != df_fsdata_f_prev.loc[i,"sid"]:
                logging.critical(f"{i},錯誤，基本面資料不匹配")
        df_fsdata_f = df_fsdata_f_e

    def calculate_fscore(fsd0,fsd1):
        df_f_score = pd.DataFrame(columns=["sid","ym","score"])

        for i in range(len(fsd1)):
            score = 0
            if fsd1.loc[i,"roa"] > 0 : score+=1                                         #ROA>0
            if fsd1.loc[i,"roa"] > fsd0.loc[i,"roa"] : score+=1                         #ROA有成長
            if fsd1.loc[i,"ocf"] > 0 : score+=1                                         #營運現金流>0
            if fsd1.loc[i,"ocf"] > fsd1.loc[i,"ni_ps_s"] * fsd1.loc[i,"so"] : score+=1  #營運現金流>淨利(應收)
            if fsd1.loc[i,"ncl"] < fsd0.loc[i,"ncl"] : score+=1                         #長期負債減少
            if fsd1.loc[i,"cr"] > fsd0.loc[i,"cr"] : score+=1                           #流動比率增加
            if fsd0.loc[i,"dcapital"] == 0 : score+=1                                   #無增資
            if fsd1.loc[i,"gpr"] > fsd0.loc[i,"gpr"] : score+=1                         #毛利率增加
            if fsd1.loc[i,"at"] > fsd0.loc[i,"at"] : score+=1                           #資產週轉率增

            #分數擴充區
            if fsd1.loc[i,"br"] >= 100 : score+=1
            # if fsd1.loc[i,"roic"] > fsd0.loc[i,"roic"] : score+=1
            # if fsd1.loc[i,"it"] > fsd0.loc[i,"it"] : score+=1

            df_f_score.loc[i,"sid"] = fsd1.loc[i,"sid"]
            df_f_score.loc[i,"ym"] = fsd1.loc[i,"ym"]
            df_f_score.loc[i,"score"] = score
            df_f_score.loc[i,"br"] = fsd1.loc[i,"br"]

        return df_f_score
        
    df_fscore = calculate_fscore(df_fsdata_f_prev,df_fsdata_f)
    # df_final_pool = df_fscore[df_fscore["score"].isin([7,8,9])]
    df_final_pool = df_fscore[df_fscore["score"].isin([8,9,10])]
    df_final_pool = df_final_pool.sort_values("score",ascending=False).head(30)
    
    # df_final_pool = df_final_pool.sort_values("br",ascending=False).iloc[int(len(df_final_pool)/3)*2:] # 0:int(len(df_final_pool)/3)  int(len(df_final_pool)/3):int(len(df_final_pool)/3)*2  int(len(df_final_pool)/3)*2:
    df_final_pool.reset_index(inplace=True,drop=True)

    #-------------------------------------------------------------------------------------------------------------
    #Additional filter
    if False:
        list_mask_fp = []
        for j in range(len(df_fsdata_f)):
            flag1,flag2 = False,False
            if str(df_fsdata_f.loc[j,"sid"]) in df_final_pool.loc[:,"sid"].to_list() : flag1 = True

            # if df_fsdata_f.loc[j,"br"] > 100 : flag2 = True
            # if df_fsdata_f.loc[j,"ltca"] > 100 : flag2 = True
            # if df_fsdata_f.loc[j,"tie"] > df_fsdata_f_prev.loc[j,"tie"] : flag2 = True
            # if df_fsdata_f.loc[j,"roic"] > df_fsdata_f_prev.loc[j,"roic"] : flag2 = True
            if df_fsdata_f.loc[j,"ni_ps_s"] *  df_fsdata_f.loc[j,"so"] > df_fsdata_f_prev.loc[j,"ni_ps_s"] * df_fsdata_f_prev.loc[j,"so"] : flag2 = True

            list_mask_fp.append(flag1 and flag2)

        df_fsdata_f_masked = df_fsdata_f.loc[list_mask_fp]
        df_final_pool_fi = df_final_pool.loc[df_final_pool.loc[:,"sid"].isin(df_fsdata_f_masked.loc[:,"sid"])]
        df_final_pool_fi.reset_index(inplace=True,drop=True)
        df_final_pool = df_final_pool_fi
    #-------------------------------------------------------------------------------------------------------------

    return df_final_pool

def get_sep_pool(is_save = False,timeperiod=None,fsdata=pd.DataFrame(),pbdata=pd.DataFrame()):
    df_pbr = pbdata
    df_fsdata = fsdata
    pd.options.mode.chained_assignment = None

    ava_ym_list = sorted(list(set(df_pbr["ym"].astype("str"))))
    ava_ym_list_int = [int(ava_ym_list[i]) for i in range(len(ava_ym_list))]
    list_trade_dates = []
    # logging.debug(ava_ym_list_int)

    #訊號日期
    updating_10y("TWII")
    prod = "^TWII"
    dt_today = datetime.now().strftime("%Y-%m-%d")
    s_tradays = pd.read_csv(f"C:\\Users\\Andyfish\\.conda\\envs\\fs_env\\codes\\stock_file\\{prod}_10y_{dt_today}.csv",index_col=0,parse_dates=True).index
    s_tradays = pd.Series(s_tradays)

    for i,ym in enumerate(ava_ym_list):
        ym = datetime.strptime(ym,"%Y%m")

        #根據季度選交易時間
        if ym.month in [3,6,9]:
            ym = ym + timedelta(weeks=10) + timedelta(days=5)
        elif ym.month == 12:
            ym += timedelta(weeks=17)
        
        list_trade_dates.append(ym)
    for i,tds in enumerate(list_trade_dates):
        if not tds in s_tradays:
            list_trade_dates[i] = s_tradays[s_tradays > tds].iloc[0]

    logging.debug(list_trade_dates)

    #Sep pool
    from fscore_pool import get_pool
    df_sep_pool = pd.DataFrame(columns=["ymd","pool_list","b_s"])
    for i,ym in enumerate(ava_ym_list_int):
        logging.debug(f"ym: {ym}")
        df_sep_pool.loc[i,"ymd"] = list_trade_dates[i]
        df_sep_pool.loc[i,"pool_list"] = get_pool(ym,pbr_raw=df_pbr,fsdata=df_fsdata)["sid"].to_list()
        df_sep_pool.loc[i,"b_s"] = "b"

    #Big pool
    list_big_pool = []
    for i in range(len(df_sep_pool)):
        for j in df_sep_pool.loc[i,"pool_list"]:
            list_big_pool.append(j)
    list_big_pool = sorted(list(set(list_big_pool)))

    #移除資料不全者
    list_to_remove = ['1524', '2069', '2211', '2236', '2239', '2243', '2743', '3712', '3714', '4442', '4552', '4767', '4804', '4804', '4806', '4807', '4949', '5223', '5228', '5244', '6431', '6456', '6512', '6517', '6538', '6556', '6578', '6807', '8455', '8455', '8472', '8473', '8473', '8488', '8499','6616','8045',"9962"]
    for p in list_to_remove:
        #from big pool
        if p in list_big_pool : list_big_pool.remove(p)
        #from signal
        for i,l in enumerate(df_sep_pool.loc[:,"pool_list"]):
            for j in l:
                if j == p : df_sep_pool.loc[i,"pool_list"].remove(p)
    
    #年訊號
    if timeperiod == "y":
        logging.info("re-save to annual data")
        ilist = [0,4,8,12,16,20,24,28,32,36]
        df_sep_pool = df_sep_pool[df_sep_pool.index.isin(ilist)].reset_index(drop=True)

    #存到pickle暫存
    if is_save:
        df_sep_pool.to_pickle(r"C:\Users\Andyfish\.conda\envs\fs_env\codes\data\signal_pool.pickle")
        pd.DataFrame(list_big_pool).to_pickle(r"C:\Users\Andyfish\.conda\envs\fs_env\codes\data\bigpool.pickle")

    return df_sep_pool , list_big_pool

# get_sep_pool(is_save=True,timeperiod="y")
# get_pool(201412,)