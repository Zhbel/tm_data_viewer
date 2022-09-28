import pandas as pd
import glob

def get_data(path):
    ## читаем рандомный эксель по пути
    data = pd.read_excel(fr"C:/Users/Dmitriev.VAleksa/Documents/Python/tm_data/do_data/{path}")
    return data

def get_multiply_data(path):
    ## читаем всю папку экселей
    files = glob.glob(fr"C:/Users/Dmitriev.VAleksa/Documents/Python/tm_data/do_data/{path}/*.xls")
    data = pd.DataFrame()
    for f in files:
        tmp = pd.read_excel(f)
        data = data.append(tmp)
    return data