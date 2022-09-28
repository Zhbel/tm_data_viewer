import pandas as pd

def convert_yamal_ids(data_y):
    ## конвертим ямал в человеческие айди
    data_y['param_id'] = data_y['param_id'].replace(18111, 18)
    data_y['param_id'] = data_y['param_id'].replace(19111, 19)
    data_y['param_id'] = data_y['param_id'].replace(20111, 20)
    data_y['param_id'] = data_y['param_id'].replace(21111, 21)
    data_y['param_id'] = data_y['param_id'].replace(25111, 25)
    data_y['param_id'] = data_y['param_id'].replace(57111, 57)
    data_y['param_id'] = data_y['param_id'].replace(58111, 58)
    data_y['param_id'] = data_y['param_id'].replace(58222, 58)
    data_y['param_id'] = data_y['param_id'].replace(59111, 59)
    data_y['param_id'] = data_y['param_id'].replace(59222, 59)
    data_y['param_id'] = data_y['param_id'].replace(67222, 67)
    return data_y