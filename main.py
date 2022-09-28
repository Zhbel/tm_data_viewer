import dash_graph
from discreteness import analysis
from readers import tm_reader, well_reader, file_reader

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from converters import converters
from common import const, db_common, clickhouse, configuration, sql_utils
from common.contracts import DatesInterval



def main():
    dt_from = datetime(2022, 9, 19, 0)
    dt_to = datetime(2022, 9, 23, 0)

    tm_map = tm_reader.get_tm_param_map()
    tm_map = pd.DataFrame(tm_map)
    wells = well_reader.get_well_info(dt_from, dt_to)
    wells = pd.DataFrame(wells)
    cl_data = tm_reader.get_oren_data(dt_from, dt_to, [], wells.well_id.unique())
    cl_data = pd.DataFrame(cl_data)
    cl_data = cl_data.merge(wells, on='well_id')
    cl_data = cl_data.merge(tm_map, on='param_id')

    run_graph(cl_data)



def run_graph(data):
    dash_graph.main(data)

main()
