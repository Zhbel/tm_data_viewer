from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from common import db_common, clickhouse

def get_clause_in_param(param_ids: list) -> str:
    ## фильтр по параметрам
    clause_in = ""
    if param_ids is not None and len(param_ids) > 0:
        params_as_str = ", ".join([str(x) for x in param_ids])
        clause_in = f" and ParameterId in ({params_as_str})"
    return clause_in

def get_clause_in_well(param_ids: list) -> str:
    ## фильтр по скважинам
    clause_in = ""
    if param_ids is not None and len(param_ids) > 0:
        params_as_str = ", ".join([str(x) for x in param_ids])
        clause_in = f" and WellId in ({params_as_str})"
    return clause_in


def get_yamal_data(dt_from: datetime, dt_to: datetime, param_ids: List[int], well_ids: List[int]) -> List[dict]:
    ## сырой Ямал
    params = {"dt_from": dt_from, "dt_to": dt_to}
    param_in = get_clause_in_param(param_ids)
    well_in = get_clause_in_well(well_ids)
    query = f"""
            select
                WellId as well_id,
                ParameterId as param_id,
                Timestamp as dt,
                Value as val
            from dwh_yamal.parameters_orion_yaml
            where Timestamp >= :dt_from
                and Timestamp <= :dt_to {param_in} {well_in}
            order by Timestamp 
        """

    query = db_common.format_to_pg_sql(query)
    qwe = clickhouse.get_data(query, params)
    return qwe

def get_shelf_data(dt_from: datetime, dt_to: datetime, param_ids: List[int], well_ids: List[int]) -> List[dict]:
    ## сырой Шельф
    params = {"dt_from": dt_from, "dt_to": dt_to}
    param_in = get_clause_in_param(param_ids)
    well_in = get_clause_in_well(well_ids)
    query = f"""
            select
                WellId as well_id,
                ParameterId as param_id,
                Timestamp as dt,
                Value as val
            from dwh_shelf.parameters_shelf
            where Timestamp >= :dt_from
                and Timestamp <= :dt_to {param_in} {well_in}
            order by Timestamp 
        """

    query = db_common.format_to_pg_sql(query)
    qwe = clickhouse.get_data(query, params)
    return qwe

def get_zapolar_data(dt_from: datetime, dt_to: datetime, param_ids: List[int], well_ids: List[int]) -> List[dict]:
    ## сырая Заполярка
    params = {"dt_from": dt_from, "dt_to": dt_to}
    clause_in = get_clause_in_param(param_ids)
    well_in = get_clause_in_well(well_ids)
    query = f"""
            select
                WellId as well_id,
                ParameterId as param_id,
                Timestamp as dt,
                Value as val
            from dwh_zap.parameters_adku_zplr
            where Timestamp >= :dt_from
                and Timestamp <= :dt_to {clause_in} {well_in}
            order by Timestamp 
        """

    query = db_common.format_to_pg_sql(query)
    qwe = clickhouse.get_data(query, params)
    return qwe

def get_msh_data(dt_from: datetime, dt_to: datetime, param_ids: List[int],  well_ids: List[int]) -> List[dict]:
    ## сырая Мессояха
    params = {"dt_from": dt_from, "dt_to": dt_to}
    clause_in = get_clause_in_param(param_ids)
    well_clause_in = get_clause_in_well(well_ids)
    query = f"""
            select
                WellId as well_id,
                ParameterId as param_id,
                Timestamp as dt,
                Value as val
            from dwh_msh.parameters_tm_mesh
            where Timestamp >= :dt_from
                and Timestamp <= :dt_to {clause_in}
                {well_clause_in}
            order by Timestamp 
        """

    query = db_common.format_to_pg_sql(query)
    qwe = clickhouse.get_data(query, params)
    return qwe

def get_oren_data(dt_from: datetime, dt_to: datetime, param_ids: List[int], well_ids: List[int]) -> List[dict]:
    ## сырой орен
    params = {"dt_from": dt_from, "dt_to": dt_to}
    clause_in = get_clause_in_param(param_ids)
    well_clause_in = get_clause_in_well(well_ids)
    query = f"""
            select
                WellId as well_id,
                ParameterId as param_id,
                Timestamp as dt,
                Value as val
            from dwh_orb.parameters_adku_oren
            where Timestamp >= :dt_from
                and Timestamp <= :dt_to
                    {clause_in}
                    {well_clause_in}
            order by Timestamp 
        """

    query = db_common.format_to_pg_sql(query)
    qwe = clickhouse.get_data(query, params)
    return qwe


def get_mrng_data(dt_from: datetime, dt_to: datetime, param_ids: List[int], well_ids: List[int]) -> List[dict]:
    ## сырая Меретояха
    params = {"dt_from": dt_from, "dt_to": dt_to}
    clause_in = get_clause_in_param(param_ids)
    well_clause_in = get_clause_in_well(well_ids)
    query = f"""
            select
                WellId as well_id,
                ParameterId as param_id,
                Timestamp as dt,
                Value as val
            from dwh_mrng.parameters_opcua_mrng
            where Timestamp >= :dt_from
                and Timestamp <= :dt_to
                    {clause_in}
                    {well_clause_in}
            order by Timestamp 
        """

    query = db_common.format_to_pg_sql(query)
    qwe = clickhouse.get_data(query, params)
    return qwe


def get_tm_param_map() -> List[dict]:
    ## Маппинг параметров
    params = {}
    query = f"""
            select
                distinct param_id, descriptions
            from dwh.tm_param_map
            order by param_id
        """
    query = db_common.format_to_pg_sql(query)
    qwe = clickhouse.get_data(query, params)
    return qwe


