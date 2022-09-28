from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from common import db_common, clickhouse

def get_well_info(dt_from: datetime, dt_to: datetime) -> List[dict]:
    params = {"dt_from_int": dt_from, "dt_to_int": dt_to}
    query = f"""
    with w_fond as
    (
        select *
        from (
            select
                f.sk_1,
                f.dz_1,
                f.xr_1,
                row_number() over(partition by f.sk_1 order by f.dz_1 desc, f.tz_1 desc) as rn
            from dwh.fond f
            where f.begin_date <= :dt_to_int
                and f.end_date > :dt_from_int
                and f.xr_1 = 'XR0011'
        ) v1
        where v1.rn = 1
    ),
    w_sost as
    (
        select *
        from (
            select
                s.sk_1,
                s.dz_1,
                s.ss_1,
                row_number() over(partition by s.sk_1 order by s.dz_1 desc, s.tz_1 desc) as rn
            from dwh.sost s
            where s.begin_date <= :dt_to_int
                and s.end_date > :dt_from_int
                and s.ss_1 in ('SS0001', 'SS0010', 'SS0002')
        ) v1
        where v1.rn = 1
    ),
    w_spons as
    (
        select *
        from (
            select
                sp.sk_1,
                sp.dz_1,
                sp.se_1,
                row_number() over(partition by sp.sk_1 order by sp.dz_1 desc, sp.tz_1 desc) as rn
            from dwh.spons sp
            where sp.begin_date <= :dt_to_int
                and sp.end_date > :dt_from_int
        ) v1
        where v1.rn = 1
    )
    select
        w.well_id,
        w.well_name,
        w.field_name,
        wrm.name
    from v_well_full w
    inner join w_fond f on f.sk_1 = w.well_id
    inner join w_sost s on s.sk_1 = w.well_id
    inner join w_spons sp on sp.sk_1 = w.well_id
    left join era_neuro.esp_well_regime ewr on ewr.well_id = w.well_id
        and ewr.dt_from = :dt_from_int and ewr.dt_to = :dt_to_int
    left join dwh.well_regime_mode wrm on ewr.well_regime = wrm.id

    -- where w.field_id in ('MS0613', 'MS0256', 'MS0611', 'MS0254', 'MS0563', 'MS0286', 'MS0313')
    -- where w.field_id in ('MS0238', 'MS0999')
    -- where w.field_id in ('MS0999')
    """
    query = db_common.format_to_pg_sql(query)
    qwe = db_common.get_data(query, params)
    return qwe