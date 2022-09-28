from datetime import timedelta

import pandas as pd

def compare_mrng(tm_df, do_df):
    ## генератор отчета по меретояхе
    export_result = []

    for well in sorted(do_df.wellid.unique()):
        w_do = do_df.loc[do_df.wellid == well]
        w_tm = tm_df.loc[tm_df.well_id == well]

        dt_from = w_tm.dt.min()
        dt_to = w_tm.dt.max()

        while dt_from <= dt_to:
            dt_to_tmp = dt_from + timedelta(days=1)
            t_tm = w_tm.loc[(w_tm.dt >= dt_from) & (w_tm.dt <= dt_to_tmp)]
            t_do = w_do.loc[(w_do.timestamp >= dt_from) & (w_do.timestamp <= dt_to_tmp)]
            minutes_diff = (dt_to_tmp - dt_from).total_seconds() / 60.0

            for param in sorted(t_do.parameterid.unique()):
                p_do = t_do.loc[w_do.parameterid == param]
                p_tm = t_tm.loc[w_tm.param_id == param]

                if(p_do.value.count() != 0):
                    discreteness_do = minutes_diff/p_do.value.count()
                else: discreteness_do = 0

                if(p_tm.val.count() != 0):
                    discreteness_tm = minutes_diff/p_tm.val.count()
                else: discreteness_tm = 0

                res = {
                    'well': well,
                    'param': param,
                    'dt_from': dt_from,
                    'dt_to': dt_to_tmp,
                    'median_click': p_tm.val.median(),
                    'median_do': p_do.value.median(),
                    'discreteness_click': discreteness_tm,
                    'discreteness_do': discreteness_do,
                    'count_click': p_tm.val.count(),
                    'count_do': p_do.value.count()
                }
                export_result.append(res)
            dt_from = dt_to_tmp
    export_result = pd.DataFrame(export_result)
    export_result.to_excel('py_res/mrng_report.xlsx')

def compare_zapolar(tm_df, do_df):
## генератор отчета по заполярке
    export_result = []

    for well in sorted(do_df.wellid.unique()):
        w_do = do_df.loc[do_df.wellid == well]
        w_tm = tm_df.loc[tm_df.well_id == well]

        for param in sorted(w_do.parameterid.unique()):
            p_do = w_do.loc[w_do.parameterid == param]
            p_tm = w_tm.loc[w_tm.param_id == param]
            dt_from = p_tm.dt.min()
            dt_to = p_tm.dt.max()
            minutes_diff = (dt_to - dt_from).total_seconds() / 60.0

            if(p_do.value.count() != 0):
                discreteness_do = minutes_diff/p_do.value.count()
            else: discreteness_do = 0

            if(p_tm.val.count() != 0):
                discreteness_tm = minutes_diff/p_tm.val.count()
            else: discreteness_tm = 0

            res = {
                'well': well,
                'param': param,
                'median_click': p_tm.val.median(),
                'median_do': p_do.value.median(),
                'discreteness_click': discreteness_tm,
                'discreteness_do': discreteness_do,
                'count_click': p_tm.val.count(),
                'count_do': p_do.value.count()
            }
            export_result.append(res)
    export_result = pd.DataFrame(export_result)
    export_result.to_excel('py_res/zap_report.xlsx')


