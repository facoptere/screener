from scipy.interpolate import interp1d
import traceback
import polars as pl
import numpy as np

def var2rank(X, Y, x):
    r = 1  # default return if x is nan
    try:
        if x == x:
            y_interp = interp1d(x=X, y=Y, fill_value=(Y[0], Y[-1]), bounds_error=False)
            r = float(y_interp(x))
    except Exception as e:
        print(e)
        print(repr(e))
        traceback.print_exc()
    return r


def var2quant2(x, Q, name):
    newx = var2rank(Q[name][0], Q[name][1], x[name]) ** Q[name][2]
    # print(f"Q:{Q[name]},name:{name},x:{x[name]} => {newx}")
    return newx


def compute_rank(df: pl.DataFrame, colname, ranking) -> pl.DataFrame:
    df = df.with_columns(pl.lit(1**-10).alias(colname))
    
    for key in ranking.keys():
        r = ranking[key]
        X = r[0]
        Y = r[1]
        power = r[2]
        
        # Use map_batches to apply interpolation on entire column
        def interpolate_col(s):
            result = []
            for val in s:
                try:
                    if val is not None and not np.isnan(val):
                        y_interp = interp1d(x=X, y=Y, fill_value=(Y[0], Y[-1]), bounds_error=False)
                        result.append(float(y_interp(val)) ** power)
                    else:
                        result.append(1.0)
                except:
                    result.append(1.0)
            return pl.Series(result)
        
        df = df.with_columns(
            (pl.col(colname) * pl.col(key).map_batches(interpolate_col)).alias(colname)
        )
    return df


ranking = {
    "VE/EBITDA": [[0, 0.01, 6, 9, 15, 10**10], [1, 4, 4, 2, 1, 1], 1],
    "VE/CA": [[0, 0.01, 0.6, 1, 2, 10**10], [1, 4, 4, 2, 1, 1], 1],
    "CAPI/TANG": [[0, 0.01, 1, 1.5, 2.5, 10**10], [1, 4, 4, 2, 1, 1], 1],
    "PER": [[0, 1, 10, 15, 20, 10**10], [1, 4, 4, 2, 1, 1], 1],
    "Rendement": [[0, 2, 3, 6,10**10], [1, 1, 2, 4, 4],1],
    "Dette nette / EBITDA": [[0, 0.01, 2, 3, 4, 10**10], [1, 4, 4, 2, 1, 1], 1],
    "Ratio courant": [[1, 1.5, 2, 10**10], [1, 2, 4, 4], 1],
    "VE/FCF": [[0, 0.01, 10, 15, 20, 10**10], [1, 4, 4, 2, 1, 1], 1],
}

'''
ranking2 = {
    "REVPS5YGR": [[-10, -1, 3, 20], [10**-18, 0.1, 1, 100], 1],
    "MARGIN5YR": [[-10, -1, 3, 20], [10**-18, 0.1, 1, 100], 1],
    "Focf2Rev_AAvg5": [[-10, -1, 3, 20], [10**-18, 0.1, 1, 100], 1],
    "score": [[-1, 25, 4**4, 4**8], [10**-18, 0.1, 50, 100], 4],
    "YLD+PRY": [[-1, 4, 9, 22, 100], [10**-18, 0.1, 1, 100, 100], 1],
    "En Solde": [[-0.2, 0, 10, 20, 100], [10**-18, 1, 20, 50, 100], 2],
}
'''