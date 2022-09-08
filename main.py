"""

    """

import asyncio

import pandas as pd
from githubdata import GithubData
from mirutil.async_requests import get_reps_texts_async
from mirutil.df_utils import save_as_prq_wo_index as sprq
from mirutil.utils import ret_clusters_indices


class GDUrl :
    cur = 'https://github.com/imahdimir/u-d-TSETMC_ID-Shenase'
    trg = 'https://github.com/imahdimir/d-TSETMC_ID-Shenase'
    src = 'https://github.com/imahdimir/d-TSETMC_ID-2-FirmTicker'

gu = GDUrl()

class Constant :
    burl = 'http://tsetmc.com/Loader.aspx?Partree=15131M&i='

cte = Constant()

class ColName :
    url = 'url'
    tid = 'TSETMC_ID'
    res = 'res'
    ftic = 'FirmTicker'
    obsd = 'ObsDate'

c = ColName()

fu0 = get_reps_texts_async

def get_df_4_each_ro(res) :
    dfs = pd.read_html(res)
    assert len(dfs) == 1
    return dfs[0]

def main() :
    pass

    ##
    # 1. Get the list of all the firms
    gd_src = GithubData(gu.src)
    gd_src.overwriting_clone()

    ##
    ds = gd_src.read_data()
    ds = ds.astype(str)
    ##
    ds[c.url] = cte.burl + ds[c.tid]

    ##
    ds[c.res] = None
    ##
    df1 = ds.copy()
    ##
    while not df1.empty :
        msk = ds[c.res].isna()
        df1 = ds[msk]
        print(len(df1))

        clus = ret_clusters_indices(df1)
        for se in clus :
            si , ei = se
            print(se)

            inds = df1.iloc[si :ei].index

            urls = df1.loc[inds , c.url]

            ou = asyncio.run(fu0(urls))

            ds.loc[inds , c.res] = ou

            # break

        # break

    ##
    da = pd.DataFrame()

    for _ , ro in ds.iterrows() :
        res = ro[c.res]
        df = get_df_4_each_ro(res)

        df[c.tid] = ro[c.tid]
        df[c.ftic] = ro[c.ftic]

        da = pd.concat([da , df])

    ##
    db = da.pivot(index = [c.tid , c.ftic] , columns = 0 , values = 1)
    ##
    db.reset_index(inplace = True)
    ##
    db[c.obsd] = pd.to_datetime('today').date()
    db[c.obsd] = db[c.obsd].astype(str)

    ##

    gd_trg = GithubData(gu.trg)
    gd_trg.overwriting_clone()
    ##
    dft = gd_trg.read_data()
    ##
    dft = pd.concat([dft , db])
    ##
    dft = dft.drop_duplicates()
    ##

    dftp = gd_trg.local_path / 'data.prq'
    sprq(dft , dftp)
    ##

    msg = 'data updated by: '
    msg += gu.cur
    ##

    gd_trg.commit_and_push(msg)

    ##

    gd_trg.rmdir()
    gd_src.rmdir()

    ##

##
if __name__ == "__main__" :
    main()

##
# noinspection PyUnreachableCode
if False :
    pass

    ##


    ##


    ##

##
