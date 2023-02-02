"""

    """

from pathlib import Path

import pandas as pd
from mirutil.utils import ret_clusters_indices

from githubdata import GitHubDataRepo
from mirutil.ns import rm_ns_module
from mirutil.ns import update_ns_module
from mirutil.async_req import get_resps_async_sync
from mirutil.df import save_as_prq_wo_index

update_ns_module()
import ns

gdu = ns.GDU()
c = ns.Col()

class Constant :
    burl = 'http://tsetmc.com/Loader.aspx?Partree=15131M&i='

cte = Constant()

class ColName :
    url = 'url'
    res = 'res'

cn = ColName()

class FilePath :
    t0 = Path('temp0.prq')

fp = FilePath()

def get_df_4_each_ro(res) :
    dfs = pd.read_html(res)
    assert len(dfs) == 1
    return dfs[0]

def main() :
    pass

    ##

    # 1. Get the list of all the firms

    gd_src = GitHubDataRepo(gdu.src)
    gd_src.clone_overwrite()

    ##
    df = gd_src.read_data()
    df = df.astype(str)

    ##
    df[cn.url] = cte.burl + df[c.tse_id]

    ##
    df[cn.res] = None

    ##
    df1 = df.copy()

    ##
    while not df1.empty :
        msk = df[cn.res].isna()
        df1 = df[msk]
        print(len(df1))

        clus = ret_clusters_indices(df1)
        for se in clus :
            si , ei = se
            print(se)

            inds = df1.iloc[si : ei].index

            urls = df1.loc[inds , cn.url]

            ou = get_resps_async_sync(urls)

            df.loc[inds , cn.res] = [x.cont for x in ou]

            # break

        # break

    ##
    df.to_parquet(fp.t0 , index = False)

    ##
    df = pd.read_parquet(fp.t0)

    ##
    df[cn.res] = df[cn.res].apply(lambda x : x.decode('utf-8'))

    ##
    dfa = pd.DataFrame()

    for _ , ro in df.iterrows() :
        res = ro[cn.res]
        _df = get_df_4_each_ro(res)

        _df[c.tse_id] = ro[c.tse_id]
        _df[c.tic] = ro[c.tic]

        dfa = pd.concat([dfa , _df])

    ##
    dfb = dfa.pivot(index = [c.tse_id , c.tic] , columns = 0 , values = 1)

    ##
    dfb = dfb.reset_index()

    ##
    for col in dfb.columns :
        if dfb[col].isna().all() :
            dfb = dfb.drop(columns = [col])
            print(col)

    ##
    dfb = dfb.astype('string')

    ##
    dfb[c.obsd] = pd.to_datetime('today').date()
    dfb[c.obsd] = dfb[c.obsd].astype(str)

    ##
    gd_trg = GitHubDataRepo(gdu.trg)
    gd_trg.clone_overwrite()

    ##
    dft = gd_trg.read_data()

    ##
    dft = pd.concat([dft , dfb])

    ##
    dft = dft.drop_duplicates()

    ##
    dft_fp = gd_trg.local_path / 'data.prq'
    save_as_prq_wo_index(dft , dft_fp)

    ##
    msg = 'data updated by: '
    msg += gdu.slf

    ##
    gd_trg.commit_and_push(msg)

    ##
    gd_trg.rmdir()
    gd_src.rmdir()
    rm_ns_module()
    fp.t0.unlink()

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
