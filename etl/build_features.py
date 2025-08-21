import argparse, os, json
import datetime as dt
import pandas as pd
import xarray as xr
import fsspec

def odf_fs():
    endpoint = os.getenv('S3_ENDPOINT')
    if not endpoint:
        host = os.getenv('BUCKET_HOST'); port = os.getenv('BUCKET_PORT')
        scheme = 'https' if os.getenv('BUCKET_TLS','false').lower() == 'true' else 'http'
        endpoint = f"{scheme}://{host}:{port}"
    key = os.getenv('AWS_ACCESS_KEY_ID'); sec = os.getenv('AWS_SECRET_ACCESS_KEY')
    return fsspec.filesystem('s3', key=key, secret=sec, client_kwargs={'endpoint_url': endpoint})


def load_nwm_series(fs, bucket, cycle):
    rows = []
    for h in range(1,7):
        key = f"raw/nwm/{cycle:%Y/%m/%d/%H}/nwm.t{cycle:%H}z.short_range.channel_rt.conus.f{h:03}.nc"
        url = f"s3://{bucket}/{key}"
        if not fs.exists(url):
            continue
        ds = xr.open_dataset(url, engine='netcdf4', backend_kwargs={'autoclose': True},
                             storage_options={'client_kwargs': {'endpoint_url': fs.client_kwargs['endpoint_url']},
                                              'key': fs.key, 'secret': fs.secret})
        varnames = [v for v in ds.data_vars]
        v = 'streamflow' if 'streamflow' in varnames else varnames[0]
        q = float(ds[v].mean().values)
        rows.append({'lead_h': h, 'q_nwm_h': q})
        ds.close()
    return pd.DataFrame(rows)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--cycle', required=True)
    ap.add_argument('--bucket', required=True)
    ap.add_argument('--gauge-id', default='FGON8')
    ap.add_argument('--flow-high-thresh-cms', type=float, default=1500.0)
    args = ap.parse_args()

    cycle = dt.datetime.fromisoformat(args.cycle.replace('Z','+00:00'))
    fs = odf_fs()

    df = load_nwm_series(fs, args.bucket, cycle)
    for h in range(1,7):
        if h not in df['lead_h'].values:
            df = pd.concat([df, pd.DataFrame([{'lead_h': h, 'q_nwm_h': None}])])
    df = df.sort_values('lead_h')
    df['dq_dt_h'] = df['q_nwm_h'].diff().fillna(0)
    q3 = df.loc[df['lead_h']==3, 'q_nwm_h'].values[0] if (df['lead_h']==3).any() else 0
    q6 = df.loc[df['lead_h']==6, 'q_nwm_h'].values[0] if (df['lead_h']==6).any() else 0
    if q3 and q3 >= args.flow_high_thresh_cms:
        label = 2
    elif q6 and q6 >= 0.7*args.flow_high_thresh_cms:
        label = 1
    else:
        label = 0
    valid_time = cycle + dt.timedelta(hours=1)
    records = []
    for _,r in df.iterrows():
        records.append({
            'gauge_id': args.gauge_id,
            'reach_id': -1,
            'valid_time': valid_time.isoformat(),
            'lead_h': int(r['lead_h']),
            'nwm_cycle': cycle.isoformat(),
            'q_nwm_h': None if pd.isna(r['q_nwm_h']) else float(r['q_nwm_h']),
            'q_pct_h': None,
            'apcp_0_1h': None,
            'apcp_1_3h': None,
            'apcp_3_6h': None,
            'dq_dt_h': float(r['dq_dt_h']) if r['dq_dt_h'] is not None else 0.0,
            'label': label,
            'split': 'train'
        })
    out_prefix = f"s3://{args.bucket}/features/red_river_fgON8/feature_date={cycle:%Y%m%d%H}/"
    out_path = out_prefix + 'part-000.parquet'
    pd.DataFrame.from_records(records).to_parquet(out_path, index=False, storage_options={'key': fs.key, 'secret': fs.secret, 'client_kwargs': {'endpoint_url': fs.client_kwargs['endpoint_url']}})
    print(json.dumps({'out': out_path}))
