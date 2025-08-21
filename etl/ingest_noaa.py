import argparse, os
import datetime as dt
import boto3
from botocore.config import Config
from botocore import UNSIGNED

PUB = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')

def s3_client_to_odf():
    endpoint = os.getenv('S3_ENDPOINT')
    if not endpoint:
        host = os.getenv('BUCKET_HOST')
        port = os.getenv('BUCKET_PORT')
        scheme = 'https' if os.getenv('BUCKET_TLS','false').lower() == 'true' else 'http'
        endpoint = f"{scheme}://{host}:{port}"
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        endpoint_url=endpoint,
        region_name=os.getenv('AWS_DEFAULT_REGION','us-east-1')
    )

def copy_obj(src_bucket, src_key, dst_bucket, dst_key, odf):
    src = PUB.get_object(Bucket=src_bucket, Key=src_key)
    odf.upload_fileobj(src['Body'], dst_bucket, dst_key)

def hrrr_keys(cycle):
    ymd = cycle.strftime('%Y%m%d')
    hh = cycle.strftime('%H')
    base = f"hrrr.{ymd}/conus/"
    keys = [f"{base}hrrr.t{hh}z.wrfsfcf{h:02}.grib2" for h in range(1,7)]
    return 'noaa-hrrr-bdp-pds', keys

def nwm_keys(cycle):
    ymd = cycle.strftime('%Y%m%d')
    hh = cycle.strftime('%H')
    base = f"nwm.{ymd}/short_range/"
    keys = [f"{base}nwm.t{hh}z.short_range.channel_rt.conus.f{h:03}.nc" for h in range(1,7)]
    return 'noaa-nwm-pds', keys

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--cycle', required=True, help='ISO time, e.g., 2025-08-21T12:00:00Z')
    ap.add_argument('--odf-bucket', dest='odf_bucket', required=True)
    args = ap.parse_args()

    cycle = dt.datetime.fromisoformat(args.cycle.replace('Z','+00:00'))
    odf = s3_client_to_odf()

    for (bucket, keys, label) in [(*hrrr_keys(cycle), 'hrrr'), (*nwm_keys(cycle), 'nwm')]:
        for k in keys:
            try:
                PUB.head_object(Bucket=bucket, Key=k)
            except Exception:
                print(f"skip missing {bucket}/{k}")
                continue
            dst_key = f"raw/{label}/{cycle:%Y/%m/%d/%H}/" + k.split('/')[-1]
            print(f"copy {bucket}/{k} -> {args.odf_bucket}/{dst_key}")
            copy_obj(bucket, k, args.odf_bucket, dst_key, odf)
    print('done')
