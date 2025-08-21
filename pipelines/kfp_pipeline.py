from kfp import dsl
from typing import NamedTuple

@dsl.component(base_image='quay.io/<org>/noaa-etl:latest')
def assemble_training(bucket: str, cycle: str, out_path: dsl.Output[dsl.Dataset]):
    import pandas as pd, fsspec, os
    host=os.getenv('BUCKET_HOST'); port=os.getenv('BUCKET_PORT'); tls=os.getenv('BUCKET_TLS','false').lower()== 'true'
    endpoint=os.getenv('S3_ENDPOINT') or (('https' if tls else 'http')+f"://{host}:{port}")
    fs=fsspec.filesystem('s3', key=os.getenv('AWS_ACCESS_KEY_ID'), secret=os.getenv('AWS_SECRET_ACCESS_KEY'), client_kwargs={'endpoint_url': endpoint})
    pref=f"s3://{bucket}/features/red_river_fgON8/feature_date="+cycle.replace('-','').replace(':','')[:10]+"/"
    files=fs.glob(pref+"*.parquet")
    df=pd.concat([pd.read_parquet(f, storage_options={'key':fs.key,'secret':fs.secret,'client_kwargs':{'endpoint_url':endpoint}}) for f in files])
    df.to_parquet(out_path.path)

@dsl.component(base_image='quay.io/<org>/noaa-etl:latest')
def train_and_export(in_data: dsl.Input[dsl.Dataset], model_uri: str) -> NamedTuple('Metrics', [('f1', float)]):
    import pandas as pd, numpy as np
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import f1_score
    from xgboost import XGBClassifier
    import joblib, os
    df=pd.read_parquet(in_data.path)
    y=df['label']
    X=df[['q_nwm_h','dq_dt_h']].fillna(0)
    X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.2,random_state=42)
    clf=XGBClassifier(n_estimators=100,max_depth=3,learning_rate=0.1,subsample=0.9,colsample_bytree=0.9,tree_method='hist')
    clf.fit(X_train,y_train)
    yhat=(clf.predict_proba(X_test)[:,2]>0.5).astype(int)
    f1=f1_score((y_test==2).astype(int), yhat)
    os.makedirs('/tmp/model', exist_ok=True)
    joblib.dump(clf, '/tmp/model/model.joblib')
    import fsspec
    key=os.getenv('AWS_ACCESS_KEY_ID'); sec=os.getenv('AWS_SECRET_ACCESS_KEY'); endpoint=os.getenv('S3_ENDPOINT')
    fs=fsspec.filesystem('s3', key=key, secret=sec, client_kwargs={'endpoint_url': endpoint})
    fs.put('/tmp/model/model.joblib', model_uri.rstrip('/')+'/model.joblib')
    from typing import NamedTuple
    return (float(f1),)

@dsl.pipeline(name='streamflow-risk-train')
def pipe(bucket: str='epic-demo-odf', cycle: str='2025082112', model_uri: str='s3://epic-demo-odf/models/streamflow_risk/latest/'):
    data = assemble_training(bucket=bucket, cycle=cycle)
    train_and_export(in_data=data.outputs['out_path'], model_uri=model_uri)
