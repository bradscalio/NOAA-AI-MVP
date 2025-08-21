# Base on an OpenShift AI-compatible image with conda/mamba
FROM quay.io/modh/odh-generic-data-science-notebook:py3.11-2024a
USER root
RUN microdnf -y install git && microdnf clean all
USER 1001
# Use conda-forge to get eccodes/cfgrib/netcdf4 reliably
RUN mamba install -y -n base -c conda-forge \
    xarray cfgrib eccodes zarr s3fs fsspec pandas numpy scikit-learn xgboost shap dask boto3 pyarrow netcdf4 && \
    mamba clean -afy
WORKDIR /app
COPY etl/ /app/
ENTRYPOINT ["bash","-lc"]
