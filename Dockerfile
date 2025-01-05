FROM jupyter/all-spark-notebook:python-3.11.6

# Set working directory
WORKDIR /container

# Install lzop, GDAL, and other dependencies
USER root
RUN apt-get update && apt-get install -y \
    lzop gcc g++ git \
    libgdal-dev # GDAL libraries installation

# Set GDAL environment variable correctly
ENV GDAL_CONFIG=/usr/bin/gdal-config

# Clone DuckDB and install
RUN git clone --depth 1 --branch v0.9.0 https://github.com/duckdb/duckdb
ENV BUILD_PYTHON=1
ENV GEN=ninja
RUN cd duckdb/tools/pythonpkg && python setup.py install

# Install httpfs extension
RUN python3 -c "import duckdb; duckdb.query('INSTALL httpfs;');"

# Install pip packages as root
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir duckdb duckdb-engine python-lzo lxml dbt dbt-duckdb sodapy dlt[duckdb] paramiko python-dotenv pyarrow==11.0.0

# Install conda packages as root
RUN conda install -y -c conda-forge xgboost python-dotenv=0.21.1 xlwings findspark pyspark polars && \
    conda install pip boto3 && \
    conda update pandas -y && \
    conda clean --all -f -y

# Switch back to notebook user
USER $NB_UID