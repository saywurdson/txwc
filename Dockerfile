FROM jupyter/all-spark-notebook:python-3.11.6

# Set working directory
WORKDIR /container

# Switch to root to install packages
USER root

# Upgrade pip and install DuckDB and your pip packages
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir \
        duckdb \
        duckdb-engine \
        python-lzo \
        lxml \
        dbt \
        dbt-duckdb \
        sodapy \
        "dlt[duckdb]" \
        paramiko \
        python-dotenv \
        pyarrow==11.0.0 \
        "ibis-framework[duckdb]"

# If you need conda packages, install them as well
RUN conda install -y -c conda-forge \
        xgboost \
        python-dotenv=0.21.1 \
        xlwings \
        findspark \
        pyspark \
        polars && \
    conda install pip boto3 && \
    conda update pandas -y && \
    conda clean --all -f -y

# Switch back to the notebook user
USER $NB_UID