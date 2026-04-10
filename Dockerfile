FROM jupyter/all-spark-notebook:python-3.11.6

# Set working directory
WORKDIR /container

# Switch to root to install system packages
USER root

# Install system dependencies including Node.js
RUN apt-get update && apt-get install -y \
    curl \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python packages
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir \
        duckdb \
        duckdb-engine \
        python-lzo \
        lxml \
        dbt \
        dbt-duckdb \
        sodapy \
        "dlt[duckdb,workspace]" \
        paramiko \
        python-dotenv \
        streamlit \
        plotly

# Install conda packages
RUN conda install -y -c conda-forge \
        xgboost \
        python-dotenv=0.21.1 \
        xlwings \
        findspark \
        pyspark \
        polars && \
    conda install -y pip boto3 && \
    conda update -y pandas && \
    conda clean --all -f -y

# Switch back to notebook user
USER $NB_UID

# Install uv (provides uvx command)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH
ENV PATH="/home/${NB_USER}/.cargo/bin:${PATH}"

# Install Claude Code CLI as the notebook user
RUN npm install -g @anthropic-ai/claude-code

# Add npm global bin to PATH
ENV PATH="/home/${NB_USER}/.npm-global/bin:${PATH}"
