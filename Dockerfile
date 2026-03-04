FROM jupyter/pyspark-notebook:latest
USER root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
USER jovyan
