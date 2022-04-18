FROM jupyter/pyspark-notebook


USER root

RUN pip install kaggle
# RUN pip install kaggle-cli


