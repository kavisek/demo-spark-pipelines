FROM jupyter/pyspark-notebook


USER root

RUN pip install kaggle
RUN pip install pytest
RUN pip install pytest-cov
RUN pip install pytest-xdist
RUN pip install black
RUN pip install black[jupyter]
