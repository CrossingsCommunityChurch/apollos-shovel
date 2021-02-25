FROM quay.io/astronomer/ap-airflow:2.0.0-buster-onbuild
ENV AIRFLOW__CORE__LAZY_LOAD_PLUGINS=False
