FROM quay.io/astronomer/ap-airflow:2.1.1-buster-onbuild
ENV AIRFLOW__CORE__LAZY_LOAD_PLUGINS=False
