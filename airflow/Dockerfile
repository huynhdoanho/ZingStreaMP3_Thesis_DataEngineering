FROM apache/airflow:2.4.1

#USER roots
#RUN mkdir /opt/airflow/mydbt
#COPY ../mydbt /opt/airflow/mydbt
#RUN sudo chmod -R 777 /opt/airflow/mydbt
USER airflow

# install packages
COPY ./requirements.txt /opt/airflow/requirements.txt
RUN pip install -U pip
RUN pip install -r /opt/airflow/requirements.txt

# Thêm lệnh chmod để cấp quyền cho thư mục logs
RUN mkdir -p /opt/airflow/logs && chmod -R 777 /opt/airflow/logs