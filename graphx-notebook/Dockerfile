#FROM andypetrella/spark-notebook:0.9.0-SNAPSHOT-scala-2.11.8-spark-2.2.0-hadoop-2.7.2
FROM jkremser/spark-notebook:0.9.0-SNAPSHOT-scala-2.11.8-spark-2.1.1-hadoop-2.7.3

ADD blockchain.snb.ipynb /opt/docker/notebooks/
ADD nodes /tmp/nodes.parquet
ADD edges /tmp/edges.parquet
ADD entrypoint.sh /entrypoint.sh


LABEL io.k8s.description="spark-notebook Notebook with blockchain analysis." \
      io.k8s.display-name="spark-notebook Notebook with blockchain analysis." \
      io.openshift.expose-services="9000:http"

RUN chmod ug+rw /etc/passwd \
    && find /opt/docker/notebooks/ ! -path /opt/docker/notebooks/ -type d -exec rm -rf {} +

USER daemon

EXPOSE 9000
ENTRYPOINT ["/entrypoint.sh", "/opt/docker/bin/spark-notebook", "-Dpidfile.path=/tmp/pid"]
