FROM andypetrella/spark-notebook:0.9.0-SNAPSHOT-scala-2.11.8-spark-2.2.0-hadoop-2.7.2
#FROM jkremser/spark-notebook:0.9.0-SNAPSHOT-scala-2.11.8-spark-2.1.1-hadoop-2.7.3

ADD blockchain.snb.ipynb /opt/docker/notebooks/
ADD addresses /tmp/addresses.parquet
ADD blocks /tmp/blocks.parquet
ADD transactions /tmp/transactions.parquet
ADD edges /tmp/edges.parquet
ADD entrypoint.sh /entrypoint.sh


LABEL io.k8s.description="spark-notebook Notebook with blockchain analysis." \
      io.k8s.display-name="spark-notebook Notebook with blockchain analysis." \
      io.openshift.expose-services="9000:http"

# /usr/sbin is needed because akka creates this /usr/sbin/.coursier/cache/v1/.structure.lock
RUN chmod ug+rw /etc/passwd \
    && chmod o+w /usr/sbin/ \
    && chown daemon:daemon /opt/docker/notebooks/blockchain.snb.ipynb \
    && find /opt/docker/notebooks/ ! -path /opt/docker/notebooks/ -type d -exec rm -rf {} + \
    && apt-get update \
    && apt-get install -y python \
    && apt-get clean all

USER daemon

EXPOSE 9000 8000
ENTRYPOINT ["/entrypoint.sh", "/opt/docker/bin/spark-notebook", "-Dpidfile.path=/tmp/pid"]
