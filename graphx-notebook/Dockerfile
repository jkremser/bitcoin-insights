FROM andypetrella/spark-notebook:0.9.0-SNAPSHOT-scala-2.11.8-spark-2.2.0-hadoop-2.7.2

ADD blockchain.snb.ipynb /opt/docker/notebooks/
ADD nodes /tmp/nodes.parquet
ADD edges /tmp/edges.parquet


LABEL io.k8s.description="spark-notebook Notebook with blockchain analysis." \
      io.k8s.display-name="spark-notebook Notebook with blockchain analysis." \
      io.openshift.expose-services="9000:http"

EXPOSE 9000
ENTRYPOINT ["bin/spark-notebook"]
