FROM radanalyticsio/openshift-spark:2.2-latest

ADD nodes /tmp/nodes.parquet
ADD edges /tmp/edges.parquet

CMD ["/opt/spark/bin/launch.sh"]
