FROM radanalyticsio/openshift-spark:2.3-latest

ADD addresses /tmp/addresses.parquet
ADD blocks /tmp/blocks.parquet
ADD transactions /tmp/transactions.parquet
ADD edges /tmp/edges.parquet

CMD ["/opt/spark/bin/launch.sh"]
