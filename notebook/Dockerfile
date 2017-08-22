FROM radanalyticsio/base-notebook

USER root
RUN mkdir /data

ENV NB_USER=nbuser
ENV NB_UID=1011

EXPOSE 8888

USER $NB_UID

ADD nodes.parquet /data/nodes.parquet
ADD edges.parquet /data/edges.parquet

ADD blockchain.ipynb /notebooks/blockchain.ipynb

USER root

RUN chown -R $NB_USER:root /home/$NB_USER /data \
    && find /home/$NB_USER -type d -exec chmod g+rwx,o+rx {} \; \
    && find /home/$NB_USER -type f -exec chmod g+rw {} \; \
    && find /data -type d -exec chmod g+rwx,o+rx {} \; \
    && find /data -type f -exec chmod g+rw {} \; \
    && chmod -f g+rw /notebooks/*

USER $NB_UID
ENV HOME /home/$NB_USER

LABEL io.k8s.description="PySpark Jupyter Notebook with blockchain analysis." \
      io.k8s.display-name="PySpark Jupyter Notebook with blockchain analysis." \
      io.openshift.expose-services="8888:http"

CMD ["/entrypoint", "/start.sh"]
