FROM radanalyticsio/base-notebook

USER root
RUN mkdir /data

ENV NB_USER=nbuser
ENV NB_UID=1011
ENV PYSPARK_DRIVER_PYTHON="jupyter"
ENV PYSPARK_DRIVER_PYTHON_OPTS="notebook"
ENV PYSPARK_SUBMIT_ARGS="--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11"

EXPOSE 8888

USER $NB_UID

ADD nodes.parquet /data/nodes.parquet
ADD edges.parquet /data/edges.parquet
ADD blockchain.ipynb /notebooks/blockchain.ipynb
ADD custom /home/$NB_USER/.jupyter/custom
ADD js /notebooks/js

USER root

# note to future me: here are some links so that you can continue after PTO :)
# custom js - http://jupyter-notebook.readthedocs.io/en/latest/examples/Notebook/JavaScript%20Notebook%20Extensions.html
# https://gist.github.com/pkerpedjiev/f2e6ebb2532dae603de13f0606563f5b
# http://languagenetwork.cotrino.com/langnet.js
# https://stackoverflow.com/questions/35946898/jupyter-input-cell-with-required-javascript-file-are-evaluated-at-page-load

RUN curl -L -O --progress-bar http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.5.0-spark2.1-s_2.11/graphframes-0.5.0-spark2.1-s_2.11.jar \
    && curl -L -O --progress-bar http://central.maven.org/maven2/com/typesafe/scala-logging/scala-logging-slf4j_2.11/2.1.2/scala-logging-slf4j_2.11-2.1.2.jar \
    && curl -L -O --progress-bar http://central.maven.org/maven2/com/typesafe/scala-logging/scala-logging-api_2.11/2.1.2/scala-logging-api_2.11-2.1.2.jar \
    && echo "56e120e4839005098ec0dfa5b69677c124788048 `ls graphframes-*`" | sha1sum -c - \
    && echo "7628485be79eacac9e7f19bf049f581394d45ea3 `ls scala-logging-slf4j_*`" | sha1sum -c - \
    && echo "fed5521041c26b8c1daa2d5e5dc5f01b9bde4fe6 `ls scala-logging-api_*`" | sha1sum -c - \
    && chown 500:500 *.jar \
    && chmod 666 *.jar \
    && mv *.jar /opt/spark/jars/ \
    && sed -i'' -e 's/^\(exec jupyter notebook\)$/#\1/' /start.sh \
    && echo "pyspark $PYSPARK_SUBMIT_ARGS" >> /start.sh \
    && chown -R $NB_USER:root /home/$NB_USER /data \
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
