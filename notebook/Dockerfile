FROM jkremser/base-notebook

USER root
RUN mkdir /data

ENV NB_USER=nbuser \
    NB_UID=1011 \
    PYSPARK_DRIVER_PYTHON="jupyter" \
    PYSPARK_DRIVER_PYTHON_OPTS="notebook" \
    SPARK_MASTER=""\
    PYSPARK_SUBMIT_ARGS="--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 --driver-memory 4G"


EXPOSE 8888

USER $NB_UID
RUN mkdir /tmp/data/

ADD addresses /tmp/addresses.parquet
ADD blocks /tmp/blocks.parquet
ADD transactions /tmp/transactions.parquet
ADD edges /tmp/edges.parquet
# ADD nodes3 /tmp/nodes3.parquet
# ADD edges3 /tmp/edges3.parquet
# ADD graphframes.ipynb /notebooks/graphframes.ipynb
ADD blockchain.ipynb /notebooks/blockchain.ipynb
ADD custom /home/$NB_USER/.jupyter/custom
ADD js /notebooks/js

USER root

RUN curl -L -O --progress-bar http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.5.0-spark2.1-s_2.11/graphframes-0.5.0-spark2.1-s_2.11.jar \
    && curl -L -O --progress-bar http://central.maven.org/maven2/com/typesafe/scala-logging/scala-logging-slf4j_2.11/2.1.2/scala-logging-slf4j_2.11-2.1.2.jar \
    && curl -L -O --progress-bar http://central.maven.org/maven2/com/typesafe/scala-logging/scala-logging-api_2.11/2.1.2/scala-logging-api_2.11-2.1.2.jar \
    && echo "56e120e4839005098ec0dfa5b69677c124788048 `ls graphframes-*`" | sha1sum -c - \
    && echo "7628485be79eacac9e7f19bf049f581394d45ea3 `ls scala-logging-slf4j_*`" | sha1sum -c - \
    && echo "fed5521041c26b8c1daa2d5e5dc5f01b9bde4fe6 `ls scala-logging-api_*`" | sha1sum -c - \
    && su $NB_USER -c "echo \"c.NotebookApp.token = ''\" >> /home/$NB_USER/.jupyter/jupyter_notebook_config.py" \
    #&& conda install -y --quiet -c conda-forge jupyter_contrib_nbextensions \
    && pip install https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tarball/master \
    && pip install networkx \
    && jupyter contrib nbextension install --user \
    && jupyter nbextension enable toc2/main \
    && jupyter nbextension enable python-markdown/main \
    && jupyter nbextension enable init_cell/main \
    && jupyter trust /notebooks/blockchain.ipynb \
    # && jupyter trust /notebooks/graphframes.ipynb \
    && chown 500:500 *.jar \
    && chmod 666 *.jar \
    && mv *.jar /opt/spark/jars/ \
    && sed -i'' -e 's/^\(exec jupyter notebook\)$/#\1/' /start.sh \
    && echo 'ADDITIONAL_ARGS=`[[ ! -z "$SPARK_MASTER" ]] && echo "--master $SPARK_MASTER" || echo ""`' >> /start.sh \
    && echo "pyspark \$ADDITIONAL_ARGS \$PYSPARK_SUBMIT_ARGS" >> /start.sh \
    && chown -R $NB_USER:root /home/$NB_USER /data \
    && find /home/$NB_USER -type d -exec chmod g+rwx,o+rx {} \; \
    && find /home/$NB_USER -type f -exec chmod g+rw {} \; \
    && find /data -type d -exec chmod g+rwx,o+rx {} \; \
    && find /data -type f -exec chmod g+rw {} \; \
    && chmod -f g+rw /notebooks/*
    # && conda remove --quiet --yes --force qt pyqt \
    # && conda remove --quiet --yes --force --feature mkl ; conda clean -tipsy

USER $NB_UID
ENV HOME /home/$NB_USER

LABEL io.k8s.description="PySpark Jupyter Notebook with blockchain analysis." \
      io.k8s.display-name="PySpark Jupyter Notebook with blockchain analysis." \
      io.openshift.expose-services="8888:http"

CMD ["/entrypoint", "/start.sh"]
