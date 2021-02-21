#!/bin/sh

export SPARK_KAFKA_VERSION=0.10 && \
spark2-submit --master yarn \
--deploy-mode client \
--driver-memory 2g \
--executor-memory 2g \
--executor-cores 2 \
--principal duc.nguyenv3 \
--keytab /home/duc.nguyenv3/duc.nguyenv3.keytab \
--files app.cfg,logging.ini \
spark_streaming.py
