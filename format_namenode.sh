#!/bin/bash
export HADOOP_HOME=/opt/bigdata/hadoop-3.3.6
export PATH=/bin:/sbin:/usr/bin:/bin
hdfs namenode -format -y
