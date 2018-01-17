#!/bin/bash
echo "begin export spark Data $1"
echo $1 $2 $3 $4

spark-submit --master yarn-cluster --num-executors 8  --driver-memory 4G --executor-memory 8g --executor-cores 8 --class com.lppz.spark.oms.ExportOMSDataSpark --name "exportOmsHostory-$1" /home/hadoop/omshistoryexport/lppz-spark-exportomshistory-1.0.0-SNAPSHOT.jar yarn-cluster omshisorderExport omsext "$2" "$3" "jdbc:mysql://10.8.202.231:3307/omsext" "oms_ro" "oms_ro" "$4"

echo "export spark Data $1 Successfully"
