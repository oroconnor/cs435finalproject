TEAM_NAME=gp9
BUILD_DIR=build
SRC_DIR=src

#
# Composite Join
#


CJ_DIR=$(SRC_DIR)/compositeJoin
CJ_SRC=$(CJ_DIR)/CompositeJoinMapReduce.java
CJ_CLASS=compositeJoin.CompositeJoinMapReduce
CJ_JAR=$(BUILD_DIR)/gp9-cj-1.0.jar

HDFS_AIR_PATH=/final/output_met.txt
HDFS_WATER_PATH=/final/output_wq.txt
HDFS_OUTPUT_PATH=/final/output

.PHONY: cj-compile cj-jar cj-run cj-clean cj-hdfs-clean cj-all

cj-compile:
	@mkdir -p $(BUILD_DIR)
	hadoop com.sun.tools.javac.Main -d $(BUILD_DIR) $(CJ_SRC) 

cj-jar: cj-compile
	jar -cf $(CJ_JAR) -C $(BUILD_DIR) compositeJoin

cj-run: cj-jar
	hadoop jar $(CJ_JAR) $(CJ_CLASS) -D mapreduce.framework.name=yarn $(HDFS_AIR_PATH) $(HDFS_WATER_PATH) $(HDFS_OUTPUT_PATH) "inner"

cj-clean:
	rm -rf $(BUILD_DIR)/*.class $(CJ_JAR)

cj-hdfs-clean:
	hadoop fs -rm -r -f $(HDFS_OUTPUT_PATH)

cj-all: cj-hdfs-clean cj-run	
