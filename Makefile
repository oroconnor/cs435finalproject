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

HDFS_INPUT_PATH=/final/air_data.txt
HDFS_SECOND_INPUT_PATH=/final/water_data.txt
HDFS_OUTPUT_PATH=/final/output

cj:
	@mkdir -p $(BUILD_DIR)
	hadoop com.sun.tools.javac.Main -d $(BUILD_DIR) $(CJ_SRC) 

cj-jar: cj
	jar -cf $(CJ_JAR) -C $(BUILD_DIR) compositeJoin

cj-run: cj-jar
	hadoop jar $(CJ_JAR) $(CJ_CLASS) -D mapreduce.framework.name=yarn $(HDFS_INPUT_PATH) $(HDFS_SECOND_INPUT_PATH) $(HDFS_OUTPUT_PATH) "inner"

# 
# Linear Regression
# 

LR_DIR=$(SRC_DIR)/LinearRegression
LR_POM=$(LR_DIR)/pom.xml
LR_JAR=$(LR_DIR)/target/gp9-lr-1.0.jar

lr: $(LR_DIR)/*.java
	mvn --file $(LR_POM) compile clean

lr-jar:
	mvn --file $(LR_POM) package

lr-run:
	spark-submit --class LinearRegressionApp $(LR_JAR)
