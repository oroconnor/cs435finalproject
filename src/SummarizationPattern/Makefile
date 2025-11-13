TEAM_NAME=gp9
BUILD_DIR=build
SP_DIR=.
SP_SRC=$(SP_DIR)/*.java
SP_JAR=$(BUILD_DIR)/$(TEAM_NAME)-sp-1.0.jar

HDFS_INPUT_PATH=/final/fullDataset
HDFS_OUTPUT_PATH=/final/SumPatOut

WQ_OUTPUT_FILE=outputFiles/part1_wq.txt
MET_OUTPUT_FILE=outputFiles/part1_met.txt

sp:
	@mkdir -p $(BUILD_DIR)
	hadoop com.sun.tools.javac.Main -d $(BUILD_DIR) $(SP_SRC)

sp-jar: sp
	jar -cvf $(SP_JAR) -C $(BUILD_DIR) .

sp-run: sp-jar
	hadoop jar $(SP_JAR) DownSampleDriver $(HDFS_INPUT_PATH) $(HDFS_OUTPUT_PATH)

split-output:
	@mkdir -p outputFiles
	java -cp $(BUILD_DIR) SplitOutputFile $(SPLIT_INPUT_FILE) $(WQ_OUTPUT_FILE) $(MET_OUTPUT_FILE)
