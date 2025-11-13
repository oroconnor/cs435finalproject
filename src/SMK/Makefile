TEAM_NAME=gp9
BUILD_DIR=build
SMK_SRC=*.java
SMK_CLASS=SMK.MainDriver
SMK_JAR=$(BUILD_DIR)/gp9-smk-1.0.jar
HDFS_PATH=/cs435-gp9
HDFS_INPUT_PATH=/cs435-gp9/data/part3.txt
HDFS_OUTPUT_PATH=/cs435-gp9/data/SMK/SMKoutput

# Seasonal Mann-Kendall
smk:
	@mkdir -p $(BUILD_DIR)
	hadoop com.sun.tools.javac.Main -d $(BUILD_DIR) $(SMK_SRC)

smk-jar: smk
	jar -cf $(SMK_JAR) -C $(BUILD_DIR) SMK

clean-hdfs:
	-hadoop fs -rm -r $(HDFS_PATH)
	hadoop fs -put ~/cs435-gp9 $(HDFS_PATH)
	-hadoop fs -rm -r $(HDFS_OUTPUT_PATH)

smk-run: smk-jar clean-hdfs
	hadoop jar $(SMK_JAR) $(SMK_CLASS) $(HDFS_INPUT_PATH)
