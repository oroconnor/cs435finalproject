package compositeJoin;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;

// Composite Join with Map Reduce (old API)
public class CompositeJoinMapReduceOld {

	public static class CompositeMapper extends MapReduceBase implements Mapper<Text, TupleWritable, Text, Text> {
		private Text outputKey =  new Text();
        private Text outputValue = new Text();

        public void map(Text key, TupleWritable value, OutputCollector<Text, Text> output, Reporter reporter)
                        	throws IOException {
								
			// stationCode and dateTimeStamp 
			outputKey.set(key.toString().trim() + "\t"); 
			
			// Obtain air and water data, removing extra spaces
            String airInfo = ((Text) value.get(0)).toString().trim();
			String waterInfo = ((Text) value.get(1)).toString().trim();

        	outputValue.set(airInfo + " " + waterInfo);
					
			output.collect(outputKey, outputValue);
        }	
    }

	
	public static void main(String[] args) throws Exception {
		Path airPath = new Path(args[2]);
		Path waterPath = new Path(args[3]);
		Path outputDir = new Path(args[4]);
		String joinType = args[5];

		JobConf conf = new JobConf("Composite Join over Air and Water Temperature Data");
       	conf.setJarByClass(CompositeJoinMapReduceOld.class);
        conf.setMapperClass(CompositeJoinMapReduceOld.CompositeMapper.class);
        conf.setNumReduceTasks(0);

		// input format 
		conf.setInputFormat(CompositeInputFormat.class);
		conf.set("mapreduce.join.expr", 
				CompositeInputFormat.compose(joinType, 
				KeyValueTextInputFormat.class, airPath, waterPath));

		// output format
        TextOutputFormat.setOutputPath(conf, outputDir);
		conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
		
		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete()) {
			Thread.sleep(1000);
		}
		System.exit(job.isSuccessful() ? 0 : 1);
	}
}
