package compositeJoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

// Composite Join with Map Reduce (new API)
public class CompositeJoinMapReduce extends Configured implements Tool {

	public static class CompositeMapper extends Mapper<Text, TupleWritable, Text, Text> {
		private Text outputKey =  new Text();
       	private Text outputValue = new Text();

        public void map(Text key, TupleWritable value, Context context)
                        	throws IOException, InterruptedException {
								
			// stationCode and dateTimeStamp 
			outputKey.set(key.toString().trim() + "\t"); 
			
			// Obtain air and water data, removing extra spaces
            String airInfo = ((Text) value.get(0)).toString().trim();
			String waterInfo = ((Text) value.get(1)).toString().trim();

            outputValue.set(airInfo + " " + waterInfo);
					
			context.write(outputKey, outputValue);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Path airPath = new Path(args[0]);
		Path waterPath = new Path(args[1]);
		Path outputDir = new Path(args[2]);
		String joinType = args[3];

       	Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Composite Join over Air and Water Temperature Data");
		
        job.setJarByClass(CompositeJoinMapReduce.class);
        job.setMapperClass(CompositeJoinMapReduce.CompositeMapper.class);
        job.setNumReduceTasks(0);

		// input format 
		job.setInputFormatClass(CompositeInputFormat.class);
		job.getConfiguration().set("mapreduce.join.expr", 
				CompositeInputFormat.compose(joinType, 
				KeyValueTextInputFormat.class, airPath, waterPath));

		// output format
        TextOutputFormat.setOutputPath(job, outputDir);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CompositeJoinMapReduce(), args);
		System.exit(res); 
	}
}
