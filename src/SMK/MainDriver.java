package SMK;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import SMK.SMKMapReduce.SMKMapper;
import SMK.SMKMapReduce.SMKReducer;
import SMK.SummaryJob1.Mapper1;
import SMK.SummaryJob1.Reducer1;


import org.apache.hadoop.conf.Configured;

public class MainDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MainDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

            if (!runJob1(args[0])) {
                return 1; // fail
            }

            if (!runJob2()) {
                return 1; // fail
            }
            if (!runJob3()) {
                return 1; // fail
            }

            if (!runJob4()) {
                return 1; // fail
            } 

            return 0; // success if all pass
        }

        private boolean runJob1(String inputPath) throws Exception {
            Configuration conf = new Configuration();
    
            Job job = Job.getInstance(conf, "SMK Job");
    
            job.setJarByClass(MainDriver.class);
    
            job.setMapperClass(SMKMapReduce.SMKMapper.class);
            job.setReducerClass(SMKMapReduce.SMKReducer.class);
    
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
    
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path("/cs435-gp9/data/SMKoutput/TestOutput"));
    
            return job.waitForCompletion(true);
        }
    
        private boolean runJob2() throws Exception {
            Configuration conf = new Configuration();
    
            Job job = Job.getInstance(conf, "Top 10 job");
    
            job.setJarByClass(MainDriver.class);
    
            job.setMapperClass(SummaryJob1.Mapper1.class);
            job.setReducerClass(SummaryJob1.Reducer1.class);
    
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
    
            FileInputFormat.addInputPath(job, new Path("/cs435-gp9/data/SMKoutput/TestOutput"));
            FileOutputFormat.setOutputPath(job, new Path("/cs435-gp9/data/SMKoutput/SummaryOutput1"));
    
            return job.waitForCompletion(true);
        }
    
        private boolean runJob3() throws Exception {
            Configuration conf = new Configuration();
    
            Job job = Job.getInstance(conf, "Counts and % of trends");
    
            job.setJarByClass(MainDriver.class);
    
            job.setMapperClass(SummaryJob2.Mapper2.class);
            job.setReducerClass(SummaryJob2.Reducer2.class);
    
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
    
            FileInputFormat.addInputPath(job, new Path("/cs435-gp9/data/SMKoutput/TestOutput"));
            FileOutputFormat.setOutputPath(job, new Path("/cs435-gp9/data/SMKoutput/SummaryOutput2"));
    
            return job.waitForCompletion(true);
        }
    
        private boolean runJob4() throws Exception {
            Configuration conf = new Configuration();
    
            Job job = Job.getInstance(conf, "Median Sen Slope");
    
            job.setJarByClass(MainDriver.class);
    
            job.setMapperClass(SummaryJob3.Mapper3.class);
            job.setReducerClass(SummaryJob3.Reducer3.class);
    
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
    
            FileInputFormat.addInputPath(job, new Path("/cs435-gp9/data/SMKoutput/TestOutput"));
            FileOutputFormat.setOutputPath(job, new Path("/cs435-gp9/data/SMKoutput/SummaryOutput3"));
    
            return job.waitForCompletion(true);
        }
    }