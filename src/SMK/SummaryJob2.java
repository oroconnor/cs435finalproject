package SMK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import SMK.DataPoint;
import SMK.SMKutils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SummaryJob2 {
    /* 
   
    */

    public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text trendType = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(", ");
            
            if (parts.length > 5) {
                String zString = parts[2].split("=")[1];
                double zScore = Double.parseDouble(zString);
                String significance = parts[5].trim();

                // determine significance, using < .05 p value as cut off
                if (significance.contains("below .05")) {
                    if (zScore > 0) {
                        trendType.set("Significant Positive Trend");
                    } else if (zScore < 0) {
                        trendType.set("Significant Negative Trend");
                    }
                } else {
                    trendType.set("No Significant Trend");
                }

                context.write(trendType, one);
            }
        }
    }

public static class Reducer2 extends Reducer<Text, IntWritable, Text, Text> {
    private int totalCount = 0;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        totalCount = 0;
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }

        // total count of trends
        totalCount += count;
        
        //store count in context for calculating percentages later
        context.getCounter("TrendCounts", key.toString()).increment(count);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text trend : new Text[] { 
                new Text("Significant Positive Trend"), 
                new Text("Significant Negative Trend"), 
                new Text("No Significant Trend") }) {
            
            long count = context.getCounter("TrendCounts", trend.toString()).getValue();
            double percentage = (double) count / totalCount * 100;
            String result = String.format("%d\t%.2f%%", count, percentage);
            
            context.write(trend, new Text(result));
        }
    }

}


}