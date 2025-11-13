package SMK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

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
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SummaryJob1 {

    public static class Mapper1 extends Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Double, Text> topTrends = new TreeMap<>();
        private int counter = 0; // to ensure unique keys for z score

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            if (parts.length == 2) {
                String trendData = parts[1];

                // handle lines with "Insufficient data"
                if (trendData.contains("Insufficient data")) {
                    System.err.println("Skipping line with insufficient data: " + line);
                    return; //skip processing this line
                }

                String[] dataFields = trendData.split(", ");
                String zString = dataFields[2].split("=")[1];
                double zScore = Math.abs(Double.parseDouble(zString));
                // need to make sure keys are unique if Z scores match
                double uniqueKey = zScore + counter++ * 0.00000001; 

                // add to TreeMap and keep only top 10
                topTrends.put(uniqueKey, new Text(line));
                if (topTrends.size() > 10) {
                    topTrends.remove(topTrends.firstKey()); 
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text t : topTrends.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class Reducer1 extends Reducer<NullWritable, Text, NullWritable, Text> {
        private TreeMap<Double, Text> topTrends = new TreeMap<>();
        private int counter = 0; 

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String line = value.toString();
                String[] parts = line.split("\t", 2);

                String[] dataFields = parts[1].split(", ");
                String zString = dataFields[2].split("=")[1];
                double zScore = Math.abs(Double.parseDouble(zString));

                double uniqueKey = zScore + counter++ * 0.00000001;

                topTrends.put(uniqueKey, new Text(line));
                if (topTrends.size() > 10) {
                    topTrends.remove(topTrends.firstKey()); 
                }
            }

            for (Text t : topTrends.descendingMap().values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }
}