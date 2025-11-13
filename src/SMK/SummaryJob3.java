package SMK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SummaryJob3 {
    /* 
   Calculates the median Sen slope for each variable (airTempC , waterTempC , waterTempCwAirRemoved) 
   from the whole set of staions. 
    */

    public static class Mapper3 extends Mapper<Object, Text, Text, DoubleWritable> {
        
        private Text variable = new Text();
        private DoubleWritable senSlope = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            if (parts.length == 2) {
                String trendData = parts[1];

                if (trendData.contains("Insufficient data")) {
                    System.err.println("Skipping line with insufficient data: " + line);
                    return; // skip processing this line
                }

                String[] dataFields = trendData.split(", ");
                String varName = dataFields[0]; 
                double slopeValue = Double.parseDouble(dataFields[4].split("=")[1]);
                variable.set(varName);
                senSlope.set(slopeValue);
                context.write(variable, senSlope);
            }
        }
    }


public static class Reducer3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> slopes = new ArrayList<>();

            // put all slopes into a list for the variable
            for (DoubleWritable value : values) {
                slopes.add(value.get());
            }

            // sort the slopes and calculate the median
            Collections.sort(slopes);
            int size = slopes.size();
            double median;
            if (size % 2 == 0) {
                median = (slopes.get(size / 2 - 1) + slopes.get(size / 2)) / 2.0;
            } else {
                median = slopes.get(size / 2);
            }

            context.write(key, new DoubleWritable(median));
        }
    }

}