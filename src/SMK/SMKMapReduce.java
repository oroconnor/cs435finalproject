package SMK;

import java.io.IOException;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
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

public class SMKMapReduce {
    /* The mapper is basically a pass through. 
    It will take the input data and make sure that it gets outputted in the format specified below. 
    input
    key: dummy offset value: String "stationID \t dateTimeStamp \t airTempC \t waterTempC \t waterTempCwAirRemoved"

    output
    key: String stationID value: String "dateTimeStamp \t airTempC \t waterTempC \t waterTempCwAirRemoved" 
    */

    public static class SMKMapper extends Mapper<LongWritable, Text, Text, Text> {


    public void map(LongWritable key, Text textInput, Context context) 
        throws IOException, InterruptedException {
            String normalizedInput = textInput.toString().replaceAll("\t", " ");
            String[] splitInput = normalizedInput.split("\\s+");
            
            if (splitInput.length == 5) {
                String stationID = splitInput[0];
                Text station = new Text(stationID);
                
                String restOfData = String.join("\t", splitInput[1], splitInput[2], splitInput[3], splitInput[4]);
                Text nums = new Text(restOfData);
                context.write(station, nums);
            }

            if (splitInput.length == 4) { // residuals haven't been calculated 
                String stationID = splitInput[0];
                Text station = new Text(stationID);
                
                String restOfData = String.join("\t", splitInput[1], splitInput[2], splitInput[3]);
                Text nums = new Text(restOfData);
                context.write(station, nums);
            }
        }

    } //end mapper

public static class SMKReducer extends Reducer<Text, Text, Text, Text> {

/*     The reducer is what will call SeasonalMannKendall Test on the data set for each station. 
    For each station (the key):
    -establish start and end date of the data set
    -do SMK test on the three variables of interest: airTempC,waterTempC,waterTempCwAirRemoved
    -export all this info together for each station in the output
    
    input
    key: String stationID value: List of [String "dateTimeStamp \t airTempC \t waterTempC \t waterTempCwAirRemoved"]
    
    output
    example:
    fakestation     airTempC, Total S=168.00, T=1.00, Z=8.54, p value below .05
    fakestation     waterTempC, Total S=168.00, T=1.00, Z=8.54, p value below .05
    fakestation     waterTempCwAirRemoved, Total S=168.00, T=1.00, Z=8.54, p value below .05
    
    */

    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
            ArrayList<DataPoint> datapoints = new ArrayList<>();
            for (Text value : values) {
                String normalizedInput = value.toString().replaceAll("\t", " ");
                String[] splitInput = normalizedInput.split("\\s+");
                String dateTime = splitInput[0];

                Double airTempC = Double.parseDouble(splitInput[1]);
                Double waterTempC = Double.parseDouble(splitInput[2]);
                if (splitInput.length == 4) {
                    Double waterTempCwAirRemoved = Double.parseDouble(splitInput[3]);
                    DataPoint dataRow = new DataPoint(dateTime,airTempC,waterTempC,waterTempCwAirRemoved);
                    datapoints.add(dataRow);  
                }
                if (splitInput.length == 3) {
                    DataPoint dataRow = new DataPoint(dateTime,airTempC,waterTempC,null);
                    datapoints.add(dataRow);  
                    System.out.printf("value:%s%n", value.toString());
                }
  
                }

                SMKutils smkUtils = new SMKutils();
                try {
                    ArrayList<String> output = smkUtils.seasonalMannKendall(datapoints, key.toString());
                    for (String out : output) {
                        context.write(key, new Text(out));
                    }
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    e.printStackTrace();
                }
               
        }

} // end reducer


}
