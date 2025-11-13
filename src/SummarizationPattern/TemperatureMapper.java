import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TemperatureMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private static final String NUTRIENT_SUFFIX = "nut"; 
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("M/d/yyyy HH:mm");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        if (!fileName.endsWith(".csv")) {
            System.err.println("Skipping non-CSV file: " + fileName);
            return;
        }

        String line = value.toString().trim();
        String[] fields = line.split(",");

        // If the line doesn't have enough fields, skip it
        if (fields.length < 10) {
            System.err.println("Skipping line due to insufficient columns: " + line);
            return;
        }

        // Trim to avoid extra spaces, also remove surrounding double quotes
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].replaceAll("^\"|\"$", "").trim();
        }

        String stationCode = fields[0];  
        String dateTimeStr = fields[2];
        String temperatureStr = null;

        // Check station type and process accordingly
        if (stationCode.endsWith("wq")) {  
            temperatureStr = fields[6];  // Water temperature column
        } else if (stationCode.endsWith("met")) { 
            temperatureStr = fields[7];  // Air temperature column
        } else {
            System.err.println("Skipping line due to invalid station code: " + stationCode);
            return;  // Skip invalid stations
        }

        // Skip if station is nutrient
        if (stationCode.endsWith(NUTRIENT_SUFFIX)) {
            System.err.println("Skipping nutrient station: " + stationCode);
            return;
        }

        // Parsing temp
        try {
            // Remove non-numeric characters like <4>, <0>, etc. from temperature data
            temperatureStr = temperatureStr.replaceAll("[^0-9.-]", "");
            Double temperature = Double.parseDouble(temperatureStr);

            // Parsing date-time stamp
            Date date = dateFormat.parse(dateTimeStr);

            // Create a key using station code and formatted date (month-year)
            Text outputKey = new Text(stationCode + "_" + (date.getMonth() + 1) + "-" + (date.getYear() + 1900));
            context.write(outputKey, new DoubleWritable(temperature));

        } catch (NumberFormatException | ParseException e) {
            System.err.println("Error processing record: " + line);
        }
    }
}

