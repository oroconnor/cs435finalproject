import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0;
        int count = 0;

        //Iterate over all temperature values for a given key (StationCode + DateTime)
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count > 0) {
            double averageTemperature = sum / count;
            result.set(averageTemperature);
            context.write(key, result);

            //Debug print statement to track reducer output
            System.out.println("Key: " + key + " Average Temperature: " + averageTemperature);
        } else {
            System.out.println("No values for key: " + key);
        }
    }
}
