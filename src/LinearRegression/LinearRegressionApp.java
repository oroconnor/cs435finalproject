import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Encoder;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.functions;

public class LinearRegressionApp {
    public static final String APP_NAME = "GP9-LinearRegression";

    public static void main(String[] args) {

        // Default directories for the input and output in Hadoop
        String inputDir = "/lr-input";
        String outputDir = "/lr-output";

        // First arguments is the input directory
        if (args.length > 0) {
            inputDir = args[0];
        }

        // Second argument is the output directory
        if (args.length > 1) {
            outputDir = args[1];
        }

        // Create the spark session
        SparkSession session = SparkSession
                .builder()
                .appName(LinearRegressionApp.APP_NAME)
                .getOrCreate();

        // Define the schema that is coming in
        StructType schema = new StructType(new StructField[] {
            new StructField("station", DataTypes.StringType, true, Metadata.empty()),
            new StructField("date", DataTypes.StringType, true, Metadata.empty()),
            new StructField("air_temp", DataTypes.DoubleType, true, Metadata.empty()),
            new StructField("water_temp", DataTypes.DoubleType, true, Metadata.empty())
        });

        // For linear regression in Spark, there needs to be a "features" column
        // which is a vector; this will create the column when applied to a dataset
        VectorAssembler vectorAssembler = new VectorAssembler()
            .setInputCols(new String[] { "air_temp" })
            .setOutputCol("features");

        // Read the file and modify it
        Dataset<Row> data = session.read()
            .option("sep", "\t")    // Files is tab-separated
            .option("header", "false")  // There is no header
            .schema(schema) // Tell Spark what the column data types are
            .csv(inputDir)  // Specify where to find the files
            .withColumn("index", functions.monotonically_increasing_id()); // Add an "index" column for rejoining with the result

        // Create the modified dataset for training
        Dataset<Row> vectorized = vectorAssembler
            .transform(data)
            .withColumnRenamed("water_temp", "label");

        // NOTE: For more applicable results, there could be an additional step to sample
        // some training, test, and validation samples from the larger dataset.
        // This would prevent over-fitting

        // Utilize the Apache Spark ML library to perform the regression
        // Further optimization can be done by tweaking the parameters
        // https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/regression/LinearRegression.html
        LinearRegression regression = new LinearRegression();

        LinearRegressionModel model = regression.fit(vectorized);

        // The summary contains the "residuals"
        // Residuals are effectively the water temperature that cannot be explained by the air temperature
        // Equivalent to 'water temp - predicted water temp'
        // Get the residuals and add an index column so we can rejoin with the original dataset for outputting
        Dataset<Row> residuals = model
            .summary()
            .residuals()
            .withColumn("index", functions.monotonically_increasing_id());

        // Join the two sets back together and print to TSV
        data.join(residuals, "index")
            .select("station", "date", "air_temp", "water_temp", "residuals")
            .write()
            .option("sep", "\t") // TSV
            .option("header", "false")
            .csv(outputDir);
    }
}
