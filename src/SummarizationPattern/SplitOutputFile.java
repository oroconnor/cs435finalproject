import java.io.*;
import java.nio.file.*;

public class SplitOutputFile {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java SplitOutputFile <inputFilePath> [wqOutputFilePath] [metOutputFilePath]");
            return;
        }

        String inputFilePath = args[0];

        // Default output file paths if not provided
        String wqOutputFilePath = args.length > 1 ? args[1] : "output_wq.txt";
        String metOutputFilePath = args.length > 2 ? args[2] : "output_met.txt";

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFilePath));
             BufferedWriter wqWriter = Files.newBufferedWriter(Paths.get(wqOutputFilePath));
             BufferedWriter metWriter = Files.newBufferedWriter(Paths.get(metOutputFilePath))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.matches(".*wq_.*")) {
                    // Remove the "wq" suffix and add a space
                    String modifiedLine = line.replaceFirst("wq_", ""); // Remove "wq"
                    modifiedLine = modifiedLine.replaceFirst("([a-zA-Z]+)(\\d+-\\d+)", "$1 $2");
                    wqWriter.write(modifiedLine);
                    wqWriter.newLine();
                } else if (line.matches(".*met_.*")) {
                    // Remove the "met" suffix and add a space
                    String modifiedLine = line.replaceFirst("met_", ""); // Remove "met"
                    modifiedLine = modifiedLine.replaceFirst("([a-zA-Z]+)(\\d+-\\d+)", "$1 $2"); 
                    metWriter.write(modifiedLine);
                    metWriter.newLine();
                }
            }

            System.out.println("Splitting completed successfully!");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
