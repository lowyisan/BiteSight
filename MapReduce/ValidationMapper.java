package YelpFoodBusinesses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ValidationMapper extends Mapper<Text, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input value by tab to separate the fields
        String[] fields = value.toString().split("\t");

        // Check if the fields are valid (none of the fields are empty or null)
        if (isValid(fields)) {
            // If the fields are valid, write the data to the context
            outputKey.set(key);  // Use the same key (businessId)
            outputValue.set(value);  // Pass the original value (business data)
            context.write(outputKey, outputValue);  // Write to context
        } else {
            // Optionally log invalid data
            System.out.println("Invalid data (empty/null fields): " + value.toString());
        }
    }

    // Method to check for null or empty fields
    private boolean isValid(String[] fields) {
        for (String field : fields) {
            if (field == null || field.trim().isEmpty()) {
                return false;  // Return false if any field is null or empty
            }
        }
        return true;  // All fields are non-empty
    }
}
