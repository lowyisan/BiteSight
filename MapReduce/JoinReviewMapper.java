package YelpFoodBusinesses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class JoinReviewMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input data by tab (\t)
        String[] fields = value.toString().split("\t");

        // Ensure that there are enough fields to process
        if (fields.length > 1) {
            // The business_id is in the first field (index 0) in the review dataset
            String businessId = fields[1];  // Assuming business_id is in the second field of the review record

            // Concatenate all fields from fields[2] to the last field (everything except the business_id)
            StringBuilder outputValueStr = new StringBuilder();
            for (int i = 2; i < fields.length; i++) {
                if (i > 2) {
                    outputValueStr.append("\t"); // Add tab delimiter between fields
                }
                outputValueStr.append(fields[i]);
            }

            // Set the key and value for the context
            outputKey.set(businessId);  // Use business_id from the second field
            outputValue.set(outputValueStr.toString() + "\tr");  // Append "r" to identify as review data
            context.write(outputKey, outputValue);  // Output business_id -> review data
        }
    }
}
