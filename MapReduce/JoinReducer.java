package YelpFoodBusinesses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.HashMap;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String businessMeta = "";
        HashMap<Integer, Integer> starCounts = new HashMap<>();
        int totalNumberOfReview = 0;

        // Initialize the starCounts map with all star ratings (0 to 5)
        for (int i = 0; i <= 5; i++) {
            starCounts.put(i, 0);  // Set initial count for each star rating to 0
        }

        // Process the values for the given business_id (key)
        for (Text val : values) {
            String[] parts = val.toString().split("\t", 2);  // Split the value by tab

            if (parts.length < 2) continue;  // Skip malformed records

            // Check if it's business data or review data
            if (parts[1].endsWith("\tb")) {  // Check for the "b" tag indicating business data
                businessMeta = parts[0] + "\t" + parts[1].substring(0, parts[1].length() - 2);  // Remove the "b" tag
            } else if (parts[1].endsWith("\tr")) {  
                // Check for the "r" tag indicating review data
                String reviewData = parts[0] + "\t" + parts[1].substring(0, parts[1].length() - 2);  // Remove the "r" tag

                // Extract the star rating, review text, and date from the review (assuming it's in specific columns)
                String[] reviewFields = reviewData.split("\t");

                if (reviewFields.length > 2) {
                    try {
                        float stars = Float.parseFloat(reviewFields[0]);  // Parse the star rating (float value)
                        int roundedStars = Math.round(stars);  // Round the float to the nearest integer (0 to 5)

                        if (roundedStars >= 0 && roundedStars <= 5) {
                            // Increment the count for this star rating
                            starCounts.put(roundedStars, starCounts.get(roundedStars) + 1);
                        }
                    } catch (NumberFormatException e) {
                        // Handle parsing errors (invalid star rating)
                        System.out.println("Error parsing star rating: " + reviewFields[0]);
                    }

                    // Now build the output for raw data (business data with review data)
                    if (businessMeta.length() > 0 && reviewFields.length > 2) {
                    	totalNumberOfReview+=1;  // Increment the count each time a line is written
                        String reviewOutput = businessMeta + "\t" + reviewFields[0] + "\t" + reviewFields[1] + "\t" + reviewFields[2];
                     
                        mos.write("raw", key, new Text(reviewOutput));  // Write the combined business and review data
                        
                    }
                }
            }
        }
        
        // Ensure that we write the aggregated data for star counts (if businessMeta is available)
        if (!businessMeta.isEmpty()) {
            // Output the raw business data (only business metadata will be written here)
            //mos.write("raw", key, new Text(businessMeta)); 
            // Prepare the output: Format star counts for each star (0 to 5)
            // Calculate the total sum of the 5 star counts
            int totalStarCount = 0;
            StringBuilder aggregatedOutput = new StringBuilder();
            for (int i = 0; i <= 5; i++) {
                int starCount = starCounts.get(i);
                aggregatedOutput.append(starCount).append("\t");  // Add the count of reviews for each star rating
                totalStarCount += starCount;  // Add the count to total sum
            }

            // Append the total sum of all the star counts to the output
            aggregatedOutput.append(totalStarCount);  // Add the total number of reviews at the end of the line

            // Output the business meta data along with the star counts and the total
            mos.write("aggregated", key, new Text(businessMeta + "\t" + aggregatedOutput.toString()));  // Write aggregated data to "aggregated" file }
    }
    }
}