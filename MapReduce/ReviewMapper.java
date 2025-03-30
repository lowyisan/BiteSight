package YelpFoodBusinesses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ReviewMapper extends Mapper<Object, Text, Text, Text> {

    private Text reviewId = new Text();
    private Text reviewData = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input data by tab (\t)
        String[] fields = value.toString().split("\t");
        System.out.println(fields.length);
        // Only process the record if it has the expected number of fields (9 for reviews)
        if (fields.length == 9) {
            String reviewIdValue = fields[0];  // review_id
            String businessIdValue = fields[2];  // business_id
            String stars = fields[3];  // stars
            String reviewText = fields[7];  // review text
            String date = fields[8];  // date

            // Validate the review data: check reviewStars and reviewText
            try {
                // Check if stars is a valid number between 1 and 5
                float reviewStars = Float.parseFloat(stars);
                if (reviewStars >= 1.0 && reviewStars <= 5.0) {
                    // Check if the review text is not empty
                    if (reviewText != null && !reviewText.trim().isEmpty()) {
                        // Prepare the data: business_id, stars, text, date
                        String review = businessIdValue + "\t" + stars + "\t" + reviewText + "\t" + date;
                        reviewId.set(reviewIdValue);  // Set the review ID as the key
                        reviewData.set(review);  // Set the value with review data

                        // Output the key-value pair: key = review_id, value = business_id, stars, text, date
                        context.write(reviewId, reviewData);
                    } else {
                        System.out.println("Invalid review text (empty): " + value.toString());
                    }
                } else {
                    System.out.println("Invalid reviewStars value (not between 1 and 5): " + value.toString());
                }
            } catch (NumberFormatException e) {
                // If reviewStars is not a valid number, skip this record
                System.out.println("Invalid reviewStars format (not a number): " + value.toString());
            }
        } else {
            System.out.println("Invalid record (incorrect number of fields): " + value.toString());
        }
    }
}
