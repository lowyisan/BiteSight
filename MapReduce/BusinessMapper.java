package YelpFoodBusinesses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class BusinessMapper extends Mapper<Object, Text, Text, Text> {
    private Text businessId = new Text();
    private Text businessData = new Text();
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input data by tab (\t) as it's expected to be a TSV format
        String[] fields = value.toString().split("\t");
        System.out.println(fields.length);
        // Check if the line has the expected number of fields (13 fields)
        if (fields.length < 14) {
            return;  // Skip this line if it doesn't have the correct number of fields
        }

        // Check if the 13th field (index 12) contains the word "food"
        String category = fields[12];
        //System.out.println(category);
        if (category.toLowerCase().contains("food")) {
        	//System.out.println("Food");
            // Extract relevant business information
            String businessIdValue = fields[0];  // Business ID is in the first field
            String name = fields[1];
            String address = fields[2];
            String city = fields[3];
            String state = fields[4];
            String postal = fields[5];
            String lat = fields[6];
            String lon = fields[7];
            String reviewCount = fields[9];
            String categories = fields[12];
            String hours = fields[13];

            // Validation: Ensure the fields are not empty and lat, lon, and reviewCount are valid numbers
            if (!name.isEmpty() && !city.isEmpty() && !state.isEmpty() && !lat.isEmpty() && !lon.isEmpty() && !reviewCount.isEmpty() && !address.isEmpty() && !postal.isEmpty() && !categories.isEmpty() && !hours.isEmpty()) {
                try {
                    // Try parsing latitude, longitude, and review count to ensure they're valid numbers
                    Double.parseDouble(lat);   // Ensure latitude is a valid number
                    Double.parseDouble(lon);   // Ensure longitude is a valid number
                    Integer.parseInt(reviewCount); // Ensure review count is a valid number

                    // If everything is valid, construct the business data and write it to context
                    String business = name + "\t" + address + "\t" + city + "\t" + state + "\t" + postal + "\t" + lat + "\t" + lon + "\t" + reviewCount+ "\t" + categories + "\t" + hours;
                    businessId.set(businessIdValue);
                    businessData.set(business);

                    // Write the output to context
                    //System.out.println(business);
                    context.write(businessId, businessData);

                } catch (NumberFormatException e) {
                    // Handle the case where lat, lon, or reviewCount cannot be parsed into numbers
                    System.out.println("Invalid number format in data: " + value.toString());
                }
            } else {
                // If any required field is empty, discard the data
                System.out.println("Invalid business data (missing fields): " + value.toString());
            }
        } else {
            // If the category doesn't contain "food", skip the line
            System.out.println("This business does not have a food-related category: " + category);
        }
    }
}
