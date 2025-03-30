package YelpFoodBusinesses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class JoinBusinessMapper extends Mapper<Object, Text, Text, Text> {

    private Text businessId = new Text();
    private Text businessData = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the business data by tab (\t)
        String[] fields = value.toString().split("\t");
        System.out.println(fields.length);
        if (fields.length == 11) {  // Ensure valid business data (assuming 7 fields for business)
            String businessIdValue = fields[0];  // business_id
            String name = fields[1];  // name
            String address = fields[2];
            String city = fields[3];  // city
            String state = fields[4];  // state
            String postal = fields[5];
            String lat = fields[6];  // latitude
            String lon = fields[7];  // longitude
            String categories = fields[9];
            String hours = fields[10];
            
            // Prepare business data
            String business = name + "\t" + address  + "\t" + city + "\t" + state + "\t" + postal  + "\t" +lat + "\t" + lon  + "\t" + categories + "\t" +hours;
            businessId.set(businessIdValue);
            businessData.set(business + "\tb");  // Append a "b" tag for business data
            System.out.println(businessData.toString());
            // Output business data with the business_id as the key
            context.write(businessId, businessData);
        }
    }
}
