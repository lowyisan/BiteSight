package YelpFoodBusinesses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EnrichmentMapper extends Mapper<Object, Text, Text, Text> {
    private final Text businessIdKey = new Text();
    private final Text outputVal = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws java.io.IOException, InterruptedException {
        // Input from aggregated-r-00000
        String[] parts = value.toString().split("\t");
        if (parts.length < 13) return;

        String businessId = parts[0];
        businessIdKey.set(businessId);
        outputVal.set("AGG\t" + value.toString());  // Tag it

        context.write(businessIdKey, outputVal);
    }
}
