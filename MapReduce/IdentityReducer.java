package YelpFoodBusinesses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class IdentityReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Simply pass all the values from the mapper to the output without modification
        for (Text value : values) {
            context.write(key, value);
        }	
    }
}
