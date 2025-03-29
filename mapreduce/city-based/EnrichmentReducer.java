package YelpFoodBusinesses;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.*;

public class EnrichmentReducer extends Reducer<Text, Text, Text, Text> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final HashMap<String, String[]> yelpMetadata = new HashMap<>();
    private final List<ObjectNode> enrichedList = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        String yelpPath = context.getConfiguration().get("yelpPath");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(yelpPath))));

        String line;
        while ((line = br.readLine()) != null) {
            JsonNode node = mapper.readTree(line);
            String id = node.has("business_id") ? node.get("business_id").asText() : null;
            if (id != null) {
                String categories = node.has("categories") ? node.get("categories").asText() : "";
                JsonNode hoursNode = node.get("hours");
                String[] hoursArray = new String[7];
                if (hoursNode != null && hoursNode.isObject()) {
                    String[] days = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
                    for (int i = 0; i < days.length; i++) {
                        JsonNode timeNode = hoursNode.get(days[i]);
                        hoursArray[i] = timeNode != null ? timeNode.asText() : null;
                    }
                }
                yelpMetadata.put(id, new String[]{categories, String.join("|||", hoursArray)});
            }
        }
        br.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        for (Text val : values) {
            String[] parts = val.toString().split("\t", 2);
            if (!parts[0].equals("AGG")) continue;

            String[] fields = parts[1].split("\t");
            if (fields.length < 13) continue;

            String[] meta = yelpMetadata.getOrDefault(fields[0], new String[]{"", ""});
            String[] hoursRaw = meta[1].split("\\|\\|\\|", -1);

            ArrayNode hoursArray = mapper.createArrayNode();
            for (String h : hoursRaw) {
                if (h == null || h.isEmpty()) {
                    hoursArray.addNull();
                } else {
                    hoursArray.add(h);
                }
            }

            double avgRating = 0;
            try {
                int total = Integer.parseInt(fields[12]);
                int sum1 = Integer.parseInt(fields[7]);
                int sum2 = Integer.parseInt(fields[8]);
                int sum3 = Integer.parseInt(fields[9]);
                int sum4 = Integer.parseInt(fields[10]);
                int sum5 = Integer.parseInt(fields[11]);
                avgRating = total > 0 ? (1 * sum1 + 2 * sum2 + 3 * sum3 + 4 * sum4 + 5 * sum5) / (double) total : 0.0;
            } catch (Exception ignored) {}

            ObjectNode node = mapper.createObjectNode();
            node.put("business_id", fields[0]);
            node.put("business_name", fields[1]);
            node.put("city", fields[2]);
            node.put("state", fields[3]);
            node.put("latitude", fields[4]);
            node.put("longitude", fields[5]);
            node.put("sum_star0", fields[6]);
            node.put("sum_star1", fields[7]);
            node.put("sum_star2", fields[8]);
            node.put("sum_star3", fields[9]);
            node.put("sum_star4", fields[10]);
            node.put("sum_star5", fields[11]);
            node.put("sum_totalreviews", fields[12]);
            node.put("avg_rating", avgRating);
            node.put("categories", meta[0]);
            node.set("hours", hoursArray);

            enrichedList.add(node);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayNode outputArray = mapper.createArrayNode();
        for (ObjectNode obj : enrichedList) {
            outputArray.add(obj);
        }
        String formatted = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(outputArray);
        context.write(null, new Text(formatted));
    }
}
