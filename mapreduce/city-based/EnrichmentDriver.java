package YelpFoodBusinesses;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EnrichmentDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: EnrichmentDriver <aggregated-input> <yelp-json> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("yelpPath", args[1]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(EnrichmentDriver.class);
        job.setJobName("Join Aggregated with Yelp Metadata");

        job.setMapperClass(EnrichmentMapper.class);
        job.setReducerClass(EnrichmentReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
