package YelpFoodBusinesses;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class JoinDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: JoinJobDriver <businesses_input> <reviews_input> <output-path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(JoinDriver.class);
        job.setJobName("Join Business and Review Data");

        // Input: Both business and review datasets
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinBusinessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinReviewMapper.class);

        // Set the reducer class (Join data)
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Ensure only 1 reducer (1 output file)
        job.setNumReduceTasks(1);

        // Configure multiple outputs: raw and aggregated files
        MultipleOutputs.addNamedOutput(job, "raw", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "aggregated", TextOutputFormat.class, Text.class, Text.class);
        
        // Output directory
        Path outputPath = new Path(args[2]);
        FileSystem fs = outputPath.getFileSystem(job.getConfiguration());

        // If the output path already exists, delete it
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);  // true = recursive deletion
        }

        // Set the output path for the job
        FileOutputFormat.setOutputPath(job, outputPath);

        // Execute the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
