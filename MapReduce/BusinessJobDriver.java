package YelpFoodBusinesses;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BusinessJobDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: BusinessJobDriver <businesses_input> <output-path>");
            System.exit(-1);
        }
        
        // Initialize the job
        Job job = Job.getInstance();
        job.setJarByClass(BusinessJobDriver.class);
        job.setJobName("Process Food Businesses");

        // Chaining the two mappers: BusinessMapper followed by BusinessValidationMapper
        ChainMapper.addMapper(job, BusinessMapper.class, Object.class, Text.class, Text.class, Text.class, job.getConfiguration());
        ChainMapper.addMapper(job, ValidationMapper.class, Text.class, Text.class, Text.class, Text.class, job.getConfiguration());

        // Use IdentityReducer to pass data unchanged
        job.setReducerClass(IdentityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input: Business data with a mapper to filter food businesses
        // Use FileInputFormat to add the input path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Output directory
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Delete if the output directory already exists
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        // Execute the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
