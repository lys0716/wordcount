import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yishuanglu on 5/9/17.
 */
public class Driver {
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create configuration

        Configuration conf = new Configuration();
        conf.setBoolean("text.to.lowercase", false);
        Job job = Job.getInstance(conf, "word count");

        job.setJobName("NYC311 Data Cleaning");
        job.setJarByClass(Driver.class);

        // Setup MapReduce
        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);

        // define output file number
        job.setNumReduceTasks(3);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);

    }
}
