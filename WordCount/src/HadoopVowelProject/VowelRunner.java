package HadoopVowelProject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class VowelRunner {
    // send jar file to execute the file and the way to execute the file is to use main method.
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Vowels Job");
        job.setMapperClass(VowelMapper.class);
        job.setReducerClass(VowelReducer.class);
        job.setJarByClass(VowelRunner.class);
        // Set the InputFormat for the job.
        // NLineInputFormat is a subclass of FileInputFormat class which is a subclass of the InputFormat.
        // NLineInputFile takes a text file as an input and splits them by line depending on N value.
        job.setInputFormatClass(NLineInputFormat.class);
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
        // TextOutputFormat is an OutputFormat that writes plain text files. OutputFormat describes
        // the output-specification for a Map-Reduce job.
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
