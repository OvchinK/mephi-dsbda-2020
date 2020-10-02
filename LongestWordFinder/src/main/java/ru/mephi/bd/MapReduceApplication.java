package ru.mephi.bd;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders!");
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "longest word");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.SnappyCodec.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
    }
}
