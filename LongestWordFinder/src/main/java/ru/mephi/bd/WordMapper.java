package ru.mephi.bd;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper class
 *
 * Splits inputted text into words, filters out malformed words and maps as keys
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final String delimiters = " !\"#$%&()*+,./:;<=>[\\]^~\n\t\r";

    /**
     * Map method for MapReduce process. Splits provided text by delimiters,
     * checks each word validity and writes them in context.
     * @param key Key value of MapReduce input, unused
     * @param value Input text
     * @param context MapReduce job context, unused
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), delimiters);
        while (itr.hasMoreTokens())
        {
            String str = itr.nextToken();
            if(str.matches("\\p{ASCII}*")) {
                context.write(new Text(str), NullWritable.get());
            }
        }
    }
}