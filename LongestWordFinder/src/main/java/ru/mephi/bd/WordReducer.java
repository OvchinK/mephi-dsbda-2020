package ru.mephi.bd;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Reducer class
 *
 * Finds the longest word in input word sequence and removes duplicate words
 */
public class WordReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private int max = Integer.MIN_VALUE;
    private final ArrayList<Text> words = new ArrayList<>();

    /**
     * Reduce method for MapReduce process. Accumulate words with currently detected maximum word length
     * @param key Next word
     * @param value Container of NullWritable to provide compatibility with MapReduce, unused
     * @param context MapReduce job context, unused
     */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
        int len =  key.getLength();
        if(len > max) {
            //If word is longer than currently detected longest word, set max length to this word's length and
            // clear accumulated words
            max = len;
            words.clear();
        }
        if(len == max) {
            //If word has the same length as currently detected longest word, add it to accumulating container
            words.add(new Text(key));
        }
    }

    /**
     * Cleanup method for MapReduce process, called on each reducer/combiner node after reduce/combine step is done
     * @param context MapReduce job context
     * @throws IOException Thrown by context.write
     * @throws InterruptedException Thrown by context.write
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
        for(Text val : words) {
            context.write(val, NullWritable.get());
        }
    }
}