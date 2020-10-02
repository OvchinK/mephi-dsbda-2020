package ru.mephi.bd;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Reducer class
 * <p>
 * Finds the longest word in input word sequence and removes duplicate words
 */
public class WordReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private int max = Integer.MIN_VALUE;
    private final ArrayList<Text> longestWords = new ArrayList<>();

    /**
     * Reduce method for MapReduce process. Accumulate words with currently detected maximum word length
     *
     * @param key     Next word
     * @param value   Container of NullWritable to provide compatibility with MapReduce, unused
     * @param context MapReduce job context, unused
     */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> value, Context context) {
        int length = key.getLength();
        if (length > max) {
            max = length;
            longestWords.clear();
        }
        if (length == max) {
            longestWords.add(new Text(key));
        }
    }

    /**
     * Cleanup method for MapReduce process, which write all longest words into context
     *
     * @param context MapReduce job context
     * @throws IOException          Thrown by context.write
     * @throws InterruptedException Thrown by context.write
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Text word : longestWords) {
            context.write(word, NullWritable.get());
        }
    }
}