package ru.mephi.bd;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, NullWritable> mapDriver;
    private ReduceDriver<Text, NullWritable, Text, NullWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, NullWritable, Text, NullWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordMapper mapper = new WordMapper();
        WordReducer reducer = new WordReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text("pride-blinded challenging хоррорсоми"))
                .withInput(new LongWritable(), new Text("cunoniaceous ундирпиир reprograming"))
                .withOutput(new Text("pride-blinded"), NullWritable.get())
                .withOutput(new Text("challenging"), NullWritable.get())
                .withOutput(new Text("cunoniaceous"), NullWritable.get())
                .withOutput(new Text("reprograming"), NullWritable.get())
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<NullWritable> values = new ArrayList<>();
        values.add(NullWritable.get());
        reduceDriver
                .withInput(new Text("challenging"), values)
                .withInput(new Text("pride-blinded"), values)
                .withInput(new Text("cunoniaceous"), values)
                .withInput(new Text("reprograming"), values)
                .withOutput(new Text("pride-blinded"), NullWritable.get())
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        String in1 = new String(Files.readAllBytes(Paths.get("src/test/resources/input/in1")));
        String in2 = new String(Files.readAllBytes(Paths.get("src/test/resources/input/in2")));
        mapReduceDriver
                .withInput(new LongWritable(), new Text(in1))
                .withInput(new LongWritable(), new Text(in2))
                .withOutput(new Text("chemotherapeutically"), NullWritable.get())
                .withOutput(new Text("tribromoacetaldehyde"), NullWritable.get())
                .runTest();
    }
}