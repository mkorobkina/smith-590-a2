// reducer function for application to calculate total number of bytes and find min, max timestamp
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class HostCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long max;
        long min;
        long totalBytes = 0;
        int i = 0;
        for (Text value : values) { //Is this iteration correct? Not sure
            if (i == 0) {
             max = value; //To start off, set the first value as the max
             min = value; //Set the first value as the min as well
             i++;
            }
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }
        
        for (Text key : keys) { //Is this iteration correct?
            long bytes = Long.parseLong(key);
            totalBytes += bytes; //Sum all the bytes
        }
        
        long elapsedTime = max - min;
        context.write(new LongWritable(totalBytes), new LongWritable(elapsedTime);
    }
}
