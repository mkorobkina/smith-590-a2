// reducer function for application to calculate total number of bytes and find min, max timestamp
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class HostCountReducer extends Reducer<Text, Text, DoubleWritable, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double max = -1;
        double min = -1;
        int i = 0;
        String strKey = key.toString();
        double bytes = Double.parseDouble(strKey);
        double timeStamp = 0;
                
        for (Text value : values) {
            String val = value.toString();
            timeStamp = Double.parseDouble(val);

        if (i == 0) {
            max = timeStamp; //To start off, set the first value as the max
            min = timeStamp; //Set the first value as the min as well
            i = 1;
        }
        if (i != 0) {
            if (timeStamp > max) {
                max = timeStamp;
            } if (timeStamp < min) {
                min = timeStamp;
            }
        }
        }
                   
//         for (Text key : keys) { //Is this iteration correct? No -- I think this can be done more easily in the command line?
//             String stringKey = key.toString();
//             long bytes = Long.parseLong(stringKey);
//             totalBytes += bytes; //Sum all the bytes
//         }
        
        
        double elapsedTime = max - min;
        
        context.write(new DoubleWritable(bytes), new DoubleWritable(elapsedTime));
    }
}
