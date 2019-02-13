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
        long max = -1;
        long min = -1;
        long totalBytes = 0;
        int i = 0;
        String strKey = key.toString();
        double bytes = Double.parseDouble(strKey);
        double timeStamp = 0;
                
        for (Text value : values) {
        String val = value.toString();
        timeStamp = Double.parseDouble(val);
        }

            
           // if (i == 0) {
             //   max = convertedVal; //To start off, set the first value as the max
               // min = convertedVal; //Set the first value as the min as well
                //i++;
            //}
            
            //if (convertedVal > max) {
              //  max = convertedVal;
            //}
            
            //if (convertedVal < min) {
             //   min = convertedVal;
            //}
            
      //  }
                   
//         for (Text key : keys) { //Is this iteration correct? No -- I think this can be done more easily in the command line?
//             String stringKey = key.toString();
//             long bytes = Long.parseLong(stringKey);
//             totalBytes += bytes; //Sum all the bytes
//         }
        context.write(new DoubleWritable(bytes), new LongWritable(timeStamp));
    }
}
