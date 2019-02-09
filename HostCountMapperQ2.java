
// map function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class HostCountMapper extends Mapper<LongWritable, LongWritable, Text, Text> {
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\s");
        String bytes = new String();
        bytes = tokens[8]; //Get byte count
        String timestamp = new String();
        timestamp = tokens[2]; //Get timestamp
        context.write(new Text(bytes), new Text(timestamp));
    }
}
