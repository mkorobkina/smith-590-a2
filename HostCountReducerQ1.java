import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class HostCountReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			Long count = new Long(0);
			Long bytes = new Long(0);
			// iterate through all the values (count == 1) with a common key
			for (Text value : values) {
				String[] tokens = value.toString().split("\\s");
			    count += Long.valueOf(tokens[0]).longValue();
			    bytes += Long.valueOf(tokens[1]).longValue();
			}
			context.write(key, new Text(count.toString() + " " + bytes.toString()));
		}
}
