import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;
public class HostCountMapper
extends Mapper<LongWritable, Text, Text, Text> {
@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\s");
			String IPaddr1 = new String();  
			String IPaddr2 = new String();
			String direction = new String();
			String byteValue = new String();
			int last_dot;
			//get the two IP address.port fields 
			IPaddr1 = tokens[2];
			IPaddr2 = tokens[4];
			direction = tokens[3];
			byteValue = tokens[5];
			//get the port part (may want to change value names?)
			last_dot = IPaddr1.lastIndexOf('.'); 
			IPaddr1 = IPaddr1.substring(last_dot + 1, IPaddr1.length);//last part of first ip w port
			last_dot = IPaddr2.lastIndexOf('.');
			IPaddr2 = IPaddr2.substring(last_dot + 1, IPaddr2.length); //last part of ip w port
			//output the key, value pairs where the key is an
            //IP address 4-tuple and the value is 1 (count) + byte value
            //port value at the end of the IP address + byte value
			if (direction.equals(">")) {
				context.write(new Text(IPaddr1), new Text("1 " + byteValue));
			}
			else {
				context.write(new Text(IPaddr2), new Text("1 " + byteValue));
			}
		}
}
