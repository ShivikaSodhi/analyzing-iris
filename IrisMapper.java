package Iris;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.StringTokenizer;

public class IrisMapper  extends Mapper <LongWritable,Text,Text,Text> {
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	   String[] token =  value.toString().split("\\s+");
	   String sepL =  token[0];
	   String sepW = token[1];
	   String petL = token[2];
	   String petW = token[3];
      String FlowerId = token[4];

      // write output to context as key-value pair where key is 
      // flowerId and value is underscore-separated concatenation of 
      // sepal/petal length/widths
      context.write(new Text(FlowerId),new Text(sepL + "_" + sepW + "_" + petL + "_" + petW));
   }
}