package Iris;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import java.io.IOException;


public class IrisReducer  extends Reducer <Text,Text,Text,Text> {
   String[] tempString;
   float tempSepalLength, tempSepalWidth, tempPetalLength, tempPetalWidth;
   float totalSepalLength, totalSepalWidth, totalPetalLength,  totalPetalWidth;
   float minSepalLength, maxSepalLength, meanSepalLength, minSepalWidth, maxSepalWidth, meanSepalWidth, minPetalLength, 
   maxPetalLength, meanPetalLength, minPetalWidth, maxPetalWidth, meanPetalWidth;

   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

       minSepalLength = minPetalLength = minSepalWidth = minPetalWidth = Float.MAX_VALUE;
       maxSepalLength = maxPetalLength = maxSepalWidth = maxPetalWidth = Float.MIN_VALUE;
       meanSepalLength = meanSepalWidth = meanPetalLength = meanPetalWidth = 0;
       tempSepalLength = tempSepalWidth = tempPetalLength = tempPetalWidth = 0;
       totalSepalLength = totalSepalWidth = totalPetalLength = totalPetalWidth = 0;
    
       int count = 0;
       
       for(Text value: values) {
    	  String[] tokens = value.toString().split("_");
    	  tempSepalLength = Float.parseFloat(tokens[0]);
          tempSepalWidth = Float.parseFloat(tokens[1]);
    	  tempPetalLength = Float.parseFloat(tokens[2]);
    	  tempPetalWidth = Float.parseFloat(tokens[3]);

         // determine if you have min/max sepal/petal length/widths and assign to min/max sepal/petal lenght/widths accordingly
    	  if(tempSepalLength<minSepalLength)
			  minSepalLength=tempSepalLength;
		  if(tempSepalLength>maxSepalLength)
		      maxSepalLength=tempSepalLength;
		  
		  if(tempSepalWidth<minSepalWidth)
			  minSepalWidth=tempSepalWidth;
		  if(tempSepalWidth>maxSepalWidth)
			  maxSepalWidth=tempSepalWidth;
		  
		  if(tempPetalLength<minPetalLength)
			  minPetalLength=tempPetalLength;
		  if(tempPetalLength>maxPetalLength)
			  maxPetalLength=tempPetalLength;
		  
		  if(tempPetalWidth<minPetalWidth)
			  minPetalWidth=tempPetalWidth;
		  if(tempPetalWidth>maxPetalWidth)
			  maxPetalWidth=tempPetalWidth;
              
         // Calculate running totals for sepal/petal length/widths for use in calculation of means
		  totalSepalLength+=tempSepalLength;
		  totalSepalWidth+=tempSepalWidth;
		  totalPetalLength+=tempPetalLength;
		  totalPetalWidth+=tempPetalWidth;

         // Increment counter for use in calculation of means
		  count++;

       } 
     
       // calculate mean sepal/petal length/width 
       meanSepalLength=totalSepalLength/count;
       meanSepalWidth=totalSepalWidth/count;
       meanPetalLength=totalPetalLength/count;
       meanPetalWidth=totalPetalWidth/count;

       // emit output to context
       context.write(key, new Text(minSepalWidth+"\t"+maxSepalWidth+"\t"+meanSepalWidth+"\t"+minSepalLength+"\t"+maxSepalLength+"\t"+meanSepalLength+"\t"+minPetalWidth+"\t"+maxPetalWidth+"\t"+meanPetalWidth+"\t"+minPetalLength+"\t"+maxPetalLength+"\t"+meanPetalLength));

   }
}
