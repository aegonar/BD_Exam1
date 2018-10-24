package org.agonar.mrp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class mrp1Reducer extends Reducer<Text, Text, Text, Text>{

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String str = "";
		for (Text value : values ){
			str+=value+" ";
		}

		context.write(key, new Text(str));
	}
}
