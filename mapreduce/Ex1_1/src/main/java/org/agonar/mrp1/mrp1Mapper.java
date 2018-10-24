package org.agonar.mrp1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.TwitterObjectFactory;
import twitter4j.Status;

public class mrp1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context){
		String rawtweet = value.toString();
		String [] searchwords = {"FLU","ZIKA","DIARRHEA","EBOLA", "SWAMP","CHANGE"};
		try {
			Status tweetstatus  = TwitterObjectFactory.createStatus(rawtweet);
			String[] words = tweetstatus.getText().toUpperCase().split(" ");
			for(int i=0; i<words.length;i++){
				for(int j=0;j<searchwords.length;j++){
					if(words[i].contains(searchwords[j])){
						context.write(new Text(searchwords[j]),new Text(tweetstatus.getId()+""));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
