import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

  public class InvertedIndex {

   public static class InvertedIndexMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
    @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {

                int lineNumber = Integer.parseInt(key.toString());
                String line = value.toString();
                String [] lines = line.split("\\W+");
                Text doc = new Text(lines[1]);
                for(int i = 1;i<lines.length;i++)
                {
                   output.collect(doc, new Text(lines[i]));
                }
        }


}

 public static class InvertedIndexReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterator<Text> values,OutputCollector<Text,Text> output, Reporter reporter) throws IOException {

			// Map to count word occurrences per file
			HashMap<String, Integer> wordByCounter = new HashMap<String, Integer>();

			while (values.hasNext()) {
				String word = values.next().toString();
				// Is the word in the map?
				Integer counter = wordByCounter.get(word);
				// if not initialize the counter. Else, take the actual counter
				// and add 1
				if (counter == null) {
					wordByCounter.put(word, 1);
				} else {
					wordByCounter.remove(word);
					Integer newCounter = counter + 1;
					wordByCounter.put(word, newCounter);
				}
			}
			
			StringBuilder result = new StringBuilder();
			for(String word:  wordByCounter.keySet())
			{
				result.append(word+"-"+wordByCounter.get(word)+" ");
			}
			
			output.collect(key, new Text(result.toString()));
       }
  }


    public static void main(String[] args) {
     JobClient client = new JobClient();
     JobConf conf = new JobConf(InvertedIndex.class);
     conf.setJobName("team29-mp2part2-InvertedIndex");
     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(Text.class);
     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
     conf.setMapperClass(InvertedIndexMapper.class);
     conf.setReducerClass(InvertedIndexReducer.class);
     client.setConf(conf);
      try {

          JobClient.runJob(conf);

       } catch (Exception e) {

      e.printStackTrace();

    }

  }

}