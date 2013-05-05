import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Galaxy {
	
	private static final String MASS ="mass";
	private static final String DISTANCE ="distance";
	private static final String DIAMETER ="diameter";
	private static final String ROTATION ="rotation";
	
	public Galaxy() {
		// TODO Auto-generated constructor stub
		}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] split = line.split("[a-z]+:");
			Text name = new Text(split[1]);

			output.collect(name, new Text(MASS + ":" + split[2]));
			output.collect(name, new Text(DISTANCE + ":" + split[3]));
			output.collect(name, new Text(DIAMETER + ":" + split[4]));
			output.collect(name, new Text(ROTATION + ":" + split[5]));

		}
	}


	public static class Reduce extends MapReduceBase implements	Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,	OutputCollector<Text, Text> output, Reporter reporteer)throws IOException {

			List<Integer> massList = new ArrayList<Integer>();
			List<Integer> distanceList = new ArrayList<Integer>();
			List<Integer> diameterList = new ArrayList<Integer>();
			List<Integer> rotationList = new ArrayList<Integer>();

			while (values.hasNext()) {
				String data = values.next().toString();
				String[] splitData = data.split(":");
				String dataType = splitData[0].trim();
				String[] integers = splitData[1].split("\\s");

				List<Integer> integerList = new ArrayList<Integer>();
				for (int i = 1; i < integers.length; i++) {
					integerList.add(Integer.parseInt(integers[i]));
				}

				if (MASS.equalsIgnoreCase(dataType)) {
					massList.addAll(integerList);
				} else if (DISTANCE.equalsIgnoreCase(dataType)) {
					distanceList.addAll(integerList);
				} else if (DIAMETER.equalsIgnoreCase(dataType)) {
					diameterList.addAll(integerList);
				} else if (ROTATION.equalsIgnoreCase(dataType)) {
					rotationList.addAll(integerList);
				}
			}

			String result = " massavg: " + calculateAverage(massList)
					+ " distanceavg: " + calculateAverage(distanceList)
					+ " diameteravg: " + calculateAverage(diameterList)
					+ " rotationavg: " + calculateAverage(rotationList);
			output.collect(key, new Text(result));
		}
	}

	private static String calculateAverage(List<Integer> integers) {
		String average = "";
		if (!integers.isEmpty()) {
			Integer sum = 0;
			for (Integer integer : integers) {
				sum += integer;
			}
			average = Integer.toString(sum / (integers.size()));
		}
		return average;
	}


	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Galaxy.class);
		conf.setJobName("team29-mp2part3-Galaxy");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}
}
