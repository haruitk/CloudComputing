package storm.mp3.bolt;

import java.util.HashMap;
import java.util.Map;

import storm.starter.tools.Rankings;
import storm.starter.util.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class CounterBolt extends BaseBasicBolt {

	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 1;
	private Map<String, Integer> countsByRegion = new HashMap<String, Integer>();
	private int emitFrequencyInSeconds;
	
	public CounterBolt() {
        this(DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public CounterBolt(int emitFrequencyInSeconds) {
        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException("The emit frequency must be >= 1 seconds (you requested "
                + emitFrequencyInSeconds + " seconds)");
        }
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	
    	if (TupleHelpers.isTickTuple(tuple)) {
    		
    		int europeCount = countsByRegion.get("Europe");
        	int asiaCount = countsByRegion.get("Asia/Pacific");
        	int usCount = countsByRegion.get("US");
        	int total = europeCount + asiaCount + usCount;
        	
//        	collector.emit(new Values("Europe", europeCount));
//    		collector.emit(new Values("Asia/Pacific", asiaCount));
//    		collector.emit(new Values("US",usCount));
    		
    		collector.emit(new Values("Europe", europeCount, convertToPercentage(europeCount, total)));
    		collector.emit(new Values("Asia/Pacific", asiaCount, convertToPercentage(asiaCount, total)));
    		collector.emit(new Values("US", usCount, convertToPercentage(usCount, total)));
        }
        else 
        {
        	String region = tuple.getString(3);
        	Integer count = countsByRegion.get(region);
    		if(count == null)
    		{
    			countsByRegion.put(region, tuple.getInteger(1) + tuple.getInteger(2));
    		}
    		else
    		{
    			count = count + tuple.getInteger(1) + tuple.getInteger(2);
    			countsByRegion.put(region, count);
    		}
        	Integer europeCount = countsByRegion.get("Europe");
        	Integer asiaCount = countsByRegion.get("Asia/Pacific");
        	Integer usCount = countsByRegion.get("US");
        	Integer total = (europeCount==null?0:europeCount) + (asiaCount==null?0:asiaCount) + (usCount==null?0:usCount);
        	countsByRegion.put("total", total);
            	
      	}
    }

	private String convertToPercentage(int europeCount, int total) {
		Integer value = (int)(europeCount *100.0 / total+0.5);
		return value.toString()+ "%";
	}
    	
    	
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("region", "count", "percentage"));
    }
    
}
