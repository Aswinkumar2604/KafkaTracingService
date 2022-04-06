package com.KafkaTracingService.DataModels;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TopicInfoM {
	private Integer partionCount;
	private Map<Integer,Long> topicData = null;
	
	public TopicInfoM(){
		partionCount = new Integer(0);
		topicData = new HashMap<Integer,Long>();
		
	}
	
	public Integer getPartionCount() {
		return partionCount;
	}
	public void setPartionCount(Integer partionCount) {
		this.partionCount = partionCount;
	}
	
	
	public void setTopicDetails(Integer partion,Long actualEndOffset) {
		topicData.put(partion, actualEndOffset);
	}
	
	
	public Map<Integer,Long> getTopicPartitonOffSetDetails() {
	     return topicData;
	}

	public void DumpPartitionTopicOffSetDetails() {
		Iterator<Map.Entry<Integer,Long>> itr = topicData.entrySet().iterator();  
	     while(itr.hasNext()) {
				Map.Entry<Integer, Long> curr = itr.next();
				System.out.println("Current Partiton:"  + curr.getKey()+ "               With Offset:" + curr.getValue());
			}
			System.out.println("-------------------------------------------------------------------");
	}
}
