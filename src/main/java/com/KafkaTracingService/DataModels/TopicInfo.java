package com.KafkaTracingService.DataModels;

import java.util.List;

public class TopicInfo {
	
	private String topicName;
	private int partitonCount;
	private List<Integer> partions;
	
	public TopicInfo() {
		this.topicName = null;
		this.partitonCount=-1;
		this.partions = null;
	}
	public TopicInfo(String topicName, 
			           int partitonCount,
			           List<Integer> partions) {
		this.topicName = topicName;
		this.partitonCount=partitonCount;
		this.partions = partions;
	}
	
	public String getTopicName() {
		return topicName;
	}
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	public int getPartitonCount() {
		return partitonCount;
	}
	public void setPartitonCount(int partitonCount) {
		this.partitonCount = partitonCount;
	}
	
	public List<Integer> getPartions() {
		return partions;
	}
	public void setPartions(List<Integer> partions) {
		this.partions = partions;
	}

}
