package com.KafkaTracingService.DataModels;

public class GetTopicDetailsResponse {

	String TopicName ;
	TopicInfoM topicData;
	public String getTopicName() {
		return TopicName;
	}
	public void setTopicName(String topicName) {
		TopicName = topicName;
	}
	public TopicInfoM getTopicData() {
		return topicData;
	}
	public void setTopicData(TopicInfoM topicData) {
		this.topicData = topicData;
	}
	
}
