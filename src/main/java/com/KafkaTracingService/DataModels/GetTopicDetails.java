package com.KafkaTracingService.DataModels;

public class GetTopicDetails {
	public String sessionStr;
	public String topicName;

	public String gettopicName() {
		return topicName;
	}
	public void settopicName(String topicName) {
		this.topicName = topicName;
	}
	public String getsessionStr() {
		return sessionStr;
	}
	public void setsessionStr(String sessionStr) {
		this.sessionStr = sessionStr;
	}
}
