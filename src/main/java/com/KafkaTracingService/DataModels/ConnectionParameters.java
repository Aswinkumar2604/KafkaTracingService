package com.KafkaTracingService.DataModels;

import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Value;

public class ConnectionParameters {
    
	@NotNull(message="Session String Mandatory")
	private String sessionStr;
	@NotNull(message="Broker Details needs to be passed")
	private String broker;
	@Value("100")
	private int timeOutinMs;
	private final int DEF_TIME_OUT = 1000;

	public String getBroker() {
		return broker;
	}

	public void setBroker(String broker) {
		this.broker = broker;
	}

	public int getTimeOutinMs() {
		return timeOutinMs;
	}

	public void setTimeOutinMs(int timeOutinMs) {
		if (timeOutinMs <= 0) {
			this.timeOutinMs = DEF_TIME_OUT;
		} else {
			this.timeOutinMs = timeOutinMs;
		}
	}

	public String getSessionStr() {
		return sessionStr;
	}

	public void setSessionStr(String sessionStr) {
		this.sessionStr = sessionStr;
	}
}
