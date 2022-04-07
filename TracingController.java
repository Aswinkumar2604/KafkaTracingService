package com.KafkaTracingService.Controller;

import java.io.IOException;
import java.util.ArrayList;
//import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

import javax.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
//import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.KafkaTracingService.DataModels.CloseSession;
import com.KafkaTracingService.DataModels.ConnectionParameters;
import com.KafkaTracingService.DataModels.GetTopicDetails;
import com.KafkaTracingService.DataModels.GetTopicDetailsResponse;
//import com.KafkaTracingService.DataModels.TopicInfo;
import com.KafkaTracingService.DataModels.TopicInfoM;
import com.KafkaTracingService.SessionMgmt.SessionManagement;
import com.KafkaTracingServiceUtils.KafkaAdminWrapperUtilsM;


@RestController
@RequestMapping("kafka")
public class TracingController {
	final int WAIT_PERIOD = 100000;
	@PostMapping(path ="/enabletrace" ,
			     consumes = { MediaType.APPLICATION_JSON_VALUE} , 
			     produces = { MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<Map<String,TopicInfoM>> EnableTrace(@Valid @RequestBody ConnectionParameters cps ) throws InterruptedException, ExecutionException {
		
		if(SessionManagement.SessionDetails.containsKey(cps.getSessionStr())) {
			return  new ResponseEntity<Map<String,TopicInfoM>>(HttpStatus.ALREADY_REPORTED);
		}
		
		//String data = "Broker:"+ cps.getBroker() +  " Timeout" + cps.getTimeOutinMs();
		KafkaAdminWrapperUtilsM kac = new KafkaAdminWrapperUtilsM();
		// Creates Kafka Admin Client
		kac.CreateAdminClient(cps);
		kac.ListKafkaTopics();
		kac.DescribeTopics();
		
		// Create a Consumer Client 
		kac.CreateConsumerClient(cps);
		SessionManagement.SessionDetails.put(cps.getSessionStr(), kac);
		
		// Start Read on seperate Thread.
		Thread newThread = new Thread(new Runnable() {
		    public void run() {
			    try {
			    	System.out.println("Thread Read Started");
					//kac.readMessages();
			    	kac.readMessagesWithDumping();
					System.out.println("Thread Read Completed.");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		});
		newThread.start();

		Map<String,TopicInfoM> topicDetails = kac.getTo();
		
		return new ResponseEntity<Map<String,TopicInfoM>>(topicDetails, HttpStatus.OK );
	}
	
	@PostMapping(path="/disabletrace",consumes = { MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<String> DisableTrace(@Valid @RequestBody CloseSession session) {
		if(!SessionManagement.SessionDetails.containsKey(session.getsessionStr())) {
			return  new ResponseEntity<String>(HttpStatus.NO_CONTENT);
		}
		KafkaAdminWrapperUtilsM kac = SessionManagement.SessionDetails.get(session.getsessionStr());
		kac.setTimedOut(true);
		// Wait for read to come out of loop
		Timer timer = new Timer();
		TimerTask closeConsumer = new TimerTask() {
		    public void run() {
		        System.out.println("Timed Out");
		        try {
		        	if(!kac.getReadCompleted()) {
					   kac.DumpBufferToFile();
		        	}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        kac.CloseConsumerClient();
		   }
		};
		timer.schedule(closeConsumer, WAIT_PERIOD);
		SessionManagement.SessionDetails.remove(session.getsessionStr());
		return  new ResponseEntity<String>("Trace Disabled Successfully", HttpStatus.OK);
	}
	
	@GetMapping(path="/topic", produces = { MediaType.APPLICATION_JSON_VALUE} )
	public ResponseEntity<GetTopicDetailsResponse> getTopicDetails(@Valid @RequestBody GetTopicDetails session ) {
		if(!SessionManagement.SessionDetails.containsKey(session.getsessionStr())) {
			return  new ResponseEntity<GetTopicDetailsResponse>(HttpStatus.NO_CONTENT);
		}
		KafkaAdminWrapperUtilsM kac = SessionManagement.SessionDetails.get(session.getsessionStr());
		Map<String,TopicInfoM> topics = kac.getTo();
		GetTopicDetailsResponse topic = new GetTopicDetailsResponse();
		topic.setTopicName(session.gettopicName());
		topic.setTopicData(topics.get(session.gettopicName()));
		return new ResponseEntity<GetTopicDetailsResponse>(topic,HttpStatus.OK);
	}
}
