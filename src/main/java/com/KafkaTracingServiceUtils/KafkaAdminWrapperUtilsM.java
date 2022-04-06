package com.KafkaTracingServiceUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
//import java.util.Timer;
//import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import com.KafkaTracingService.DataModels.ConnectionParameters;
import com.KafkaTracingService.DataModels.TopicInfoM;

public class KafkaAdminWrapperUtilsM {

	public static AdminClient admin;
	public static KafkaConsumer<String, String> tConsumer;
	private Set<String> kTopics = null;
	private Map<String,TopicInfoM> to =null;
	private List<TopicPartition> tp=null;
	private KafkaConsumer<String, String> consumer = null;
	
	private Boolean timedOut = false;
	private Boolean readCompleted = false;
	StringBuffer messages = null;
	
	public Boolean getReadCompleted() {
		return readCompleted;
	}



	public Map<String, TopicInfoM> getTo() {
		return to;
	}



	public void setTo(Map<String, TopicInfoM> to) {
		this.to = to;
	}



	public void setReadCompleted(Boolean readCompleted) {
		this.readCompleted = readCompleted;
	}



	public KafkaAdminWrapperUtilsM(){
		to = new HashMap<String,TopicInfoM>();
		tp=  new ArrayList<TopicPartition>();
		
	}
	

	
	public Boolean getTimedOut() {
		return timedOut;
	}



	public void setTimedOut(Boolean timedOut) {
		this.timedOut = timedOut;
	}



	private Map<String, Object> CreateAndSetProperties(ConnectionParameters cp) {
		Map<String, Object> properties = new HashMap<String, Object>();
	    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cp.getBroker());
		properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, cp.getTimeOutinMs());
		return properties;
	}
	
	public void CreateAdminClient(ConnectionParameters cp) {
		admin = AdminClient.create(CreateAndSetProperties(cp));
	}
	
	public void CreateConsumerClient(ConnectionParameters cps) {
	     Properties props = new Properties();
	     props.setProperty("bootstrap.servers",cps.getBroker());
	     //props.setProperty("group.id", "test");
	     props.setProperty("enable.auto.commit", "false");
	     //props.setProperty("auto.commit.interval.ms", "1000");
	     props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     consumer = new KafkaConsumer<String, String>(props);
		 consumer.assign(tp);
     	 ListIterator<TopicPartition> tpIter = tp.listIterator();
		 while(tpIter.hasNext()) {
	         TopicPartition tpInfo = tpIter.next();
	         Long actualEndOffset = consumer.endOffsets(tp).get(tpInfo);
		     TopicInfoM tim = to.get(tpInfo.topic());
		     tim.setTopicDetails(tpInfo.partition(), actualEndOffset);
		     to.put(tpInfo.topic(), tim);
		 }
		 /*
		 Timer timer = new Timer();
		 TimerTask task = new TimerTask() {
		        public void run() {
		        	System.out.println("Timed Out");
		        	timedOut = true;
		        }
		 };
		 timer.schedule(task, 20000);
		 */
	}
	
	@SuppressWarnings("deprecation")
	public void readMessages() throws IOException {
		// Go to end offset
		consumer.seekToEnd(tp);
		messages = new StringBuffer();
		System.out.println("Read Started");
		while(timedOut == false) {
		    ConsumerRecords<String, String> records = consumer.poll(100);
		    for (ConsumerRecord<String, String> record : records) {
                String Message = "Received message: (Topic:"+record.topic() + " Partition:" + record.partition() +  
                                   "  Offset:"+ record.offset() + "  Key : " + record.key() + ", Message" + record.value() + ") \n" ;
                messages.append(Message);
            }
		}
		readCompleted = true;
		DumpBufferToFile();
		 
	}
	
	public void DumpBufferToFile() throws IOException {
		BufferedWriter bwr = new BufferedWriter(new FileWriter(new File("./MessageDetails.txt")));
		 //write contents of StringBuffer to a file
		 bwr.write(messages.toString());
		 //flush the stream
		 bwr.flush();
		 //close the stream
		 bwr.close();
	}
	
	
	
	public void CloseConsumerClient() {
		System.out.println("Consumer Closed");
		consumer.close();
	}
	
	
	public void DisplayTopics() {
		 Iterator<String> iter = kTopics.iterator();
		 while(iter.hasNext()) {
			 System.out.println(iter.next());
		 }
	}
	
	public void DescribeTopics() throws InterruptedException, ExecutionException {
		 Iterator<String> iter = kTopics.iterator();
		 while(iter.hasNext()) {
			 DescribeTopicInformation(iter.next());
		 }
	}
	
	public void DisplayTopicDetails() {
		 for (Map.Entry<String,TopicInfoM> entry : to.entrySet()) {
			 TopicInfoM tim = entry.getValue();
	          System.out.println("Topic Name = " + entry.getKey() +  ", Partitons Count = " + tim.getPartionCount() );
	          System.out.println("---------------------------------------------------------------------------------" );
	          tim.DumpPartitionTopicOffSetDetails();
	          System.out.println("---------------------------------------------------------------------------------" );
		 }
	}
	
	private void DescribeTopicInformation(String topic) throws InterruptedException, ExecutionException {
		
		//DescribeTopicsResult result = admin.describeTopics(Arrays.asList(topic));
		DescribeTopicsResult result = admin.describeTopics(kTopics);
		Map<String, KafkaFuture<TopicDescription>>  values = result.values();
		KafkaFuture<TopicDescription> topicDescription = values.get(topic);
		int partitionCount = topicDescription.get().partitions().size();
		List<TopicPartitionInfo> tpo  = topicDescription.get().partitions();
		ListIterator<TopicPartitionInfo> topIter  = tpo.listIterator();

		while (topIter.hasNext()) {
			TopicPartitionInfo tInfo = topIter.next();
		    // No entry Found for Topic Create topic
        	TopicInfoM tim = new TopicInfoM();
	    	tim.setPartionCount(partitionCount);
	    	tim.setTopicDetails(tInfo.partition(), new Long(-1));
	    	to.put(topic, tim);
			TopicPartition part = new TopicPartition(topic, tInfo.partition());
			tp.add(part);
		}
	}
	
	public void ListKafkaTopics() throws InterruptedException, ExecutionException {
		 ListTopicsOptions options = new ListTopicsOptions();
		 options.listInternal(false);
		 ListTopicsResult topics = admin.listTopics(options);
		 
		 kTopics = topics.names().get();
	}
	
}
