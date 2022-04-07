package com.KafkaTracingServiceUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MessageFileDumper {
	
	private static Map<String,BufferedWriter>  bwr;
	static
    {
    	bwr = new HashMap<String,BufferedWriter>();
    }
	
	public void DumpBufferToFile(StringBuffer buffer, String topic) throws IOException {
		System.out.println("Buffer Dump Started for Topic : " + topic);
		BufferedWriter currWriter;
		if(!bwr.containsKey(topic)) {
			currWriter = new BufferedWriter(new FileWriter(new File("./Messages-"+ topic+ ".txt")));
		} else {
			currWriter = bwr.get(topic);
		}
		currWriter.write(buffer.toString());
		currWriter.newLine();
		currWriter.flush();
		System.out.println("Buffer Dump Completed for Topic : " + topic);
	}
    
	public void CloseBufferDumper( ) throws IOException {
		System.out.println("CloseBufferDumper Started");
		 for(Map.Entry<String,BufferedWriter> bw:bwr.entrySet())  
	     {  
			 BufferedWriter currBwr = bw.getValue();
			 //close the stream
			 currBwr.close();
	     }
		 System.out.println("CloseBufferDumper Completed");
	}
	
	

}
