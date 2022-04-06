package com.KafkaTracingService.SessionMgmt;

import java.util.HashMap;
import java.util.Map;

import com.KafkaTracingServiceUtils.KafkaAdminWrapperUtilsM;

public class SessionManagement {

	public static Map<String,KafkaAdminWrapperUtilsM> SessionDetails ;
	 static
	    {
		 SessionDetails = new HashMap<>();
	    }
	 public static StringBuffer messagesBuffer;
	 static
	  {
		 messagesBuffer = new StringBuffer();
	  }
}
