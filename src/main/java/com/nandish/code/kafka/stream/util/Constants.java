package com.nandish.code.kafka.stream.util;

public interface Constants {
	
	public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
	
	public static class KafkaTopic{
		public final static String Click = "click";
		public final static String Impression = "impression";
		public final static String ClickImp = "clickimp";
	}
	
	public static class KafkaStreamType{
		public final static String impression = "impression";
		public final static String click = "click";
	}
	
}
