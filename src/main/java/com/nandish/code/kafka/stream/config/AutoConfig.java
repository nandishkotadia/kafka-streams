package com.nandish.code.kafka.stream.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.nandish.code.kafka.stream.util.Constants;

@Configuration
@PropertySources({
	@PropertySource({"classpath:application.properties"})
})
public class AutoConfig implements Constants{

	public static Gson gson(){
		return new GsonBuilder().setDateFormat(DATE_FORMAT).create();
	}
	
}
