package com.nandish.code.kafka.stream.model;

import java.util.Date;

public class Click extends Parent{

	private String impressionId;
	private Date eventDate;
	private String clickId;

	public Date getEventDate() {
		return eventDate;
	}
	public void setEventDate(Date eventDate) {
		this.eventDate = eventDate;
	}
	public String getClickId() {
		return clickId;
	}
	public void setClickId(String clickId) {
		this.clickId = clickId;
	}
	public String getImpressionId() {
		return impressionId;
	}
	public void setImpressionId(String impressionId) {
		this.impressionId = impressionId;
	}
}
