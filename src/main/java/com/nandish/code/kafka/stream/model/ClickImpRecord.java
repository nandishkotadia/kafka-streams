package com.nandish.code.kafka.stream.model;

import java.util.Date;

public class ClickImpRecord extends Parent{
	
	private String impressionId;
	private String clickId;
	private Date eventDate;
	private Double bid;
	private Long adId;
	
	public Long getAdId() {
		return adId;
	}
	public void setAdId(Long adId) {
		this.adId = adId;
	}
	public String getClickId() {
		return clickId;
	}
	public void setClickId(String clickId) {
		this.clickId = clickId;
	}
	public Date getEventDate() {
		return eventDate;
	}
	public void setEventDate(Date eventDate) {
		this.eventDate = eventDate;
	}
	public Double getBid() {
		return bid;
	}
	public void setBid(Double bid) {
		this.bid = bid;
	}
	public String getImpressionId() {
		return impressionId;
	}
	public void setImpressionId(String impressionId) {
		this.impressionId = impressionId;
	}
	
}
