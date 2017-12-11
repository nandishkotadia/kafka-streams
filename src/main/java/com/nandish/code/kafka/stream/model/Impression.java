package com.nandish.code.kafka.stream.model;

public class Impression extends Parent{

	private String impressionId;
	private Long adId;
	private Double bid;

	public Long getAdId() {
		return adId;
	}
	public void setAdId(Long adId) {
		this.adId = adId;
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
