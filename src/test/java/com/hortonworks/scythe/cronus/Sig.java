package com.hortonworks.scythe.cronus;

public class Sig {

	public Sig(String tagNm, java.sql.Timestamp time, Double value) {
		this.TagNm = tagNm;
		this.time = time;
		this.value = value;
	}
	
	private String TagNm = null;
	private java.sql.Timestamp time = null;
	private Double value = null;
	public String getTagNm() {
		return TagNm;
	}
	public void setTagNm(String tagNm) {
		TagNm = tagNm;
	}
	public java.sql.Timestamp getTime() {
		return time;
	}
	public void setTime(java.sql.Timestamp time) {
		this.time = time;
	}
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	
	
	
	
}
