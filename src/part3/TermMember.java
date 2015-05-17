package part3;

import java.util.ArrayList;

public class TermMember {
	private String			fileName;
	private Double 			termFreq;
	private ArrayList<Long> offset;
	
	public TermMember() {
		this.fileName = "";
		this.termFreq = (Double)0.0;
		this.offset   = new ArrayList<Long>();
	}
	
	public TermMember(String fileName, Double termFreq, ArrayList<Long> offset) {
		this.fileName = fileName;
		this.termFreq = termFreq;
		this.offset   = offset;
	}
	
	public String getFileName(){
		return fileName;
	}
	
	public Double getTermFreq(){
		return termFreq;
	}
	
	public ArrayList<Long> getOffset(){
		return offset;
	}
	
	public void setTermFreq(Double termFreq) {
		this.termFreq = termFreq;
	}
	
	public void setOffset(ArrayList<Long> offset) {
		this.offset = offset;
	}
	
	public void appendOffset(ArrayList<Long> offset) {
		this.offset.addAll(offset);
	}
}
