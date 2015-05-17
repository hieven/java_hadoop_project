package part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import part4.Retrieval;

public class TableValue implements Writable {
	private int						docFreq;
	private ArrayList<TermMember>   termMembers;

	public TableValue(){
		this.docFreq 	  = 0;
		this.termMembers  = new ArrayList<TermMember>();
	}
	
	public TableValue(int docFreq, ArrayList<TermMember> termMembers) {
		this.docFreq	  = docFreq;
		this.termMembers  = termMembers;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		// Write docFreq
		out.writeInt(docFreq);
		
		
		// Write arrayList termMembers
		int Len = termMembers.size();
		out.writeInt(Len);
		TermMember termMember = new TermMember();
		for (int i = 0; i < Len; i++) {
			termMember = termMembers.get(i);
			
			// Write fileName
			Text.writeString(out, termMember.getFileName());
			
			// Write termFreq
			out.writeDouble(termMember.getTermFreq());
			
			// Write offset
			int len = termMember.getOffset().size();
			out.writeInt(len);
			for (int j = 0; j < len; j++) {
				out.writeLong(termMember.getOffset().get(j));
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		// Read docFrequency
		docFreq = in.readInt();
				
		
		// Read arrayList termMembers
		termMembers.clear();
		int Len = in.readInt();
		for (int i = 0; i < Len; i++) {
					
			// Read fileName
			String fileName = Text.readString(in);
					
			// Read termFreq
			Double termFreq = in.readDouble();
			
			// Read offset
			int len = in.readInt();
			ArrayList<Long> arr = new ArrayList<Long>();
			for (int j = 0; j < len; j++) {
				arr.add(in.readLong());
			}
			TermMember termMember = new TermMember(fileName, termFreq, arr);
			termMembers.add(termMember);
		}
		
	}
	
	@Override
	public String toString(){
		// Output format:
		// docFrequency fileName termFrequency [offset1,offset2,...]
		
		String str = Integer.toString(docFreq) + " ";
		
		
		TermMember termMember = termMembers.get(0);
		str = str + termMember.getFileName() + " ";
		str = str + termMember.getTermFreq().toString() + " ";
		
		int len = termMember.getOffset().size();
		str = str.concat("[" + termMember.getOffset().get(0).toString());
		for (int j = 1; j < len; j++) {
			str = str.concat("," + termMember.getOffset().get(j).toString());
		}
		str = str.concat("]");	
		
		int Len = termMembers.size();
		for (int i = 1; i < Len; i++) {
			termMember = termMembers.get(i);
			str = str + ";";
			str = str + termMember.getFileName() + " ";
			str = str + termMember.getTermFreq().toString() + " ";
			
			len = termMember.getOffset().size();
			str = str.concat("[" + termMember.getOffset().get(0).toString());
			for (int j = 1; j < len; j++) {
				str = str.concat("," + termMember.getOffset().get(j).toString());
			}
			str = str.concat("]");	
		}

		return str;
	}
	
	public int getDocFreq() {
		return docFreq;
	}
	
	public ArrayList<TermMember> getTermMembers(){
		return termMembers;
	}
	
	public ArrayList<Double> getWeights() {
		ArrayList<Double> weights = new ArrayList<Double>();
		int len = termMembers.size();
		
		for (int i = 0; i < len; i ++) {
			double TF     = termMembers.get(i).getTermFreq();
			double N      = Retrieval.getFilesNum();
			double IDF    = Math.log(N / (double)docFreq);
			double weight = TF * IDF;
			
			System.out.println("OMG");
			if (termMembers.get(i).getFileName().equals("glossary")) {
				System.out.println(TF);
				System.out.println(N);
				System.out.println(IDF);
				System.out.println(weight);
			}
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			weights.add(weight);
		}
		return weights;
	}
	
	public Double getWeight(String fileName) {
		double N      = Retrieval.getFilesNum();
		double IDF    = Math.log(N / (double)docFreq);
		double TF     = 0.0;
		double weight = 0.0;
		
		for (TermMember tm : termMembers) {
			
			if (tm.getFileName().equals(fileName)) {
				TF = tm.getTermFreq();
				break;
			}
		}
		weight = TF * IDF;
		return weight;
	}
}