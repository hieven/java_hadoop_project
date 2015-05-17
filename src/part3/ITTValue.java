package part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class ITTValue implements Writable {
	private ArrayList<Long> offset;
	private Double 			termFrequency;

	public ITTValue(){
		this.offset 	   = new ArrayList<Long>();
		this.termFrequency = new Double(0);
	}

	public ITTValue(ArrayList<Long> offset, Double termFrequency) {
		this.offset 	   = offset;
		this.termFrequency = termFrequency;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		// Write arrayList offset
		int len = offset.size();
		out.writeInt(len);
		for (int i = 0; i < len; i++) {
			out.writeLong(offset.get(i));
		}
		
		// Write termFrequency
		out.writeDouble(termFrequency);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		// Read arrayList offset
		offset.clear();
		int i = in.readInt();
		for (; i > 0; i--) {
			offset.add(in.readLong());
		}
		
		// Read termFrequency
		termFrequency = in.readDouble();
	}
	
	@Override
	public String toString(){
		// Output format:
		// termFrequency [offset1,offset2,...]
				
		String str = termFrequency.toString() + " ";
				
		int len = offset.size();
		str = str.concat("[" + offset.get(0).toString());
		for (int i = 1; i < len; i++) {
			str = str.concat("," + offset.get(i).toString());
		}
		str = str.concat("]");

		return str;
	}
	
	
	public ArrayList<Long> getOffset(){
		return offset;
	}
	
	public Double getTermFrequency() {
		return termFrequency;
	}
}

