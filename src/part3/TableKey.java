package part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;;


public class TableKey implements WritableComparable<TableKey> {
	private String word;

	public TableKey(){
		this.word = new String();
	}

	public TableKey(String word) {
		this.word = new String(word);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, word);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word = Text.readString(in);
	}
	
	@Override
	public String toString(){
		return word;
	}
	
	public String getWord(){
		return this.word;
	}

	@Override
	public int compareTo(TableKey e) {
		if (this.getWord().compareTo(e.getWord()) != 0) {
			return this.getWord().compareTo(e.getWord());
		} else {
			return 0;	
		}	
	}

}