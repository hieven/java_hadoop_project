package part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class ITTKey implements WritableComparable<ITTKey> {
	private String file;
	private String word;

	public ITTKey(){
		this.file = new String();
		this.word = new String();
	}

	public ITTKey(String file, String word) {
		this.file = new String(file);
		this.word = new String(word);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, file);
		Text.writeString(out, word);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		file = Text.readString(in);
		word = Text.readString(in);
	}
	
	@Override
	public String toString(){
		return word + "\t" + file;
	}
	
	
	public String getFile(){
		return this.file;
	}
	
	public String getWord(){
		return this.word;
	}

	@Override
	public int compareTo(ITTKey e) {
		if (this.getWord().compareTo(e.getWord()) != 0) {
			return this.getWord().compareTo(e.getWord());
		} else if (this.getFile().compareTo(e.getFile()) != 0) {
			return this.getFile().compareTo(e.getFile());
		} else {
			return 0;	
		}
		
	}
}

