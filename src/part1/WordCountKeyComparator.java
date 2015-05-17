package part1;

import java.lang.Character;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountKeyComparator extends WritableComparator {
	
	public WordCountKeyComparator() {
		super(Text.class, true);
	}

	// TODO Order by A -> a -> B -> b .... 
	public int compare(WritableComparable o1, WritableComparable o2) {
		Text key1 = (Text) o1;
		Text key2 = (Text) o2;
		String s1 = key1.toString().substring(0, 1);
		String s2 = key2.toString().substring(0, 1);
		
		if(s1.compareToIgnoreCase(s2)<0)		return -1;
		else if(s1.compareToIgnoreCase(s2)>0)	return 1;
		else									return s1.compareTo(s2);
	}
}
