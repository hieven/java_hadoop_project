package part4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import part3.TableValue;
import part3.TermMember;

public class Retrieval {
    
    // Default setting
	static String   INPUT_DIR  = "./input";
	static String   OUTPUT_DIR = "./tableOutput";
    static String[] PART      = {OUTPUT_DIR + "/part-00000", OUTPUT_DIR + "/part-00001"};
    static int      MAX       = 10;
    
    public static void main(String[] args) throws Exception {
        
    	// Build HashMap
        HashMap<String, TableValue> map = new HashMap<String, TableValue>();
        int len = PART.length;
        for (int i = 0; i < len; i++) {
            HashMap<String, TableValue> row = createHashRows(PART[i]);
            map.putAll(row);
        }
        
        // Scan User input
        // Template: search "cat"
        // Advanced: search "cat or dog", "cat and dog"
	    Scanner input      = new Scanner(System.in);
	    String  inputStr   = input.nextLine();
	    String  patternStr = "([A-Za-z]+)\\s*\"(.*)\"";
	    Pattern pattern    = Pattern.compile(patternStr);
	    Matcher matcher    = pattern.matcher(inputStr);
	    input.close();
	    
	    // Split str
	    matcher.find();
        String action = matcher.group(1); 
        String params = matcher.group(2);
        
        
        ArrayList<String> andVocabs = new ArrayList<String>();
        ArrayList<String> notVocabs = new ArrayList<String>();
        ArrayList<String>    logics = new ArrayList<String>();
    	// Params might be
        // Basic: cat, Advanced: cat or dog
        String[] paramsArr = params.split("\\s");
        andVocabs.add(paramsArr[0]);
        	
        if (paramsArr.length > 1) {   	
        	for (int i = 1; i < paramsArr.length; i+= 2) {
        		if (paramsArr[i].equals("and")) {
	        		andVocabs.add(paramsArr[i+1]);
	        	} else {
	        		notVocabs.add(paramsArr[i+1]);
	        	}
        		logics.add(paramsArr[i]);
        	}
        }
        int andVocabsLen = andVocabs.size();
        int notVocabsLen = notVocabs.size();
	    int    logicsLen =    logics.size();
        
	    // Handle and/not logic gate
	    // Key:   fileName
	    // Value: counter
        HashMap<String, Integer> andMap = new HashMap<String, Integer>();
	    for (int i = 0; i < andVocabsLen; i++) {
            TableValue tValue = map.get(andVocabs.get(i));
            
            for (TermMember termMember : tValue.getTermMembers()) {
            	
            	if (andMap.get(termMember.getFileName()) == null) {
            		andMap.put(termMember.getFileName(), 1);
            	} else {
            		andMap.replace(termMember.getFileName(), andMap.get(termMember.getFileName())+1);
            	}
            }
        }
	    for (int i = 0; i < notVocabsLen; i++) {
	    	TableValue tValue = map.get(notVocabs.get(i));
	            
			for (TermMember termMember : tValue.getTermMembers()) {		
				if (andMap.get(termMember.getFileName()) != null) {
					andMap.replace(termMember.getFileName(), 0);
				}
			}
	    }

	    HashMap<String, Integer> FinalMap = new HashMap<String, Integer>();
	    for (String key : andMap.keySet()) {
	    	if (andMap.get(key) == andVocabsLen) {
	    		FinalMap.put(key, andMap.get(key));
	    	}
	    }
	    System.out.println(FinalMap);
	    
	    
	    // Create tValueMap to integrate andVocabs into a tValue.
	    // Key  : fileName
	    // Value: TermMember
	    HashMap<String, TermMember> tValueMap = new HashMap<String, TermMember>();
	    HashMap<String, Double>       weights = new HashMap<String, Double>();
	    // Each vocab we search
	    for (String vocab : andVocabs) {
	    	System.out.println();
	    	System.out.println(vocab);
	    	System.out.println("------------------------");
	    	// Look up each vocab in each file.
	    	for (TermMember termMember : map.get(vocab).getTermMembers()) {
	    		// Look up whether the file in FinalMap or not.
	    		for (String key : FinalMap.keySet()) {
	    		
	    			if (termMember.getFileName().equals(key)) {
	    				
	    				if (tValueMap.get(key) == null) {
	    					tValueMap.put(key, termMember);
	    				} else {
	    					TermMember tmp = tValueMap.get(key);
	    					tmp.setTermFreq(tmp.getTermFreq() + termMember.getTermFreq());
	    					tmp.appendOffset(termMember.getOffset());
	    					tValueMap.replace(key, tmp);
	    				}
	    				
	    				if (weights.get(key) == null) {
	    					weights.put(key, map.get(vocab).getWeight(key));
	    				} else {
	    					weights.replace(key, weights.get(key) + map.get(vocab).getWeight(key));
	    				}

	    			}
	    			
	    		}
	    	}
	    }
	    
	    // Build ordered {weight:termMember} pair
	    TreeMap<Double, TermMember> pairMap = createPairMap(weights, tValueMap);
	    
	    // Change order into Desc
	    NavigableMap<Double, TermMember> nmap = pairMap.descendingMap();
	    
	    
        // Results
        int Len = (nmap.size() < MAX) ? nmap.size() : MAX;
        for (int i = 0; i < Len; i++) {

        	Entry<Double, TermMember> entry    = nmap.pollFirstEntry();
        	String 					  fileName = entry.getValue().getFileName();
        	Double					  score    = entry.getKey();
        	RandomAccessFile 		  ra       = new RandomAccessFile(INPUT_DIR + "/" + fileName, "r");
        	
        	
        	System.out.println(Integer.toString(i+1) + "\t" + fileName + " = " + score);
        	System.out.println("************************");
        	
        	len = entry.getValue().getOffset().size();
        	for (int j = 0; j < len; j++) {
        		// Jump to that position in O(1);
            	ra.seek(entry.getValue().getOffset().get(j)-5);
            	System.out.println(ra.readLine());
        	}
        	
        	System.out.println("************************");
        }        
    }
    
    public static HashMap<String, TableValue> createHashRows(String dir) throws NumberFormatException, IOException {
        HashMap<String, TableValue> map = new HashMap<String, TableValue>();
        
        FileReader     fr = new FileReader(dir);
        BufferedReader br = new BufferedReader(fr);
        
        while (br.ready()) { 
            String   str    = br.readLine();
            String[] strArr = str.split(";");

            // Special case for first data to
            // Retrieve word, docFreq
            String[] strPart   = strArr[0].split("[\\s]+");
            String   word      = strPart[0];
            int      docFreq   = Integer.valueOf(strPart[1]);
            
            // Remove word, docFreq
            // Normalize strArr
            int len = strPart.length;
                str = "";
            for (int i = 2; i < len; i++) {
                str = str.concat(strPart[i]) + " ";
            }
            strArr[0] = str;

            // Collect termMembers
            ArrayList<TermMember> termMembers = new ArrayList<TermMember>();
            len = strArr.length;        
            for (int i = 0; i < len; i++) {
                TermMember termMember = createTermMember(strArr[i]);
                termMembers.add(termMember);
            }
            
            // Create tValue
            TableValue tValue = new TableValue(docFreq, termMembers);
                
            // Insert into HashMap
            map.put(word, tValue);    
        }   
        fr.close();
        return map;    
    }

    public static TermMember createTermMember(String strArr) {
        String[] strPart = strArr.split("[\\s]+");
    
        // Default setting
        String fileName        = strPart[0];
        Double termFreq        = Double.parseDouble(strPart[1]);
        ArrayList<Long> offset = new ArrayList<Long>();

        // Retrieve offset
        String  inputStr   = strPart[2];
        String  patternStr = "(\\d+)";
        Pattern pattern    = Pattern.compile(patternStr);
        Matcher matcher    = pattern.matcher(inputStr);
        while (matcher.find()) {
            offset.add(Long.parseLong(matcher.group().trim()));
        }
        
        return new TermMember(fileName, termFreq, offset);
    }
    
    public static double getFilesNum() {
    	String[] str = new File(INPUT_DIR).list();
    	int tmp = str.length;
    	int len = str.length;
    	
    	for (int i = 0; i < tmp; i++) {
    		if (str[i].charAt(0) == '.') {
    			len--;
    		}
    	}
    	return (double)len;
    }

    public static TreeMap<Double, TermMember> createPairMap(HashMap<String, Double> weights, HashMap<String, TermMember> tValueMap) {
    	TreeMap<Double, TermMember> pairMap = new TreeMap<Double, TermMember>();
    	
    	
    	for (String vocab : weights.keySet()) {
    		pairMap.put(weights.get(vocab), tValueMap.get(vocab));
    	}
    	return pairMap;
    }
}
