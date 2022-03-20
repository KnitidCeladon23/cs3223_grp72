package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class HashJoinScan implements Scan {

	private Scan s1, s2;
	private String fldname1, fldname2;
	private List<String> s1Fields;
	private int numOfPartitions;
	
	private int curr_index, curr_partition;
	private Map<String, Constant> curr_s1Record;
	private ArrayList<Map<String, Constant>> curr_arr;
	private Map<Constant, ArrayList<Map<String, Constant>>> hashTable;
	
	/**
	 * Constructor for HashJoinScan - initializes variables to those passed to it.
	 * @param s1
	 * @param s2
	 * @param fldname1
	 * @param fldname2
	 * @param s1Fields
	 * @param numOfPartitions
	 */
	public HashJoinScan(Scan s1, Scan s2, String fldname1, String fldname2, List<String> s1Fields, int numOfPartitions) {
		this.s1 = s1;
		this.s2 = s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.s1Fields = s1Fields;
		this.numOfPartitions = numOfPartitions;
		this.curr_index = 0;
		
		beforeFirst();
	}
	
	public void close() {
		s1.close();
		s2.close();
	}
	
	public void beforeFirst() {
		
	}
	
	/**
	 * Returns true if there remain partitions with unmatched records.
	 */
	public boolean next() {
		
		while (true) {
			
			// if curr_arr has already been initialised and there are still unmatched records
			if (curr_arr != null && curr_index < curr_arr.size()) {
				curr_s1Record = curr_arr.get(curr_index);
				curr_index++;
				return true;
			}
			
			//iterating through s2
			while(s2.next()) {
				// Pass s2 value into hash function and match against hash table.
				if(curr_partition == (s2.getVal(fldname2).hashCode() % numOfPartitions)) {
					if(hashTable.keySet().contains(s2.getVal(fldname2))) {
						curr_index = 0;
						curr_arr = hashTable.get(s2.getVal(fldname2));
						curr_s1Record = curr_arr.get(curr_index);
						curr_index++;
						return true;
					}
				}
			}
			curr_arr = null;
			
			// checking if there is next partition
			curr_partition++;
			if(curr_partition >= numOfPartitions) {
				return false;
			}
			
			s1.beforeFirst();
			s2.beforeFirst();
			hashTable = new HashMap<Constant, ArrayList<Map<String, Constant>>>();

			//iterating through s1
			while(s1.next()) {
				if(curr_partition == s1.getVal(fldname1).hashCode() % numOfPartitions) {
					Map<String, Constant> curr_record = new HashMap<>();
					for(String curr_fieldName: s1Fields) {
						curr_record.put(curr_fieldName, s1.getVal(curr_fieldName));
					}
					hashTable.computeIfAbsent(s1.getVal(fldname1), k -> new ArrayList<Map<String, Constant>>()).add(curr_record);
				}
			}
			
			return true;
		}
		
	}
	
	/**
	 * Returns the integer corresponding to the given field in s2 if one exists, else the String corresponding to the given field
	 * in s1.
	 */
	public int getInt(String fldname) {
		if (s2.hasField(fldname)) {
			return s2.getInt(fldname);
		} else { 
			return curr_s1Record.get(fldname).asInt();
		}
	}
	
	/**
	 * Returns the String corresponding to the given field in s2 if one exists, else the String corresponding to the given field
	 * in s1.
	 */
	public String getString(String fldname) {
		if (s2.hasField(fldname)) {
			return s2.getString(fldname);
		} else {
			return curr_s1Record.get(fldname).asString();
		}
	}
	
	/**
	 * Returns the value corresponding to the given key in s2 if it exists, else the value corresponding to the given key in s1.
	 */
	public Constant getVal(String fldname) {
		if (s2.hasField(fldname)) {
			return s2.getVal(fldname);
		} else {
			return curr_s1Record.get(fldname);
		}
	}
	
	/**
	 * Checks if either s1 or s2 have the given field in their keyset.
	 */
	public boolean hasField(String fldname) {
		return s2.hasField(fldname) || curr_s1Record.containsKey(fldname);
	}
}