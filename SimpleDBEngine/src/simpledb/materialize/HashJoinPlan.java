package simpledb.materialize;

import java.util.List;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {
	
	private Transaction tx;
	private Plan p1, p2;
	private String fldname1, fldname2;
	private Schema sch = new Schema();
	
	private int numOfPartitions;
	
	public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		this.tx = tx;
		this.p1 = p1;
		this.p2 = p2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		sch.addAll(p1.schema());
		sch.addAll(p2.schema());
		this.numOfPartitions = tx.availableBuffs() - 1;
	}
	
	public Scan open() {
		Scan s1 = p1.open();
		Scan s2 = p2.open();
		List<String> s1Fields = p1.schema().fields();
		return new HashJoinScan(s1, s2, fldname1, fldname2, s1Fields, numOfPartitions);
	}
	
	public int blocksAccessed() {
		return 3 * (p1.blocksAccessed() + p2.blocksAccessed());
	}
	
	public int recordsOutput() {
		return p1.recordsOutput() * p2.recordsOutput();
	}
	
	public int distinctValues(String fldname) {
		if (p1.schema().hasField(fldname)) {
			return p1.distinctValues(fldname);
		} else {
			return p2.distinctValues(fldname);
		}
	}
	
	public Schema schema() {
		return sch;
	}
	
	 /**
	    * Returns the String representation of the HashJoin plan.
	    * @return The corresponding String.
	    */
	   public String toString() {
		   return p1.toString() + " Hash Join + " + p2.toString() + " where " + fldname1 + " = " + fldname2;
	   }

}
