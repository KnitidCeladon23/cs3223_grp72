package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class NestedLoopJoinPlan implements Plan {
	private Plan outerLoop, innerLoop;
	private String fldname1, fldname2;
	private Schema sch = new Schema();
	private Transaction tx;
	
	public NestedLoopJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		if (p1.recordsOutput() < p2.recordsOutput()) {
	        innerLoop = p2;
	        outerLoop = p1;
	        this.fldname1 = fldname1;
	        this.fldname2 = fldname2;
		} else {
	        innerLoop = p1;
            outerLoop = p2;
	        this.fldname1 = fldname2;
	        this.fldname2 = fldname1;
		}
	    this.tx = tx;
	    sch.addAll(p1.schema());
	    sch.addAll(p2.schema());
    }
	
	public Scan open() {
		TableScan innerTs = (TableScan) outerLoop.open();
		TableScan outerTs = (TableScan) innerLoop.open();
		return new NestedLoopJoinScan(innerTs, outerTs, fldname1, fldname2, tx);
	}
	
	//size of outer + #outer blocks * size of inner
	public int blocksAccessed() {
		return outerLoop.recordsOutput() + (outerLoop.blocksAccessed() * innerLoop.recordsOutput());
	}
	
	public int recordsOutput() {
    	return outerLoop.recordsOutput() * innerLoop.recordsOutput();
	}

    public int distinctValues(String fldname) {
    	if (outerLoop.schema().hasField(fldname)) {
    		return outerLoop.distinctValues(fldname);
    	} else {
    		return innerLoop.distinctValues(fldname);
    	}
   }

   public Schema schema() {
      return sch;
   }
}
