package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class NestedLoopJoinPlan implements Plan {
	private Transaction tx;
	private Predicate pred;
	private Plan p1, p2, innerPlan, outerPlan;
	private String innerFldName, outerFldName;
	private Schema sch = new Schema();
	
	/**
	 * Constructor for NestedLoopJoinPlan - initializes variables to those passed to it.
	 * @param tx
	 * @param p1
	 * @param p2
	 * @param fldname1
	 * @param fldname2
	 */
	public NestedLoopJoinPlan(Transaction tx, Plan p1, Plan p2, Predicate pred) {
		this.tx = tx;
		this.p1 = p1;
		this.p2 = p2;
		this.pred = pred;

		sch.addAll(p1.schema());
		sch.addAll(p2.schema());
		if (p1.recordsOutput() < p2.recordsOutput()) {
			this.innerPlan = p2;
			this.outerPlan = p1;
		} else {
			this.innerPlan = p1;
			this.outerPlan = p2;
		}
    }
	
	public Scan open() {
		Scan inner = this.innerPlan.open();
		TempTable outer = copyRecordsFrom(new MaterializePlan(tx, this.outerPlan));
		return new NestedLoopJoinScan(tx, inner, innerFldName, outer, outerFldName, pred);
	}

	private TempTable copyRecordsFrom(Plan p) {
		Scan   src = p.open(); 
		Schema sch = p.schema();
		TempTable t = new TempTable(tx, sch);
		UpdateScan dest = (UpdateScan) t.open();
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields())
				dest.setVal(fldname, src.getVal(fldname));
		}
		src.close();
		dest.close();
		return t;
	}
	
	//size of outer + #outer blocks * size of inner
	public int blocksAccessed() {
		int blockSize  = tx.availableBuffs() - 2; // 1 for Input, 1 for output.
		int innerPages = innerPlan.blocksAccessed();
		int outerPages = outerPlan.blocksAccessed();
		return (int) Math.round(outerPages + Math.ceil(outerPages / blockSize) * innerPages);
	}
	
	/**
	 * Counts the records output by the nested loop join, which is the sum of those output by the inner and outer looops.
	 */
	public int recordsOutput() {
    	return innerPlan.recordsOutput() * outerPlan.recordsOutput();
	}

	/**
	 * Returns the distinct values corresponding to fldname in outerloop if they exist, else those in innerloop.
	 */
    public int distinctValues(String fldname) {
    	if (outerPlan.schema().hasField(fldname)) {
    		return outerPlan.distinctValues(fldname);
    	} else {
    		return innerPlan.distinctValues(fldname);
    	}
   }

    /**
     * Returns the schema of the nested loop join.
     */
   public Schema schema() {
      return sch;
   }
}
