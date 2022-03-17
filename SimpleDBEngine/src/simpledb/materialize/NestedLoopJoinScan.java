package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class NestedLoopJoinScan implements Scan {
	
	private TableScan innerTs, outerTs;
	private String fldnameInner, fldnameOuter;
	private Transaction tx;
	
	private int blockSize;
	private RID rid_prevBlock;
	private int currRecordInBlock;
	
	public NestedLoopJoinScan(TableScan innerTs, TableScan outerTs, String fldnameInner, String fldnameOuter, Transaction tx) {
		this.innerTs = innerTs;
		this.outerTs = outerTs;
		this.fldnameInner = fldnameInner;
		this.fldnameOuter = fldnameOuter;
		this.tx = tx;
		this.blockSize = Math.max(tx.availableBuffs() - 2, 1);
		
		//initialisation
		beforeFirst();
	}
	
	public void beforeFirst() {
		outerTs.beforeFirst();
		rid_prevBlock = outerTs.getRid();
		innerTs.beforeFirst();
		currRecordInBlock = -1;
	}
	
	public boolean next() {
		
		//init pointers for outer block and next booleans for inner and outer blocks
		boolean hasNextOuter = outerTs.next();
		boolean hasNextInner;
		currRecordInBlock = (currRecordInBlock + 1) % blockSize;
		
		//iterator
		//break out of loop if:
		//  a) there are no next outer block
		//  b) there are no next inner block
		while (true) {
			
			//first iter condition
			if (currRecordInBlock == 0 || hasNextOuter == false) {
				//init pointer for inner block
				hasNextInner = innerTs.next();
				
				if (hasNextInner == false && hasNextOuter == false) { //terminating condition
					return false; 
				} else {
					if (hasNextInner == false) {
						//shift down pointer to first inner record
						innerTs.beforeFirst();
						innerTs.next();
						
						//shift forward pointer to next outer block
						outerTs.moveToRid(rid_prevBlock);
						while (currRecordInBlock < blockSize) {
							if (!outerTs.next()) {
								return false;
							}
							currRecordInBlock++;
			            }
						currRecordInBlock = 0;
						rid_prevBlock = outerTs.getRid();
						outerTs.next();
					} else {
						//reset pointer to beginning of block if there is a next inner block
						outerTs.moveToRid(rid_prevBlock);
						currRecordInBlock = 0;
						outerTs.next();
					}
				}
			}
			
			//fulfilling query conditions
			while (currRecordInBlock < blockSize) {
				if (outerTs.getVal(fldnameInner).equals(innerTs.getVal(fldnameOuter))) {
					return true;
				}
				
				hasNextOuter = outerTs.next();
				if (hasNextOuter == false) {
					break;
				}
				
				currRecordInBlock++;
			}
			
			//re-init after loop
			currRecordInBlock = 0;			
		}
	}
	
	public int getInt(String fldname) {
		if (innerTs.hasField(fldname)) {
			return innerTs.getInt(fldname);
		} else {
			return outerTs.getInt(fldname);
		}
	}
	
	public Constant getVal(String fldname) {
		if (innerTs.hasField(fldname)) {
			return innerTs.getVal(fldname);
		} else {
			return outerTs.getVal(fldname);
		}
	}
	
	public String getString(String fldname) {
		if (innerTs.hasField(fldname)) {
			return innerTs.getString(fldname);
		} else {
			return outerTs.getString(fldname);
		}
	}
	
	public boolean hasField(String fldname) {
		return outerTs.hasField(fldname) || innerTs.hasField(fldname);
	}
	
	public void close() {
		innerTs.close();
		outerTs.close();
	}
}

