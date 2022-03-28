package simpledb.materialize;

import simpledb.multibuffer.BufferNeeds;
import simpledb.multibuffer.ChunkScan;
import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class NestedLoopJoinScan implements Scan {
	
	private Scan inner;
	private ChunkScan outer;

	private Layout layout;
	private Predicate pred;
	private Transaction tx;
	private String filename;
	private int chunksize, nextblknum, filesize;
	
	public NestedLoopJoinScan(Transaction tx, Scan inner, String fldname1, TempTable tt, String fldname2, Predicate pred) {
		this.tx = tx;
		this.pred = pred;
		this.inner = inner;
		this.filename = tt.tableName() + ".tbl";
		this.filesize = tx.size(this.filename);
		this.layout   = tt.getLayout();
		int available = tx.availableBuffs();
		this.chunksize = BufferNeeds.bestFactor(available, filesize);
		
		//initialisation
		beforeFirst();
	}
	
	public void beforeFirst() {
		nextblknum = 0;
		useNextChunk();
	}

	private boolean useNextChunk() {
		if (nextblknum >= filesize)
			return false;
		if (outer != null)
			outer.close();
		int end = nextblknum + chunksize - 1;
		if (end >= filesize)
			end = filesize - 1;
		this.outer = new ChunkScan(tx, filename, layout, nextblknum, end);
		inner.beforeFirst();
		outer.beforeFirst();
		outer.next();
		nextblknum = end + 1;
		return true;
	}
	
	public boolean next() {
		
		boolean innerHasMore = inner.next();
		
		// It was at the last record.
		if(!innerHasMore) {
			// Check if Outer block still has next record.
			if(outer.next()) {
				inner.beforeFirst();
				innerHasMore = inner.next();
			}
			// Else, check if there's next chunk.
			else if(useNextChunk())			
				innerHasMore = inner.next();
		}

		while(innerHasMore) {
			// Inner Scan matches with Outer Scan.
			if(pred.isSatisfied(this))
				return true;
			// Inner scan reaches the end.
			if (!inner.next()) {
				// Moves Outer Scan + Move back Inner Scan.
				if(outer.next()) 
					inner.beforeFirst();
				// End of Outer Scan + No next block -> Return false.
				else if(!useNextChunk()) 
					return false;
				innerHasMore = inner.next();
			}
		}
		// No next Outer record, chunk and inner is at the end.
		return false;
	}
	
	public int getInt(String fldname) {
		if (inner.hasField(fldname))
			return inner.getInt(fldname);
		else
			return outer.getInt(fldname);
	}
	
	public Constant getVal(String fldname) {
		if (inner.hasField(fldname))
			return inner.getVal(fldname);
		else
			return outer.getVal(fldname);
	}
	
	public String getString(String fldname) {
		if (inner.hasField(fldname))
			return inner.getString(fldname);
		else
			return outer.getString(fldname);
	}
	
	public boolean hasField(String fldname) {
		return inner.hasField(fldname) || outer.hasField(fldname);
	}
	
	public void close() {
		inner.close();
		outer.close();
	}
}

