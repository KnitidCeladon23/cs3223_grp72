package simpledb.materialize;

import java.util.*;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>sort</i> operator.
 *
 * @author Edward Sciore
 */
public class DistinctPlan implements Plan {
    private Transaction tx;
    private Plan p;
    private Schema sch;
    private RecordComparator comp;

    private Map<String, Map<Constant, Boolean>> duplicateTracker;
    private List<String> distinct;

    /**
     * Create a sort plan for the specified query.
     *
     * @param tx         the calling transaction
     * @param p          the plan for the underlying query
     * @param sortfields the fields to sort by
     */

    public DistinctPlan(Transaction tx, Plan p, List<String> distinct) {
        this.tx = tx;
        this.p = p;
        sch = p.schema();
        this.distinct = distinct;
        this.duplicateTracker = new HashMap<String, Map<Constant, Boolean>>();
        comp = new RecordComparator(distinct);
    }

    /**
     * This method is where most of the action is. Up to 2 sorted temporary tables
     * are created, and are passed into SortScan for final merging.
     *
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan s = p.open();
        List<TempTable> runs = splitIntoRuns(s);
        s.close();

        if (runs.size() == 1) {
            runs.add(removeDupes(runs.get(0)));
        }
        while (runs.size() > 1) {
            runs = mergeIter(runs);
        }
        return runs.get(0).open();
    }

    private List<TempTable> mergeIter(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() >= 2) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1) {
            result.add(runs.remove(0));
        }
        return result;
    }

    /**
     * @param p1 first run
     * @param p2 second run
     * @return merged run
     */
    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();
        boolean hasNext1 = s1.next();
        boolean hasNext2 = s2.next();
        boolean hasDupes1;
        boolean hasDupes2;

        // Use hash table to check for dupes
        for (String fldname : this.distinct) {
            duplicateTracker.put(fldname, new HashMap<Constant, Boolean>());
        }
        Map<Constant, Boolean> dupesHashMap = null;
        // iterating through runs until there are no more records
        while (hasNext1 && hasNext2) {
            // checking for dupes in both runs
            hasDupes1 = false;
            hasDupes2 = false;
            for (String fldname : distinct) {
                Constant val1 = s1.getVal(fldname);
                Constant val2 = s2.getVal(fldname);
                dupesHashMap = duplicateTracker.get(fldname);
                hasDupes1 = dupesHashMap.containsKey(val1);
                hasDupes2 = dupesHashMap.containsKey(val2);
            }

            // Case 1: neither runs has dupes
            if (!hasDupes1 && !hasDupes2) {
                boolean valueIsDistinct = comp.compareDistinct(s1, s2);
                if (comp.compare(s1, s2) < 0) {
                    stash(s1);
                    hasNext1 = copy(s1, dest);
                    if (!valueIsDistinct) {
                        hasNext2 = s2.next();
                    }
                } else {
                    stash(s2);
                    hasNext2 = copy(s2, dest);
                    if (!valueIsDistinct) {
                        hasNext1 = s1.next();
                    }
                }
            } else if (!hasDupes1) { // Case 2: run 2 has dupes
                stash(s1);
                hasNext1 = copy(s1, dest);
                hasNext2 = s2.next();
            } else if (!hasDupes2) { // Case 3: run 1 has dupes
                stash(s2);
                hasNext2 = copy(s2, dest);
                hasNext1 = s1.next();
            } else { // Case 4: both runs have dupes
                hasNext1 = s1.next();
                hasNext2 = s2.next();
            }
        }

        // iterating through run 1 only because run 2 is out of records
        while (hasNext1) {
            boolean hasDupes = false;
            for (String fldname : distinct) {
                Constant val = s1.getVal(fldname);
                dupesHashMap = duplicateTracker.get(fldname);
                if (dupesHashMap.containsKey(val)) {
                    hasDupes = true;
                    hasNext1 = s1.next();
                    break;
                }
            }
            if (!hasDupes) {
                stash(s1);
                hasNext1 = copy(s1, dest);
            }
        }

        // iterating through run 2 only because run 1 is out of records
        while (hasNext2) {
            boolean hasDupes = false;
            for (String fldname : distinct) {
                Constant val = s2.getVal(fldname);
                dupesHashMap = duplicateTracker.get(fldname);
                if (dupesHashMap.containsKey(val)) {
                    hasDupes = true;
                    hasNext2 = s2.next();
                    break;
                }
            }
            if (!hasDupes) {
                stash(s2);
                hasNext2 = copy(s2, dest);
            }
        }

        s1.close();
        s2.close();
        dest.close();
        return result;
    }

    private TempTable removeDupes(TempTable p) {
        // Initialize the hash table, in order to keep track duplicates.
        for (String fldname : this.distinct) {
            duplicateTracker.put(fldname, new HashMap<Constant, Boolean>());
        }
        Scan s = p.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();
        boolean hasNext = s.next(), hasDupes = false;
        Map<Constant, Boolean> dupesHashMap = null;

        while (hasNext) {
            // Check if any is duplicated
            for (String fldname : this.distinct) {
                // Get the hash map
                dupesHashMap = duplicateTracker.get(fldname);
                Constant val = s.getVal(fldname);
                if (dupesHashMap.containsKey(val)) {
                    hasDupes = true;
                    break;
                }
            }
            if (!hasDupes) {
                stash(s);
                hasNext = copy(s, dest);
            } else {
                hasNext = s.next();
            }
        }
        s.close();
        dest.close();
        return result;
    }

    private void stash(Scan s) {
        Map<Constant, Boolean> dupesHashMap = null;
        for (String fldname : distinct) {
            // Get the hash map
            dupesHashMap = duplicateTracker.get(fldname);
            Constant val = s.getVal(fldname);
            dupesHashMap.put(val, true);
        }
    }

    /**
     * Return the number of blocks in the sorted table, which is the same as it
     * would be in a materialized table. It does <i>not</i> include the one-time
     * cost of materializing and sorting the records.
     *
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    /**
     * Return the number of records in the sorted table, which is the same as in the
     * underlying query.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Return the number of distinct field values in the sorted table, which is the
     * same as in the underlying query.
     *
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Return the schema of the sorted table, which is the same as in the underlying
     * query.
     *
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRuns(Scan s) {
        List<TempTable> tt = new ArrayList<>();
        TempTable curr = new TempTable(tx, sch);

        s.beforeFirst();
        if (!s.next()) {
            return tt;
        }

        tt.add(curr);
        UpdateScan currentscan = curr.open();
        while (copy(s, currentscan)) {
            if (comp.compare(s, currentscan) < 0) {
                currentscan.close();
                curr = new TempTable(tx, sch);
                tt.add(curr);
                currentscan = (UpdateScan) curr.open();
            }
        }
        currentscan.close();
        return tt;
    }

    private boolean copy(Scan s, UpdateScan us) {
        us.insert();
        for (String fldname : sch.fields())
            us.setVal(fldname, s.getVal(fldname));
        return s.next();
    }
}
