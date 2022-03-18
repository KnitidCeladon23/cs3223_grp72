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
        Scan src = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();

        if (runs.size() == 1) runs.add(removeDuplicates(runs.get(0)));
        while (runs.size() > 1) runs = doAMergeIteration(runs);

        return runs.get(0).open();
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

    private void save(Scan s) {
        Map<Constant, Boolean> temp = null;
        for (String attribute : distinct) {
            temp = duplicateTracker.get(attribute);
            temp.put(s.getVal(attribute), true);
        }
    }

    private TempTable removeDuplicate(TempTable p) {
        for (String s : this.distinct) {
            duplicateTracker.put(s, new HashMap<Constant, Boolean>());
        }
        Scan src = p.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();
        boolean hasmore = src.next(), srccheck = false;
        Map<Constant, Boolean> temp = null;

        while (hasmore) {
            for (String s : distinct) {
                temp = duplicateTracker.get(s);
                Constant val = src.getVal(s);
                if (temp.containsKey(val)) {
                    srccheck = true;
                    break;
                }
            }
            if (!srccheck) {
                save(src);
                hasmore = copy(src, dest);
            } else {
                hasmore = src.next();
            }
        }
        src.close();
        dest.close();
        return result;
    }
    
    private List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        if (!src.next()) return temps;
        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan)) if (comp.compare(src, currentscan) < 0) {
            currentscan.close();
            currenttemp = new TempTable(tx, sch);
            temps.add(currenttemp);
            currentscan = (UpdateScan) currenttemp.open();
        }
        currentscan.close();
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1) {
            result.add(runs.get(0));
        }
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();
        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();

        boolean src1check, src2check;
        for (String s : this.distinct)
            duplicateTracker.put(s, new HashMap<Constant, Boolean>());
        
        Map<Constant, Boolean> temp = null;

        while (hasmore1 && hasmore2) {
            src1check = false;
            src2check = false;
            for (String s : distinct) {
                temp = duplicateTracker.get(s);
                if (temp.containsKey(src1.getVal(s))) src1check = true;
                if (temp.containsKey(src2.getVal(s))) src2check = true;
            }
            if (!src1check && !src2check) {
                boolean noDuplicates = comp.compareDistinct(src1, src2);
                if (comp.compare(src1, src2) < 0) {
                    save(src1);
                    hasmore1 = copy(src1, dest);
                    if (!noDuplicates) hasmore2 = src2.next();
                } else {
                    save(src2);
                    hasmore2 = copy(src2, dest);
                    if (!noDuplicates) hasmore1 = src1.next();
                }
            } else if (!src1check) {
                save(src1);
                hasmore1 = copy(src1, dest);
                hasmore2 = src2.next();
            } else if (!src2check) {
                save(src2);
                hasmore2 = copy(src2, dest);
                hasmore1 = src1.next();
            } else {
                hasmore1 = src1.next();
                hasmore2 = src2.next();
            }
        }

        if (hasmore1) while (hasmore1) {
            boolean srccheck = false;
            for (String s : distinct) {
                temp = duplicateTracker.get(s);
                Constant val = src1.getVal(s);
                if (temp.containsKey(val)) {
                    srccheck = true;
                    hasmore1 = src1.next();
                    break;
                }
            }
            if (!srccheck) {
                save(src1);
                hasmore1 = copy(src1, dest);
            }
        }
        else while (hasmore2) {
            boolean srccheck = false;
            for (String s : distinct) {
                temp = duplicateTracker.get(s);
                Constant val = src2.getVal(s);
                if (temp.containsKey(val)) {
                    srccheck = true;
                    hasmore2 = src2.next();
                    break;
                }
            }
            if (!srccheck) {
                save(src1);
                hasmore2 = copy(src2, dest);
            }
        }
        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
        return src.next();
    }
}
