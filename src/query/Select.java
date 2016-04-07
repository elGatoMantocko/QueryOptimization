package query;

import global.*;
import heap.HeapFile;
import parser.AST_Select;
import index.HashIndex;
import relop.*;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

/**
 * Execution plan for selecting tuples.
 */
class Select extends TestablePlan {

  private String[] tables;
  private String[] cols;
  private SortKey[] orders;
  private Predicate[][] preds;
  private Schema schema;
  private boolean explain;
  private boolean distinct;

  private Iterator finalIterator;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {

    this.tables = tree.getTables();
    this.orders = tree.getOrders();
    this.preds = tree.getPredicates();
    this.cols = tree.getColumns();
    this.schema = new Schema(0);
    this.explain = tree.isExplain;
    this.distinct = tree.isDistinct;

    HashMap<String, IndexDesc> indexes = new HashMap<String, IndexDesc>();
    HashMap<String, Iterator> iteratorMap = new HashMap<String, Iterator>();
    ArrayList<Predicate[]> predsList = new ArrayList<Predicate[]>();

    for (Predicate[] p : preds) {
      predsList.add(p);
    }

    Integer[] fieldNums = new Integer[cols.length];
    // check that the predicates are valid
    try {
      // validate the query input
      for (String table : tables) {
        QueryCheck.tableExists(table);
        Schema tableSchema = Minibase.SystemCatalog.getSchema(table);
        schema = Schema.join(tableSchema, schema);

        // this could possibly be bad if there are multiple
        //  indexes with different names that have the same column names
        for (IndexDesc desc : Minibase.SystemCatalog.getIndexes(table)) {
          indexes.put(desc.columnName.toLowerCase(), desc);
        }

        // create a new filescan out of the current table
        HeapFile file = new HeapFile(table);
        iteratorMap.put(table, new FileScan(tableSchema, file));
      }

      QueryCheck.predicates(schema, preds);

      // have to convert the column names to column numbers
      for (int i = 0; i < cols.length; i++) {
        // validate the column
        QueryCheck.columnExists(schema, cols[i]);
        fieldNums[i] = schema.fieldNumber(cols[i]);
      }
    } catch(QueryException e){
      for (Iterator i : iteratorMap.values()) {
        i.close();
      }
      throw e;
    }

    // for each table being joined
    for (Map.Entry<String, Iterator> entry : iteratorMap.entrySet()) {
      // save the schema for this table
      Schema tableSchema = Minibase.SystemCatalog.getSchema(entry.getKey());

      // for all of the and predicates
      for (int i = 0; i < preds.length; i++) {
        // lets build a list of all of the passable or preds
        boolean canPushSelect = true;

        // for all of the predicates in the or list
        for (Predicate pred : preds[i]) {
          // if the predicate is valid for the current table's schema
          //  add it to the list of or predicates to push the selection down
          if (canPushSelect = canPushSelect && pred.validate(tableSchema)) {
            // build keyscan on tables with indexes on a row in the pred
            //  if there is an index on the predicate's left value
            if (indexes.containsKey(((String)pred.getLeft()).toLowerCase())) {
              IndexDesc desc = indexes.get(pred.getLeft());
              HashIndex index = new HashIndex(desc.indexName);
              KeyScan scan = new KeyScan(tableSchema, index, new SearchKey(pred.getRight()), new HeapFile(desc.indexName));
              
              iteratorMap.get(entry.getKey()).close();
              iteratorMap.put(entry.getKey(), scan);
            }
          }
        }
        
        // this will build a selection using the iterator in the map
        //  if the iterator is a selection that means there is at least one and statement in the predicates
        if (canPushSelect) {
          predsList.remove(preds[i]);
          iteratorMap.put(entry.getKey(), new Selection(iteratorMap.get(entry.getKey()), preds[i]));
        }
      }
    } // push selections

    // build the finalIterator by determining join order of the iteratorMap
    System.out.println(iteratorMap);
    String[] fileNames = iteratorMap.keySet().toArray(new String[iteratorMap.size()]);
    // for each table we have to see what the join cost is for every other table
    for (int i = 0; i < fileNames.length; i++) {
      // this returns an iterator of all of the indexes on the left table
      Selection leftIndexes = new Selection(getAllCatsJoined(), new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 1, AttrType.STRING, fileNames[i]));

      int leftCount = Minibase.SystemCatalog.getRecCount(fileNames[i]);
      Schema leftSchema = Minibase.SystemCatalog.getSchema(fileNames[i]);
      for (int j = i + 1; j < fileNames.length; j++) {
        // this returns an iterator of all of the indexes on the right table
        Selection rightIndexes = new Selection(getAllCatsJoined(), new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 1, AttrType.STRING, fileNames[j]));

        int rightCount = Minibase.SystemCatalog.getRecCount(fileNames[j]);
        Schema rightSchema = Minibase.SystemCatalog.getSchema(fileNames[j]);
        System.out.println("compute cost of join " + fileNames[i] + ": " + leftCount + " " + fileNames[j] + ": " + rightCount);

        Schema joinedSchema = Schema.join(leftSchema, rightSchema);
        
        // for each of the or candidates for a join predicate
        //  we need to determine which predicate works best with this particular join
        for (Predicate[] candidate : predsList) {
          boolean valid = true;
          for (Predicate pred : candidate) {
            // check that all of the or predicats are valid on the current join
            if (valid = valid && pred.validate(joinedSchema)) {
              // first see if there is an index on any col in the pred
              // TODO: pick the cost of the best index
              //  have to look at left and right tables seperately
            }
          }

          // all of the or preds passed and we can use it to create a score
          if (valid) {
            // TODO: apply the reduction factor depending if there is an index or not
          }
        }
      }
    }

    // explaining for testing purposes
    // finalIterator.explain(0);
    setFinalIterator(finalIterator);
  } // public Select(AST_Select tree) throws QueryException

  private int indexCostReduction(Iterator tabIndexes, String fileName, Schema schema, Predicate pred) {
    int reduction = 10;
    if (pred.getLtype() == AttrType.COLNAME) {
      // need to reset the left indexes
      tabIndexes.restart();

      // find all indexes on the left colname
      Selection lhsIndexes = new Selection(tabIndexes, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 6, AttrType.STRING, (String)pred.getLeft()));
      if (lhsIndexes.hasNext()) {
        // there was at least one index found for that column
        //  this means that we can apply a much better reduction factor
        // TODO: we need to figure out the number of keys in that index here
      }
      lhsIndexes.close();
    }

    if (pred.getRtype() == AttrType.COLNAME) {
      // need to reset the left indexes
      tabIndexes.restart();

      // find all indexes on the right colname
      Selection rhsIndexes = new Selection(tabIndexes, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 6, AttrType.STRING, (String)pred.getLeft()));
      if (rhsIndexes.hasNext()) {
        // there was at least one index found for that column
        //  this means that we can apply a much better reduction factor
        // TODO: we need to figure out the number of keys in that index here
      }
      rhsIndexes.close();
    }

    return reduction;
  }

  private Iterator getAllCatsJoined() {
    FileScan relScan = new FileScan(Minibase.SystemCatalog.s_rel, Minibase.SystemCatalog.f_rel);
    FileScan attScan = new FileScan(Minibase.SystemCatalog.s_att, Minibase.SystemCatalog.f_att);
    FileScan indScan = new FileScan(Minibase.SystemCatalog.s_ind, Minibase.SystemCatalog.f_ind);

    SimpleJoin relAttJoin = new SimpleJoin(relScan, attScan, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 0, AttrType.FIELDNO, 2));
    // this joined catalog can help find all of the available indexes on a particular table
    SimpleJoin joinAll = new SimpleJoin(indScan, relAttJoin, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO, 9));

    Projection reduceCols = new Projection(joinAll, 0, 1, 4, 6, 7, 8, 9);

    return reduceCols;
  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    if (explain) {
      // finalIterator.explain(0);
      // finalIterator.close();
    } else {
      // finalIterator.execute();
    }


    // System.out.println("(Not implemented)");

  } // public void execute()

} // class Select implements Plan
