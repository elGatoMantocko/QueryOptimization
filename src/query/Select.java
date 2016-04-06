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
    for (int i = 0; i < fileNames.length; i++) {
      for (int j = i + 1; j < fileNames.length; j++) {
        System.out.println("compute cost of join " + fileNames[i] + " " + fileNames[j]);

      }
    }

    // explaining for testing purposes
    // finalIterator.explain(0);
    setFinalIterator(finalIterator);
  } // public Select(AST_Select tree) throws QueryException

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
