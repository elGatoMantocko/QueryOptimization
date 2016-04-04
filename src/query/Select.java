package query;

import global.Minibase;
import global.SortKey;
import global.AttrOperator;
import global.AttrType;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Select;
import index.HashIndex;
import relop.KeyScan;
import relop.FileScan;
import relop.Iterator;
import relop.Projection;
import relop.Selection;
import relop.SimpleJoin;
import relop.Predicate;
import relop.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  private String[] tables;
  private String[] cols;
  private SortKey[] orders;
  private Predicate[][] preds;
  private Schema schema;
  private boolean explain;
  private boolean distinct;

  private Iterator final_iterator;

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
    ArrayList<Predicate> joinPreds = new ArrayList<Predicate>();
    ArrayList<Predicate[]> predsList = new ArrayList<Predicate[]>();

    for (Predicate[] p : preds) {
      predsList.add(p);
    }

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

    // check that the predicates are valid
    QueryCheck.predicates(schema, preds);

    // have to convert the column names to column numbers
    Integer[] fieldNums = new Integer[cols.length];
    for (int i = 0; i < cols.length; i++) {
      // validate the column
      QueryCheck.columnExists(schema, cols[i]);
      fieldNums[i] = schema.fieldNumber(cols[i]);
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
    }

    Iterator[] iters = iteratorMap.values().toArray(new Iterator[iteratorMap.size()]);

    // if there is more than one iterator, we need to make some join(s)
    if (iters.length > 1) {

      HashMap<Predicate[], Integer> score = new HashMap<Predicate[], Integer>();
      for (Predicate[] candidate : predsList) {
        score.put(candidate, new Integer(0));
        for (Predicate pred : candidate) {
          if (pred.getLtype() == AttrType.COLNAME && pred.getRtype() == AttrType.COLNAME && pred.getOper() == AttrOperator.EQ) {
            score.put(candidate, new Integer(score.get(candidate) + 1));
          }
        }
      }

      List<Map.Entry<Predicate[], Integer>> entryList = new ArrayList<>();
      entryList.addAll(score.entrySet());
      Collections.sort(entryList, new Comparator<Map.Entry<Predicate[], Integer>>() {
        @Override
        public int compare(Map.Entry<Predicate[], Integer> left, Map.Entry<Predicate[], Integer> right) {
          return right.getValue() - left.getValue();
        }
      });

      final_iterator = new SimpleJoin(iters[0], iters[1], entryList.get(0).getKey());

      for (int i = 2; i < iters.length; i++) {
        final_iterator = new SimpleJoin(final_iterator, iters[i], entryList.get(0).getKey());
      }
    } else {
      // if its just one, then the final iterator can be set to that iterator
      final_iterator = iters[0];
    }

    // if the query is asking to project columns, build the projection
    if (cols != null && cols.length > 0) {
      final_iterator = new Projection(final_iterator, fieldNums);
    }

    // explaining for testing purposes
    // final_iterator.explain(0);

  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    if (explain) {
      final_iterator.explain(0);
    }
    
    final_iterator.execute();
    // System.out.println("(Not implemented)");

  } // public void execute()

} // class Select implements Plan
