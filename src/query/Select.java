package query;

import global.Minibase;
import global.SortKey;
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
import java.util.ArrayList;
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

  private HashMap<String, IndexDesc> indexes;
  private HashMap<String, Iterator> iteratorMap;

  private Iterator iter;

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

    this.indexes = new HashMap<String, IndexDesc>();
    this.iteratorMap = new HashMap<String, Iterator>();

    // validate the query input
    for (String table : tables) {
      QueryCheck.tableExists(table);
      Schema tableSchema = Minibase.SystemCatalog.getSchema(table);
      schema = Schema.join(schema, tableSchema);

      for (IndexDesc desc : Minibase.SystemCatalog.getIndexes(table)) {
        indexes.put(desc.columnName.toLowerCase(), desc);
      }

      HeapFile file = new HeapFile(table);
      iteratorMap.put(table, new FileScan(schema, file));
    }

    QueryCheck.predicates(schema, preds);

    for (String column : cols) {
      QueryCheck.columnExists(schema, column);
    }

    for (Map.Entry<String, Iterator> entry : iteratorMap.entrySet()) {
      for (int i = 0; i < preds.length; i++) {
        ArrayList<Predicate> orPreds = new ArrayList<Predicate>();

        for (Predicate pred : preds[i]) {
          Schema tableSchema = Minibase.SystemCatalog.getSchema(entry.getKey());

          if (pred.validate(tableSchema)) {
            if (!orPreds.contains(pred)) {
              orPreds.add(pred);
            }

            // build keyscan on tables with indexes on a row in the pred
            if (indexes.containsKey(((String)pred.getLeft()).toLowerCase())) {
              IndexDesc desc = indexes.get(pred.getLeft());
              HashIndex index = new HashIndex(desc.indexName);
              KeyScan scan = new KeyScan(tableSchema, index, new SearchKey(pred.getRight()), new HeapFile(desc.indexName));
              
              iteratorMap.put(entry.getKey(), scan);
            }
          }
          else { // probably join predicates

          }
        }
        
        // evaluate predicates into a selection iterator here
        if (!orPreds.isEmpty()) {
          Predicate[] predsArr = orPreds.toArray(new Predicate[orPreds.size()]);

          // this will build a selection using the iterator in the map
          //  if the iterator is a selection that means there is at least one and statement in the predicates
          iteratorMap.put(entry.getKey(), new Selection(iteratorMap.get(entry.getKey()), predsArr));
        }
      }
      entry.getValue().explain(0);
    }

    System.out.println(iteratorMap);

    // build the Iterator

  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    // if (explain) {
    //   iter.explain(0);
    // }
    
    // iter.execute();
    // System.out.println("(Not implemented)");

  } // public void execute()

} // class Select implements Plan
