package query;

import global.Minibase;
import global.SortKey;
import global.AttrType;
import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Projection;
import relop.Selection;
import relop.SimpleJoin;
import relop.Predicate;
import relop.Schema;

import java.util.ArrayList;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  private ArrayList<String> joinList;

  private String[] tables;
  private String[] cols;
  private SortKey[] orders;
  private Predicate[][] preds;
  private Schema schema;
  private boolean explain;
  private boolean distinct;

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

    this.joinList = new ArrayList<String>();

    // validate the query input
    for (String table : tables) {
      QueryCheck.tableExists(table);
      Schema tableSchema = Minibase.SystemCatalog.getSchema(table);
      schema = Schema.join(schema, tableSchema);

      // building list of tables to push selection on
      for (Predicate[] orPreds : preds) {
        for (Predicate pred : orPreds) {
          if ((pred.getLtype() == 21 && tableSchema.fieldNumber((String)pred.getLeft()) != -1 || 
              pred.getRtype() == 21 && tableSchema.fieldNumber((String)pred.getRight()) != -1) && 
              !joinList.contains(table)) { // colname
            joinList.add(table);
          }
        }
      }
    }

    QueryCheck.predicates(schema, preds);

    for (String column : cols) {
      QueryCheck.columnExists(schema, column);
    }

    // build the Iterator

  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    if (explain) {
      iter.explain(0);
    }
    
    iter.execute();
    // System.out.println("(Not implemented)");

  } // public void execute()

} // class Select implements Plan
