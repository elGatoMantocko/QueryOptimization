package query;

import global.Minibase;
import global.SortKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.*;

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

    for (String table : tables) {
      QueryCheck.tableExists(table);
      schema = Schema.join(schema, Minibase.SystemCatalog.getSchema(table));
    }

    QueryCheck.predicates(schema, preds);

    for (String column : cols) {
      QueryCheck.columnExists(schema, column);
    }

  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Select implements Plan
