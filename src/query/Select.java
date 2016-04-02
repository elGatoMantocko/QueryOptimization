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
  private SortKey[] orders;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {

    this.tables = tree.getTables();
    this.orders = tree.getOrders();

    for (String table : tables) {
      QueryCheck.tableExists(table);
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
