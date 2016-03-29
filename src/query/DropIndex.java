package query;

import parser.AST_DropIndex;
import global.Minibase;
import index.HashIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

  private String fileName;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index doesn't exist
   */
  public DropIndex(AST_DropIndex tree) throws QueryException {
    QueryCheck.indexExists(fileName = tree.getFileName());
  } // public DropIndex(AST_DropIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    new HashIndex(fileName).deleteFile();
    Minibase.SystemCatalog.dropIndex(fileName);

    System.out.println("Index dropped.");
  } // public void execute()

} // class DropIndex implements Plan
