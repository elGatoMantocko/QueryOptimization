package query;

import parser.AST_Delete;

import global.Minibase;
import heap.HeapFile;
import relop.Tuple;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {

  private String fileName;
  private Predicate[][] predicates;
  private Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
    fileName = tree.getFileName();
    predicates = tree.getPredicates();

    QueryCheck.tableExists(fileName);
    schema = Minibase.SystemCatalog.getSchema(fileName);
    QueryCheck.predicates(schema, predicates);
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HeapFile file = new HeapFile(fileName);
    FileScan scan = new FileScan(schema, file);

    int rowCount = 0;
    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      
      boolean andPasses = true;
      boolean orPasses = false;
      for (Predicate[] andClause : predicates) {
        for (Predicate pred : andClause) {
          orPasses = orPasses || pred.evaluate(t);
        }

        andPasses = andPasses && orPasses;
        orPasses = false;
      }

      if (andPasses) {
        file.deleteRecord(scan.getLastRID());
        rowCount++;
      }
    }

    scan.close();
    System.out.println(rowCount + " row deleted.");
  } // public void execute()
} // class Delete implements Plan
