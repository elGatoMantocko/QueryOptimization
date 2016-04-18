package query;

import parser.AST_Update;

import global.Minibase;
import global.SearchKey;
import index.HashIndex;
import heap.HeapFile;
import relop.Tuple;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

  private String fileName;
  private Predicate[][] predicates;
  private String[] cols;
  private Object[] values;
  private int[] fieldnos;
  private Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {
    this.fileName = tree.getFileName();
    this.predicates = tree.getPredicates();
    this.cols = tree.getColumns();
    this.values = tree.getValues();
    this.schema = Minibase.SystemCatalog.getSchema(fileName);

    fieldnos = new int[values.length];

    QueryCheck.tableExists(fileName);
    QueryCheck.predicates(schema, predicates);
    for (int i = 0; i < cols.length; i++) {
      QueryCheck.columnExists(schema, cols[i]);
      fieldnos[i] = schema.fieldNumber(cols[i]);
    }

    QueryCheck.updateFields(schema, cols);
    QueryCheck.updateValues(schema, fieldnos, values);
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HeapFile file = new HeapFile(fileName);
    FileScan scan = new FileScan(schema, file);

    int rowCount = 0;
    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      Tuple updatedTup = new Tuple(schema, t.getData().clone());

      // first find the tuple to update
      boolean andPasses = true;
      boolean orPasses = false;
      for (Predicate[] andClause : predicates) {
        for (Predicate pred : andClause) {
          orPasses = orPasses || pred.evaluate(t);
        }

        andPasses = andPasses && orPasses;
        orPasses = false;
      }

      // if the tuple passes the predicates update it
      if (andPasses) {
        // for each of the columns update the field
        for (int i = 0; i < cols.length; i++) {
          updatedTup.setField(cols[i], values[i]);
        }

        file.updateRecord(scan.getLastRID(), updatedTup.getData());

        for (IndexDesc desc : Minibase.SystemCatalog.getIndexes(fileName, schema, fieldnos)) {
          HashIndex index = new HashIndex(desc.indexName);
          index.deleteEntry(new SearchKey(t.getField(desc.columnName)), scan.getLastRID());
          index.insertEntry(new SearchKey(updatedTup.getField(desc.columnName)), scan.getLastRID());
        }
        rowCount++;
      }
    }

    scan.close();
    // print the output message
    System.out.println(rowCount + " rows updated.");

  } // public void execute()

} // class Update implements Plan
