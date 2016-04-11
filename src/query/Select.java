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
import java.util.Map;

/**
 * Execution plan for selecting tuples.
 */
class Select extends TestablePlan {

  private String[] tables;
  private String[] cols;
  private Predicate[][] preds;
  private Schema schema;
  private boolean explain;

  private Iterator finalIterator;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {

    this.tables = tree.getTables();
    this.preds = tree.getPredicates();
    this.cols = tree.getColumns();
    this.schema = new Schema(0);
    this.explain = tree.isExplain;

    HashMap<String, ArrayList<IndexDesc>> indexes = new HashMap<String, ArrayList<IndexDesc>>();
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
          if (indexes.get(table) == null) {
            indexes.put(table, new ArrayList<IndexDesc>());
            indexes.get(table).add(desc);
          } else {
            indexes.get(table).add(desc);
          }
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

    while (iteratorMap.size() != 1 && predsList.size() != 0) {
      pushSelectionOperator(iteratorMap, indexes, predsList);
      pushJoinOperator(iteratorMap, predsList);
    }

    List<Map.Entry<String, Iterator>> entries = new ArrayList<>();
    entries.addAll(iteratorMap.entrySet());
    if (fieldNums.length > 0) {
      finalIterator = new Projection(entries.get(0).getValue(), fieldNums);
    } else {
      finalIterator = entries.get(0).getValue();
    }

    // explaining for testing purposes
    // finalIterator.explain(0);
    setFinalIterator(finalIterator);
  } // public Select(AST_Select tree) throws QueryException

  private void pushJoinOperator(HashMap<String, Iterator> iteratorMap, ArrayList<Predicate[]> predsList) {
    // build the finalIterator by determining join order of the iteratorMap
    String[] fileNames = iteratorMap.keySet().toArray(new String[iteratorMap.size()]);
    // for each table we have to see what the join cost is for every other table
    String[] joinToDo = new String[2];
    float costOfJoin = Float.MAX_VALUE;

    Predicate[] predToJoinOn = null;
    float costOfJoinPred = Float.MAX_VALUE;

    for (int i = 0; i < fileNames.length; i++) {
      // this returns an iterator of all of the indexes on the left table
      //  with info about where it is in the table
      int leftCount = Minibase.SystemCatalog.getRecCount(fileNames[i]);
      Schema leftSchema = Minibase.SystemCatalog.getSchema(fileNames[i]);
      for (int j = i + 1; j < fileNames.length; j++) {
        // this returns an iterator of all of the indexes on the right table
        //  with info about where it is in the table
        int rightCount = Minibase.SystemCatalog.getRecCount(fileNames[j]);
        Schema rightSchema = Minibase.SystemCatalog.getSchema(fileNames[j]);

        Schema joinedSchema = Schema.join(leftSchema, rightSchema);
        // for each of the or candidates for a join predicate
        //  we need to determine which predicate works best with this particular join
        for (Predicate[] candidate : predsList) {
          boolean valid = true;
          int reduction = 1; // for now just assume cross
          for (Predicate pred : candidate) {
            // check that all of the or predicats are valid on the current join
            if (valid = valid && pred.validate(joinedSchema)) {
              // first see if there is an index on any col in the pred
              if ((pred.getLtype() == AttrType.COLNAME || pred.getLtype() == AttrType.FIELDNO) && 
                  (pred.getRtype() == AttrType.COLNAME || pred.getRtype() == AttrType.FIELDNO) && 
                  pred.getOper() == AttrOperator.EQ) {
                // find the largest reduction factor between 10, number of keys on left, and number of keys on right
                reduction = 10;
              } else if ((pred.getLtype() == AttrType.COLNAME || pred.getLtype() == AttrType.FIELDNO) && 
                  !(pred.getRtype() == AttrType.COLNAME || pred.getRtype() == AttrType.FIELDNO) && 
                  pred.getOper() == AttrOperator.EQ) {
                // find the largest reduction factor between 2, number of keys on left, and number of keys on right
                reduction = 10;
              } else if ((pred.getLtype() == AttrType.COLNAME || pred.getLtype() == AttrType.FIELDNO) && 
                  !(pred.getRtype() == AttrType.COLNAME || pred.getRtype() == AttrType.FIELDNO) && 
                  pred.getOper() != AttrOperator.EQ) {
                // find the largest reduction factor between 10, number of keys on left, and number of keys on right
                reduction = 3;
              }
            }
          }

          // all of the or preds passed and we can use it to create a score
          if (valid && ((float)(leftCount * rightCount) / (float)reduction) < costOfJoinPred) {
            predToJoinOn = candidate;
            costOfJoinPred = (float)(leftCount * rightCount) / (float)reduction;
          }
        }

        if (predToJoinOn == null) {
          costOfJoinPred = leftCount * rightCount;
        }

        if (costOfJoinPred < costOfJoin) {
          joinToDo = new String[] { fileNames[i], fileNames[j] };
          costOfJoin = costOfJoinPred;
        }
      }
    }

    SimpleJoin join;
    if (predToJoinOn != null) {
      join = new SimpleJoin(iteratorMap.get(joinToDo[0]), iteratorMap.get(joinToDo[1]), predToJoinOn);
    } else {
      join = new SimpleJoin(iteratorMap.get(joinToDo[0]), iteratorMap.get(joinToDo[1]));
    }
    iteratorMap.put(joinToDo[0] + joinToDo[1], join);

    // need to update the iterator list
    iteratorMap.remove(joinToDo[0]);
    iteratorMap.remove(joinToDo[1]);
  }

  private void pushSelectionOperator(HashMap<String, Iterator> iteratorMap, HashMap<String, ArrayList<IndexDesc>> indexes, ArrayList<Predicate[]> predsList) {
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
            if (indexes.containsKey(entry.getKey())) {
              for (IndexDesc desc : indexes.get(entry.getKey())) {
                if (pred.getLtype() == AttrType.COLNAME && desc.columnName.equals(pred.getLeft())) {
                  HashIndex index = new HashIndex(desc.indexName);
                  Iterator scan;
                  if (pred.getOper() == AttrOperator.EQ) {
                    scan = new KeyScan(tableSchema, index, new SearchKey(pred.getRight()), new HeapFile(desc.tableName));
                  } else {
                    scan = new IndexScan(tableSchema, index, new HeapFile(desc.tableName));
                  }
                  
                  iteratorMap.get(entry.getKey()).close();
                  iteratorMap.put(entry.getKey(), scan);
                }
              }
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
  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    if (explain) {
      finalIterator.explain(0);
      finalIterator.close();
    } else {
      finalIterator.execute();
    }
  } // public void execute()

} // class Select implements Plan

class TableData extends Object {
  private ArrayList<String> tables;
  protected Schema schema;
  protected float cost;

  public TableData(String table, float size) {
    this.tables = new ArrayList<>();
    this.tables.add(table);
    
    this.schema = Minibase.SystemCatalog.getSchema(table);

    this.cost = size;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TableData && 
      Arrays.deepEquals(this.getTables(), ((TableData)o).getTables()) &&
      this.schema.equals(((TableData)o).schema) &&
      this.cost == ((TableData)o).cost;
  }

  public void addTable(String table, float updatedCost) {
    tables.add(table);
    schema = Schema.join(schema, Minibase.SystemCatalog.getSchema(table));
    cost = updatedCost;
  }

  public String[] getTables() {
    return tables.toArray(new String[tables.size()]);
  }
}

