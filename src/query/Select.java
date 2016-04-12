package query;

import global.*;
import heap.HeapFile;
import parser.AST_Select;
import index.HashIndex;
import relop.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Execution plan for selecting tuples.
 */
class Select extends TestablePlan {

  private String[] tables;
  private String[] cols;
  private Predicate[][] preds;
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
    this.explain = tree.isExplain;

    HashMap<String, ArrayList<IndexDesc>> indexes = new HashMap<String, ArrayList<IndexDesc>>();
    HashMap<TableData, Iterator> iteratorMap = new HashMap<TableData, Iterator>();
    ArrayList<Predicate[]> predsList = new ArrayList<Predicate[]>();

    for (Predicate[] p : preds) {
      predsList.add(p);
    }

    // check that the predicates are valid
    try {
      // validate the query input
      Schema schemaValidation = new Schema(0);
      for (String table : tables) {
        QueryCheck.tableExists(table);
        Schema tableSchema = Minibase.SystemCatalog.getSchema(table);
        schemaValidation = Schema.join(tableSchema, schemaValidation);

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
        iteratorMap.put(new TableData(table), new FileScan(tableSchema, file));
      }

      QueryCheck.predicates(schemaValidation, preds);

      // have to convert the column names to column numbers
      for (int i = 0; i < cols.length; i++) {
        // validate the column
        QueryCheck.columnExists(schemaValidation, cols[i]);
      }
    } catch(QueryException e){
      for (Iterator i : iteratorMap.values()) {
        i.close();
      }
      throw e;
    }

    while (iteratorMap.size() != 1 || predsList.size() != 0) {
      if (predsList.size() != 0) {
        pushSelectionOperator(iteratorMap, indexes, predsList);
      }

      if (iteratorMap.size() != 1) {
        pushJoinOperator(iteratorMap, predsList);
      }
    }

    List<Map.Entry<TableData, Iterator>> entries = new ArrayList<>();
    entries.addAll(iteratorMap.entrySet());
    Schema finalSchema = entries.get(0).getKey().schema;

    Integer[] fieldNums = new Integer[cols.length];
    for (int i = 0; i < cols.length; i++) {
      fieldNums[i] = finalSchema.fieldNumber(cols[i]);
    }

    if (fieldNums.length > 0) {
      finalIterator = new Projection(entries.get(0).getValue(), fieldNums);
    } else {
      finalIterator = entries.get(0).getValue();
    }

    // explaining for testing purposes
    // finalIterator.explain(0);
    setFinalIterator(finalIterator);
  } // public Select(AST_Select tree) throws QueryException

  private void pushJoinOperator(HashMap<TableData, Iterator> iteratorMap, ArrayList<Predicate[]> predsList) {
    // build the finalIterator by determining join order of the iteratorMap
    TableData[] fileNames = iteratorMap.keySet().toArray(new TableData[iteratorMap.size()]);
    // for each table we have to see what the join cost is for every other table
    TableData[] joinToDo = new TableData[2];
    float costOfJoin = Float.MAX_VALUE;

    Predicate[] predToJoinOn = null;
    float costOfJoinPred;

    for (int i = 0; i < fileNames.length; i++) {
      // this returns an iterator of all of the indexes on the left table
      //  with info about where it is in the table
      for (int j = i + 1; j < fileNames.length; j++) {
        // this returns an iterator of all of the indexes on the right table
        //  with info about where it is in the table
        TableData joinedData = TableData.join(fileNames[i], fileNames[j]);
        costOfJoinPred = joinedData.cost;
        
        // for each of the or candidates for a join predicate
        //  we need to determine which predicate works best with this particular join
        for (Predicate[] candidate : predsList) {
          int reduction = 1; // for now just assume cross

          // dont count reduction for preds that are exactly the same
          List<Predicate> distinctCand = new ArrayList<Predicate>(new HashSet<Predicate>(Arrays.asList(candidate)));
          for (Predicate pred : distinctCand) {
            if (!pred.validate(joinedData.schema)){
              reduction = 1;
              costOfJoinPred = joinedData.cost;
              predToJoinOn = null;
              break;
            }

            if (pred.getLtype() == AttrType.COLNAME && pred.getRtype() == AttrType.COLNAME && pred.getOper() == AttrOperator.EQ) {
              reduction *= 10;
            } else if (pred.getLtype() == AttrType.COLNAME && pred.getRtype() != AttrType.COLNAME && pred.getOper() == AttrOperator.EQ) {
              reduction *= 10;
            } else if (pred.getLtype() == AttrType.COLNAME && pred.getRtype() != AttrType.COLNAME && pred.getOper() != AttrOperator.EQ) {
              reduction *= 3;
            }
          }

          // all of the or preds passed and we can use it to create a score
          if (fileNames[i].cost * fileNames[j].cost / (float)reduction < costOfJoinPred) {
            predToJoinOn = candidate;
            costOfJoinPred = fileNames[i].cost * fileNames[j].cost / (float)reduction;
          }
        }

        if (predToJoinOn == null) {
          costOfJoinPred = fileNames[i].cost * fileNames[j].cost;
        }

        if (costOfJoinPred <= costOfJoin) {
          joinToDo = new TableData[] { fileNames[i], fileNames[j] };
          costOfJoin = costOfJoinPred;
        }
      }
    }

    SimpleJoin join;
    if (predToJoinOn != null) {
      join = new SimpleJoin(iteratorMap.get(joinToDo[0]), iteratorMap.get(joinToDo[1]), predToJoinOn);
      predsList.remove(predToJoinOn);
    } else {
      join = new SimpleJoin(iteratorMap.get(joinToDo[0]), iteratorMap.get(joinToDo[1]));
    }
    TableData bestJoin = TableData.join(joinToDo[0], joinToDo[1]);
    bestJoin.updateCost(costOfJoin);
    iteratorMap.put(TableData.join(joinToDo[0], joinToDo[1]), join);

    // need to update the iterator list
    iteratorMap.remove(joinToDo[0]);
    iteratorMap.remove(joinToDo[1]);
  }

  private void pushSelectionOperator(HashMap<TableData, Iterator> iteratorMap, HashMap<String, ArrayList<IndexDesc>> indexes, ArrayList<Predicate[]> predsList) {
    Predicate[][] remainingPreds = predsList.toArray(new Predicate[predsList.size()][]);
    // for each table being joined
    for (Map.Entry<TableData, Iterator> entry : iteratorMap.entrySet()) {
      // save the schema for this table
      Schema tableSchema = entry.getKey().schema;

      // for all of the and predicates
      for (int i = 0; i < remainingPreds.length; i++) {
        // lets build a list of all of the passable or preds
        boolean canPushSelect = true;

        // for all of the predicates in the or list
        for (Predicate pred : remainingPreds[i]) {
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
          predsList.remove(remainingPreds[i]);
          iteratorMap.put(entry.getKey(), new Selection(iteratorMap.get(entry.getKey()), remainingPreds[i]));
        } else if (iteratorMap.get(entry.getKey()) instanceof KeyScan || iteratorMap.get(entry.getKey()) instanceof IndexScan) {
          // TODO: need to reset back to a filescan
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

  public TableData(String table) {
    this.tables = new ArrayList<>();
    this.tables.add(table);
    
    this.schema = Minibase.SystemCatalog.getSchema(table);

    this.cost = (float)Minibase.SystemCatalog.getRecCount(table);
  }

  private TableData(TableData copy) {
    this.tables = new ArrayList<>();
    this.tables.addAll(Arrays.asList(copy.getTables()));

    this.schema = copy.schema;

    this.cost = copy.cost;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    } else if (!(o instanceof TableData)) {
      return false;
    }

    TableData d = (TableData)o;
    return 
      Arrays.deepEquals(this.getTables(), d.getTables()) &&
      this.schema.equals(d.schema) &&
      new Float(this.cost).equals(new Float(d.cost));
  }

  @Override
  public int hashCode() {
    return 37 * (tables.hashCode() + schema.hashCode() + new Float(cost).hashCode());
  }
  
  private void addTable(String table) {
    tables.add(table);
    schema = Schema.join(schema, Minibase.SystemCatalog.getSchema(table));
  }

  public void updateCost(float cost) {
    this.cost = cost;
  }

  private String[] getTables() {
    return tables.toArray(new String[tables.size()]);
  }

  public static TableData join(TableData left, TableData right) {
    TableData join = new TableData(left);
    for (String table : right.getTables()) {
      join.addTable(table);
    }
    join.updateCost(left.cost * right.cost);
    return join;
  }

  public String toString() {
    return Arrays.deepToString(getTables()) + " " + cost;
  }
}

