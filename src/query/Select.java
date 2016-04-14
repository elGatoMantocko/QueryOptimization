package query;

import global.*;
import heap.HeapFile;
import parser.AST_Select;
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
    TableData[] tables = iteratorMap.keySet().toArray(new TableData[iteratorMap.keySet().size()]);

    int[] tablesToJoin = null;
    int costOfJoin = Integer.MAX_VALUE;

    int bestPredScore = Integer.MIN_VALUE;
    Predicate[] predToJoinOn = null;

    for (int i = 0; i < tables.length; i++) {
      for (int j = i + 1; j < tables.length; j++) {
        TableData joinedData = TableData.join(tables[i], tables[j]);

        if (joinedData.cost < costOfJoin) {
          costOfJoin = joinedData.cost;
          tablesToJoin = new int[] { i, j };
          for (Predicate[] candidate : predsList) {
            boolean validCandidate = true;
            int score = 0;

            for (Predicate p : candidate) {
              validCandidate = validCandidate && p.validate(joinedData.schema);
              if (p.getOper() == AttrOperator.EQ && p.getLtype() == AttrType.COLNAME && p.getRtype() == AttrType.COLNAME) {
                score++;
              }
            }

            if (validCandidate && score > bestPredScore) {
              predToJoinOn = candidate;
              bestPredScore = score;
            }
          }
        }
      }
    }

    if (tablesToJoin == null) {
      throw new RuntimeException("We should have found some tables to join");
    } else {
      int i = tablesToJoin[0];
      int j = tablesToJoin[1];
      SimpleJoin join;
      if (predToJoinOn != null) {
        join = new SimpleJoin(
            iteratorMap.get(tables[i]),
            iteratorMap.get(tables[j]), 
            predToJoinOn
        );
      } else {
        join = new SimpleJoin(
            iteratorMap.get(tables[i]),
            iteratorMap.get(tables[j])
        );
      }

      predsList.remove(predToJoinOn);

      iteratorMap.remove(tables[i]);
      iteratorMap.remove(tables[j]);

      iteratorMap.put(TableData.join(tables[i], tables[j]), join);
    }
  }

  private void pushSelectionOperator(HashMap<TableData, Iterator> iteratorMap, HashMap<String, ArrayList<IndexDesc>> indexes, ArrayList<Predicate[]> predsList) {
    // for each table being joined
    for (TableData key : iteratorMap.keySet()) {
      // save the schema for this table
      Schema tableSchema = key.schema;
      ArrayList<Predicate[]> pListCopy = (ArrayList<Predicate[]>)predsList.clone();

      for (Predicate[] candidate : pListCopy) {
        boolean canPushSelect = true;

        for (Predicate p : candidate) {
          canPushSelect = canPushSelect && p.validate(tableSchema);
        }

        if (canPushSelect) {
          predsList.remove(candidate);
          iteratorMap.put(key, new Selection(iteratorMap.get(key), candidate));
        }
      }
    }
  } // push selections

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
  protected int cost;

  public TableData(String table) {
    this.tables = new ArrayList<>();
    this.tables.add(table);
    
    this.schema = Minibase.SystemCatalog.getSchema(table);

    this.cost = Minibase.SystemCatalog.getRecCount(table);
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
      new Integer(this.cost).equals(new Integer(d.cost));
  }

  @Override
  public int hashCode() {
    int result = 7;
    for (String table : tables) {
      result += table.hashCode();
    }
    result += schema.hashCode();
    result += new Integer(cost).hashCode();

    return 37 * result;
  }
  
  private void addTable(String table) {
    tables.add(table);
    schema = Schema.join(schema, Minibase.SystemCatalog.getSchema(table));
  }

  public void updateCost(int cost) {
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

