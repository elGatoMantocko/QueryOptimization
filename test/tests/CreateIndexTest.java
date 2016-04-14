package tests;

import global.Minibase;
import global.Msql;
import parser.TokenMgrError;
import parser.ParseException;
import query.QueryException;
import query.IndexDesc;

import org.junit.Assert;
import org.junit.Test;

public class CreateIndexTest extends MinibaseTest {

  @Test
  public void testCreateIndex() {
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    boolean passes = false;

    for (IndexDesc desc : Minibase.SystemCatalog.getIndexes("Students")) {
      if (desc.columnName.equals("Age") && desc.indexName.equals("IX_Age")) {
        passes = true;
      }
    }

    Assert.assertTrue("The index was not built on the correct column.", passes);
  }

  @Test (expected=QueryException.class)
  public void testCreateDuplicateNamedIndex() throws QueryException {
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
      Msql.execute("CREATE INDEX IX_Age ON Students(Name);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } 
  }

  @Test (expected=QueryException.class)
  public void testCreateIndexOnBadTable() throws QueryException {
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Grades(GPA);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    }
  }

  @Test (expected=QueryException.class)
  public void testCreateIndexOnBadColumn() throws QueryException{
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(GPA);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    }
  }
}

