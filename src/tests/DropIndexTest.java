package tests;

import global.Minibase;
import global.Msql;
import parser.TokenMgrError;
import parser.ParseException;
import query.QueryException;
import query.IndexDesc;

import org.junit.Assert;
import org.junit.Test;

public class DropIndexTest extends MinibaseTest {
  
  @Test
  public void testDropIndex() {
    try {
      // set up index
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

    try {
      Msql.execute("DROP INDEX IX_Age;\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    for (IndexDesc desc : Minibase.SystemCatalog.getIndexes("Students")) {
      if (desc.columnName.equals("Age") && desc.indexName.equals("IX_Age")) {
        Assert.fail("The index shouldn\'t exist.");
      }
    }
  }

  @Test (expected=QueryException.class)
  public void testDropBadIndex() throws QueryException {
    try {
      Msql.execute("DROP INDEX IX_Age;\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } 
  }

}
