package tests;

import global.Minibase;

import parser.AST_Start;
import parser.MiniSql;
import parser.SimpleCharStream;
import parser.MiniSqlTokenManager;
import parser.ParseException;
import parser.TokenMgrError;
import query.Optimizer;
import query.Plan;
import query.QueryException;

import org.junit.Before;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class MinibaseTest {

  /** Default database file name. */
  protected static String PATH = System.getProperty("user.name") + ".junit.minibase";

  /** Default database size (in pages). */
  protected static int DB_SIZE = 10000;

  /** Default buffer pool size (in pages) */
  protected static int BUF_SIZE = 100;
  
  /**Default prefetch size */
  protected static int PRE_SIZE = 10;

  /** Command line prompt, when interactive. */
  protected static String PROMPT = "\nMSQL> ";

  private int allocs;
  private int reads;
  private int writes;

  @Before
  public void setUp() {
    // print the welcome message and load the database
    System.out.println("Minibase SQL Utility 1.0");
    if (new File(PATH).exists()) {
      // this likely wont happen because tearDown destroys the DB
      System.out.println("Loading database...");
      new Minibase(PATH, DB_SIZE, BUF_SIZE, PRE_SIZE, "Clock", true);
    } else {
      System.out.println("Creating database...");
      new Minibase(PATH, DB_SIZE, BUF_SIZE, PRE_SIZE, "Clock", false);
    }

    // initialize the performance counters
    allocs = Minibase.DiskManager.getAllocCount();
    reads = Minibase.DiskManager.getReadCount();
    writes = Minibase.DiskManager.getWriteCount();
  }

  @After
  public void tearDown() {
    // close the database and exit
    System.out.println("Destroying database...");
    Minibase.DiskManager.destroyDB();
    Minibase.DiskManager.closeDB();
  }

  @Test
  public void testCreateIndex() {
    // TODO: Verify that the index has been created
    String query = "CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;";
    MiniSql parser;
    AST_Start node;
    SimpleCharStream stream;

    try {
      stream = new SimpleCharStream(new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)), "UTF-8");
    } catch(UnsupportedEncodingException e){
      System.out.println("Encoding not supported.");
      e.printStackTrace();

      stream = new SimpleCharStream(new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)));
    } 
    
    parser = new MiniSql(new MiniSqlTokenManager(stream));

    while (true) {
      try {
        node = parser.Start();
        System.out.println();

        if (node.isQuit) {
          break;
        }

        Plan plan = Optimizer.evaluate(node);
        plan.execute();
      } catch (TokenMgrError err) {
        System.out.println("\nERROR: " + err.getMessage());
        parser.ReInit(new MiniSqlTokenManager(stream));
      } catch (ParseException exc) {
        System.out.print("\nERROR: " + exc.getMessage());
        if (exc.currentToken.kind == 0) {
          System.out.println();
        }
        parser.ReInit(new MiniSqlTokenManager(stream));
      } catch (QueryException exc) {
        System.out.println("ERROR: " + exc.getMessage());
      } catch (RuntimeException exc) {
        exc.printStackTrace();
        System.out.println();
      }
    }
  }

  @Test @Ignore
  public void testDropIndex() {
    // TODO: Set system.in to some InputStream and send that to the parser
    // TODO: Create a table and create an index on that table (similar to queries.sql)
    // TODO: Drop that index
    // TODO: Verify that the index has been dropped
  }
}
