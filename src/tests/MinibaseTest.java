package tests;

import global.Minibase;

import org.junit.Before;
import org.junit.After;

import java.io.File;

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

  @Before
  public void setUp() throws Exception {
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
  }

  @After
  public void tearDown() {
    // close the database and exit
    System.out.println("Destroying database...");
    Minibase.DiskManager.destroyDB();
    Minibase.DiskManager.closeDB();
  }

}
