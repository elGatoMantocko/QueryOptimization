# David and Elliott's Query Optimizer

Our query optimizer is designed with a modified left deep join in mind. Tables are sorted and joined together in size order from smallest to greatest to avoid rapid growth. In addition, we first join all tables with a usable index first (in size order) before joining the rest.  As records are inserted and deleted, we maintain statistics on the number of records in the relation and be sure to update the appropriate index if one exists. Similarly, when records are updated, the values in the index would need to be updated appropriately as well.


## Building and Running

Our project includes necessary dependencies to run junit tests as well as running all queries in the `queries.sql` file. 

To run the commands in the `queries.sql` file, simply run `make`. In order to run your tests, replace the `queries.sql` file. If necessary, the Makefile can be changed to run whatever other files you need, but we recommend simply replacing the `queries.sql`.

In addition, we have JUnit support. The jars for junit (and its dependencies) are included in the lib folder. To run out Junit test suite use `make test`. 

If you would like to run the db interactively, `make interactive` can be used. 

`make clean` is also available if needed. 

## Pitfalls

Our database has a few sore spots. Mainly, if three tables of the same size are joined, it is possible that a cross join will occur if the tables are listed in the incorrect order. For example joining A, B, C where A relates to B and B relates to C, it is possible that A and C will be crossjoined rather than joining A to B then to C. However this is unlikely in practice. 
