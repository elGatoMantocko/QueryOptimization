# David and Elliott's Query Optimizer

Our query optimizer is designed with a modified left deep join in mind. Tables are sorted and joined together in size order from smallest to greatest to avoid rapid growth. As records are inserted and deleted, we maintain statistics on the number of records in the relation and be sure to update the appropriate index if one exists. Similarly, when records are updated, the values in the index would need to be updated appropriately as well.


## Building and Running

Our project includes necessary dependencies to run junit tests as well as running all queries in the queries.sql file. 

To run the commands in the queries.sql file, simply run `make`. Feel free to place whatever test queries you may have into that file. 

In addition, we have JUnit support. The jars for junit (and its dependencies) are included in the lib folder. To run out Junit test suite use `make test`. 

If you would like to run the db interactively, `make interactive` can be used. 

`make clean` is also available if needed. 

