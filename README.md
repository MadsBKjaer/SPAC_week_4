# Week 4: Integrating MySQL with Python
## Before running
Create a .env file with a line like: 
* env_key = "user=your_username,password=your_password,host=your_host,port=your_port"

Example of entry in .env file that allows for connecting to localhost:
* localhost = "user=root,password=*****,host=localhost,port=3306"
* Swap "*****" for you actual password.

## Workflow
1. First iteration was focus on basic functionality.
2. Second iteration is more focus on streamlining the code and writing doc strings with doc tests.

## Future work
* Split class into a Connection class and a database class to make them more manageable, maybe with some inheritance.
* Finish rewriting and writing docs and doctests.
* Make cursor.fetchall(), fetchone() and maybe reset() accessible from class.
* Handle som exceptions when running commands more than once without resetting the database:
  * Duplicate entires in unique columns
  * Multiple primary keys defined.