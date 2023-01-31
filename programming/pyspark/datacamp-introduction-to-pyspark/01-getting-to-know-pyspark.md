# 1. Getting to know PySpark

&nbsp;

## What is *Spark*?

*Spark* = a *platform* for *cluster computing*.

*Spark* lets you spread *data* and *computations* over clusters with *multiple nodes`*.

> A node = A separate computer.

**Each node only works with a small amount of data** to work with **very large datasets**.

---

&nbsp;

## Using *Spark* in *Python*

Start using *Spark* by connecting to the *cluster*.

In a *cluster* is composed of:
- A **Master** node
    - Manages splitting up the *data* and the *computations*.
    - The **master** sends *data* and *calculations* to run on **worker** nodes, 
- Multiple **Worker** nodes
    - Each **worker** node then send their results back to the **master** node.

In programming, create a *connection to the cluster* by creating an instance of the `SparkContext` class. 

The *class constructor* of the `SparkContext` class takes *a few optional arguments* that allow you to specify *the attributes of the cluster* you're connecting to. 

> An object holding all these attributes can be created with the `SparkConf()` constructor.

> Mostly in Spark environment a `SparkContext` instance is prepared in advance as `sc` in the code or Jupyter notebook.

---

&nbsp;

## Examining The SparkContext

```python
# NOTE: `sc` is a SparkContext instance.

# Verify SparkContext
print(sc)

# Output
# <SparkContext master=local[*] appName=pyspark-shell>

# Print Spark version
print(sc.version)

# Output
#
# 3.2.0
```

---

&nbsp;

## Using DataFrames

**Resilient Distributed Dataset (RDD)** = *Spark*'s core data structure

- = *A low level object* that lets Spark work its magic by splitting data a*cross multiple nodes* in the cluster. 

> However, **RDDs** are *hard to work* with directly, so in this course you'll be using the **Spark DataFrame** instead. It is an *abstraction built on top of RDDs*.

**Spark DataFrame** vs, **Resilient Distributed Dataset (RDD)**
 
- **Spark DataFrame** = easier to understand

- **Spark DataFrame** = more optimized for complicated operations than **RDDs**.

To start working with **Spark DataFrames**, you first have to create a `SparkSession` object from your `SparkContext`. 

- `SparkContext` = connection to the cluster
- `SparkSession` = interface with that connection.

> Mostly in Spark environment a `SparkSession` instance is prepared in advance as `spark` in the code or Jupyter notebook.

---

&nbsp;

## Creating a SparkSession

> NOTE: Creating **multiple** `SparkSessions` and `SparkContexts` can cause issues, so it's best practice to use the `SparkSession.builder.getOrCreate()` method. This returns an existing `SparkSession` if there's already one in the environment, or creates a new one if necessary!

```python
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark if not exist and get it.
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

# Output
#
# <pyspark.sql.session.SparkSession object at 0x7f6412d850a0>
```

---

&nbsp;

## Viewing tables

`SparkSession` instance has an attribute called `catalog` = lists all the data inside the cluster.

`catalog` attribute has a few methods for extracting different pieces of information.

One of the most useful is the `.listTables()` method = returns *the names of all the tables in your cluster* as a list.

```python
# NOTE: `spark` is a SparkSession instance.

# Print the tables in the catalog
print(spark.catalog.listTables())

# Output
#
# [Table(name='flights', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]
```

---

&nbsp;

## Running a query on a table

Running a query on a table is as easy as using the `.sql()` method on your `SparkSession` instance. 

> NOTE: This method takes *a string containing the query* and returns *a DataFrame* with the results!

```python
# NOTE: `spark` is a SparkSession instance.

query = "SELECT * FROM flights LIMIT 10 "

# Get the first 10 rows of flights
flights10 = spark.sql(query)

# Show the results
flights10.show()

# Output
#
# <Show 10 rows in the flights table here>
```

---

&nbsp;

## Convert Spark DataFrame into Pandas DataFrame

*Spark DataFrames* make that easy with the `.toPandas()` method. 

Calling this method on *a Spark DataFrame* returns the corresponding *pandas DataFrame* to work with it **locally** instead of work on the cluster.

```python
# NOTE: `spark` is a SparkSession instance.

query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts.head())

# Output
#
#   origin dest    N
# 0    SEA  RNO    8
# 1    SEA  DTW   98
# 2    SEA  CLE    2
# 3    SEA  LAX  450
# 4    PDX  SEA  144
```

---

&nbsp;

## Convert Pandas DataFrame into Spark DataFrame

The `.createDataFrame()` method of the `SparkSession` instance takes *a pandas DataFrame* and returns *a Spark DataFrame*.

- IMPORTANTLY, the output of `.createDataFrame()` is **stored locally**, NOT in the `SparkSession` `catalog`. This means that you **CAN use all the Spark DataFrame methods on it**, BUT you **CANNOT access the data in other contexts**.

- For example, a SQL query (using the `.sql()` method) that references your DataFrame will *throw an error*. To access the data in this way, you have to save it as *a temporary table*.

You can save the *local Spark DataFrame* as *a temporary table* using the `.createTempView()` *Spark DataFrame* method, which takes as its only argument *the name of the temporary table* you'd like to register. 

-  `.createTempView()` registers the DataFrame as *a table in the catalog*, but as this table is *temporary*, it can **ONLY be accessed from the specific `SparkSession` used to create the Spark DataFrame**.

There is also the method `.createOrReplaceTempView()`. 

- This safely creates a new temporary table if nothing was there before, or updates an existing table if one was already defined. 

- You'll use this method to avoid running into problems with duplicate tables.

```python
# NOTE: `spark` is a SparkSession instance.

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(spark.catalog.listTables())

# Output: Notice that nothing in the catalog.
#
# []

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView('temp')

# Examine the tables in the catalog again
print(spark.catalog.listTables())

# Output: After `.createOrReplaceTempView('temp')`, the temporary table `temp` appear on the catalog of SparkSession.
#
# [Table(name='temp', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]
```

---

&nbsp;

## Read a text file into Spark DataFrame

Your `SparkSession` has a `.read` attribute which has several methods for reading *different data sources* into *Spark DataFrames*.

```python
# NOTE: `spark` is a SparkSession instance.

file_path = "/usr/local/share/datasets/airports.csv"

# Read in the airports data
airports = spark.read.csv(file_path, header=True)

# Show the data
airports.show()

# Output
#
# <A table of airports data with top 20 rows>
```

---

&nbsp;

## Takeaways

`SparkContext` = connection to the cluster

`SparkSession` = interface with that connection.

Create a SparkSession by:

```python
spark = SparkSession.builder.getOrCreate()
```

Print the tables in the catalog by:

```python
print(spark.catalog.listTables())
```

Query the table `flights` in the catalog and print it by:

```python
spark_df = spark.sql("SELECT * FROM flights LIMIT 10")
```

Print the Spark DataFrame into the stdout by:

```python
# Default = Show ONLY first 20 rows
spark_df.show()
```

Convert Spark DataFrame into Pandas DataFrame by:

```python
pd_df = spark_df.toPandas()
```

Convert Pandas DataFrame into *local* Spark DataFrame

```python
# Create pandas DataFrame
pd_df = pd.DataFrame(np.random.random(10))

# Create spark_df Spark DataFrame from pd_df
spark_df = spark.createDataFrame(pd_df)

# NOTE: spark_df is still in local, not on the cluster.
```

Save the *local Spark DataFrame* as *a temporary table* on the SparkSession catalog on the cluster by:

```python
spark_df.createOrReplaceTempView('a_temp_table_name')

# OR

# It will throws `TempTableAlreadyExistsException`, if the view name already exists in the catalog.
spark_df.createTempView('a_temp_table_name')
```

> NOTE: The lifetime of this temporary table is tied to the `SparkSession` that was used to create this Spark DataFrame.

---
