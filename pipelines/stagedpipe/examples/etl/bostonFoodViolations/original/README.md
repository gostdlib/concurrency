## NOTICE: This is originally from https://github.com/353words/food
## NOTICE: You can read the article that goes with the code here: https://www.ardanlabs.com/blog/2021/09/extract-transform-load-in-go.html

## CHANGES FROM ORIGINAL: 
* Erases the table before starting
* Uses postgres vs. SQLite
* If the violdttm column is empty, we do not do a time conversion (leaving a 0 time) and return no error
* You must download the file at https://data.boston.gov/dataset/food-establishment-inspections , `rename it violations.csv`, and put it in the root `bostonFoodViolations/` directory

# Where *Not* to Eat? - ETL in Go
+++
title = "Where *Not* to Eat? - ETL in Go"
date = "FIXME"
tags = ["golang"]
categories = [ "golang" ]
url = "FIXME"
author = "mikit"
+++

You are about to visit Boston, and would like to taste some good food. You ask your friend who lives there what are good places to eat. They reply with "Everything is good, you can't go wrong". Which makes you think, maybe I should check where *not* to eat.

The data geek in you arises, and you find out that the city of Boson has a [dataset of food violations](https://data.boston.gov/dataset/food-establishment-inspections). You download it and decide to have a look.

The data is in CSV format (which you hate). Since you're going to play around with the data, you decide to load this data to an SQL database. Once the data is inside the database, you can dynamically query it with SQL or even use fancy tools such as [Grafana](https://grafana.com/) to visualize the data.

### Interlude: ETL

[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) stands for "Extract, Transform, Load". A lot of data in it's raw format is in files (logs, CSV, JSON ...). And you want to upload this data to a common place (sometimes called [data lake](https://en.wikipedia.org/wiki/Data_lake)) where data scientists can analyze it. 

The three stages of ETL are:

- Extract: Data in logs, CSVs and other formats need to be parsed (or extracted). Sometimes we want only parts of the data.
- Transform: Here we rename field, convert data types, enrich (e.g. [geolocation](https://en.wikipedia.org/wiki/Internet_geolocation)), and more
- Load: Finally, we load the data to its destination.

_Note: Sometimes the order is changed, and we do [ELT](https://en.wikipedia.org/wiki/Extract,_load,_transform). First we extract and load, and then transformations are done in the database._

## First Look at the Data

CSV has many, many faults, but it's easy to look at the data since it's textual. Let's look at the header line.

**Listing 1: First Look** 

```
$ wc -l boston-food.csv 
655317 boston-food.csv
$ head -1 boston-food.csv           
businessname,dbaname,legalowner,namelast,namefirst,licenseno,issdttm,expdttm,licstatus,licensecat,descript,result,resultdttm,violation,viollevel,violdesc,violdttm,violstatus,statusdate,comments,address,city,state,zip,property_id,location
```

In listing 1 we use the `wc` command to see how many lines we have and then use the `head` command to see the first line that contains the column names.

Some names, such as `businessname`, make sense. Some, such as `expdttm` are more cryptic. A quick search online finds [the data description](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/6MUQKX).

_Note: In your company, make sure *every* column/field is documented. I'm getting paid to poke in companies data, and the amount of times they can't explain a field to me is way too big._

After reading the data description, you decide to use only some of the fields and rename some of them.

- `businessname` will become `business_name`
- `licstatus` will become `license_status`
- `violdesc` will become `description`
- `violstatus` will become `status`
- `viollevel` which is either `*`, `**` or `***` will become `level` (1, 2 or 3 - integer)
- `result`, `comments`, `address`, `city` and `zip` will keep their names

You're going to ignore all other fields.

**Listing 2: Database schema**

```
01 CREATE TABLE IF NOT EXISTS violations (
02     business_name TEXT,
03     license_status TEXT,
04     result TEXT,
05     description TEXT,
06     time TIMESTAMP,
07     status TEXT,
08     level INTEGER,
09     comments TEXT,
10     address TEXT,
11     city TEXT,
12     zip TEXT
13 );
```

Listing 2 contains the database schema, which is in `schema.sql`.

### Packages

**Listing 3: go.mod**

```
01 module github.com/353words/food
02 
03 go 1.17
04 
05 require (
06     github.com/jmoiron/sqlx v1.3.4
07     github.com/jszwec/csvutil v1.5.1
08     github.com/mattn/go-sqlite3 v1.14.8
09 )
```

Listing 3 shows the content of `go.mod`. To parse the CSV we'll be using [csvutil](https://pkg.go.dev/github.com/jszwec/csvutil). For the database, we'll use [go-sqlite3](https://pkg.go.dev/github.com/mattn/go-sqlite3) (I *love* [SQLite](https://sqlite.org/) ☺) and [sqlx](https://pkg.go.dev/github.com/jmoiron/sqlx).

## ETL Code

**Listing 4: imports**

```
03 import (
04     _ "embed"
05     "encoding/csv"
06     "fmt"
07     "io"
08     "log"
09     "os"
10     "time"
11 
12     "github.com/jmoiron/sqlx"
13     "github.com/jszwec/csvutil"
14     _ "github.com/mattn/go-sqlite3"
15 )
```

Listing 4 shows our imports. On line 04 we `_` import the [embed](https://pkg.go.dev/embed) package. We're going to write SQL in .sql files and then embed them in the executable with `//go:embed` directives. On line 14 we `_` import `go-sqlite`, this will register the packages as an `sqlite3` driver for `database/sql` (which sqlx uses).

**Listing 5: SQL statements**

```
17 //go:embed schema.sql
18 var schemaSQL string
19 
20 //go:embed insert.sql
21 var insertSQL string
```

On lines 17-21 we use a `//go:embed` directive to embed the SQL written in .sql files into our code. This lets us write SQL outside the Go code and still ship a single executable.

**Listing 6: Row**

```
23 type Row struct {
24     Business   string    `csv:"businessname" db:"business_name"`
25     Licstatus  string    `csv:"licstatus" db:"license_status"`
26     Result     string    `csv:"result" db:"result"`
27     Violdesc   string    `csv:"violdesc" db:"description"`
28     Violdttm   time.Time `csv:"violdttm" db:"time"`
29     Violstatus string    `csv:"violstatus" db:"status"`
30     Viollevel  string    `csv:"viollevel" db:"-"`
31     Level      int       `db:"level"`
32     Comments   string    `csv:"comments" db:"comments"`
33     Address    string    `csv:"address" db:"address"`
34     City       string    `csv:"city" db:"city"`
35     Zip        string    `csv:"zip" db:"zip"`
36 }
```

On lines 23-36 we define the `Row` struct. It is used both by `csvutil` to parse rows in the CSV file and by `sqlx` to insert values to the database. We use [field tags](https://golang.org/ref/spec#Struct_types) to specify the corresponding columns in the CSV and the database.

On line 31 we define `Level` which is numeric. This field will be the integer representation of the `*` values in the `Viollevel` field in the CSV.

**Listing 7: parseLevel**

```
44 func parseLevel(value string) int {
45     switch value {
46     case "*":
47         return 1
48     case "**":
49         return 2
50     case "***":
51         return 3
52     }
53 
54     return -1
55 }
```

Listing 7 shows the `parseLevel` function that converts `*` to numeric level. On line 54 we return -1 for unknown values.

**Listing 8: unmarshalTime**

```
38 func unmarshalTime(data []byte, t *time.Time) error {
39     var err error
40     *t, err = time.Parse("2006-01-02 15:04:05", string(data))
41     return err
42 }
```

Listing 8 shows `unmarshalTime` which is used by `csvutil` to parse time values in the CSV file.

_Note: I never remember how to specify time format. My go-to place is the [Constants](https://pkg.go.dev/time#pkg-constants) section in the `time` package documentation._

**Listing 9: ETL***

```
57 func ETL(csvFile io.Reader, tx *sqlx.Tx) (int, int, error) {
58     r := csv.NewReader(csvFile)
59     dec, err := csvutil.NewDecoder(r)
60     if err != nil {
61         return 0, 0, err
62     }
63     dec.Register(unmarshalTime)
64     numRecords := 0
65     numErrors := 0
66 
67     for {
68         numRecords++
69         var row Row
70         err = dec.Decode(&row)
71         if err == io.EOF {
72             break
73         }
74         if err != nil {
75             log.Printf("error: %d: %s", numRecords, err)
76             numErrors++
77             continue
78         }
79         row.Level = parseLevel(row.Viollevel)
80         if _, err := tx.NamedExec(insertSQL, &row); err != nil {
81             return 0, 0, err
82         }
83     }
84 
85     return numRecords, numErrors, nil
86 }
```

Listing 8 shows the `ETL` function. On line 57 we see `ETL` receives an `io.Reader` as the CSV and a [transaction](https://en.wikipedia.org/wiki/Database_transaction) which is used to insert values to the database, `ETL` returns number of records, num of bad records and an error value.
On line 63 we register `unmarshalTime` to handle time values. On lines 64,45 we initial number of records and number of errors which are returned by `ETL`. On line 67 we start a `for` loop. On line 70 we decode a row from the CSV and on line 71 we check if the returned error is `io.EOF` signaling end-of--file. On line 74 we check for other errors and if there is log them and increase `numErrors` on line 76. Then on 79 we convert to numerical level and on line 80 we insert the record to the database, again checking for errors. Finally, on line 85 we return number of records, number of errors and signal that there was no critical error.

**Listing 10: main**

```
88 func main() {
89     file, err := os.Open("boston-food.csv")
90     if err != nil {
91         log.Fatal(err)
92     }
93     defer file.Close()
94 
95     db, err := sqlx.Open("sqlite3", "./food.db")
96     if err != nil {
97         log.Fatal(err)
98     }
99     defer db.Close()
100 
101     if _, err := db.Exec(schemaSQL); err != nil {
102         log.Fatal(err)
103     }
104 
105     tx, err := db.Beginx()
106     if err != nil {
107         log.Fatal(err)
108     }
109 
110     start := time.Now()
111     numRecords, numErrors, err := ETL(file, tx)
112     duration := time.Since(start)
113     if err != nil {
114         tx.Rollback()
115         log.Fatal(err)
116     }
117 
118     frac := float64(numErrors) / float64(numRecords)
119     if frac > 0.1 {
120         tx.Rollback()
121         log.Fatalf("too many errors: %d/%d = %f", numErrors, numRecords, frac)
122     }
123     tx.Commit()
124     fmt.Printf("%d records (%.2f errors) in %v\n", numRecords, frac, duration)
125 }
```

Listing 10 shows the `main` function. On lines 89-93 we open the CSV file. On lines 95-103 we open the database and create the table. On line 105 we create a transaction. On line 100 we record the start time and on line 112 we execute the ETL. On line 112 we calculate the duration. On line 113 we check for error and if there is we issue a [rollback](https://en.wikipedia.org/wiki/Rollback_(data_management)). On lines 118 we calculate the fraction of errors and if it's more than 10% we issue a rollback on line 120. Finally on line 123 we commit the transaction and on line 124 print some statistics.

### Interlude: Transactions

Inserting data via a transaction means that either all of the data is inserted or none of it. If we didn't use transactions, and half of the data went in - we had a serious issue. We need either to restart the ETL from the middle or delete the data that did manage to get it. Both options are hard to get right and will make your code complicated. Transactions are one of the main reasons (apart from SQL) that I love using transactional databases such as SQLLite, PostgreSQL and others.

## Running the ETL

**Listing 11: Running the ETL**

```
$ go run etl.go
... (many log lines reducted)
2021/09/11 12:21:44 error: 655301: parsing time " " as "2006-01-02 15:04:05": cannot parse " " as "2006"
655317 records (0.06 errors) in 13.879984129s
```

About 6% of the rows had errors in them, mostly missing time. This is OK since we've defined the error threshold to be 10%.

## Analysing the Data

Once the data is in the database, we can use SQL to query it.

**Listing 12: Query**

```
01 SELECT 
02     business_name, COUNT(business_name) as num_violations
03 FROM
04     violations
05 WHERE
06     license_status = 'Active' AND 
07     time >= '2016-01-01'
08 GROUP BY business_name
09 ORDER BY num_violations DESC
10 LIMIT 20
```

Listing 12 shows the SQL query to select the top 20 businesses which have had the most violations in the last 5 years. On line 02 we select the business_name column and the count of it. On lines 05-07 we limit the records to ones that are active and the time is after 2016. On line 08 we group the row by the business_name, on line 09 we order the results by the number of violations and finally on line 10 we limit to 20 results.

**Listing 13: Running the Query**

```
$ sqlite3 food.db < query.sql 
Dunkin Donuts|1031
Subway|996
The Real Deal|756
Mcdonalds|723
Caffe Nero|640
ORIENTAL HOUSE|599
Burger King|537
Dumpling Palace|463
Sweetgreen|454
The Upper Crust|453
Dunkin' Donuts|436
Yamato II|435
Anh Hong Restaurant|413
Chilacates|408
Yely's Coffee Shop|401
India Quality|386
Domino's Pizza|374
Fan Fan Restaurant|362
Pavement Coffeehouse|357
FLAMES RESTAURANT|357
```

Listing 13 shows how to run the query using the `sqlite3` command line utility.

## Final Thoughts

Data science is dominated by, well - data :) Data pipelines and ETLs are what brings the data to a place where you can query and analyze it. Go is a great fit for running ETL, it's fast, efficient and has great libraries.

Using transactions and SQL will save you a lot of effort in the long run. You don't need to be an SQL expert (I'm not) in order to use them, and there's a lot of knowledge out there on SQL - it's been around since the 70's.

As for where not to eat - I'll leave that to your discretion :)

The code is available [here](https://github.com/353words/food)
