# Spark commands

```python
text_file = sc.textFile("BORROWERS.TXT")

seperated_file = text_file.map(lambda line: line.split("|"))

seperated_file.first()

borrower_df = seperated_file.toDF()

borrower_df.show()
```

```SQL
SELECT
    gender, department, sum(bid)
    FROM
    BORROWRES
    INNER JOIN LOANS ON borrower.bid, loan.bid
    Group by
    gender, department
    Group by
    gender
    Group by
    department
    Group by
    none
```