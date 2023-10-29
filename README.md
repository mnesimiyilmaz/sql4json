# Overview
SQL4Json is allows you to select, filter, aggregate your json data with using nearly same syntax of SQL Select query. Here it is fully fledged query example.
```sql
SELECT user.name AS userName,
SUM(value)       AS total,
COUNT(value)     AS cnt,
COUNT(*)         AS count_including_nulls,
MAX(value)       AS max_val,
MIN(value)       AS min_val,
AVG(value)       AS avg_val

FROM $r.nested.object

WHERE ((a > 5 AND x.z < 10) OR c = 20)
    AND age IS NOT NULL
    AND isActive = true
    AND isDeleted IS NULL
    AND LOWER(name, 'en-US') LIKE '%cahit%'
    AND (some.number >= 20 OR COALESCE(some.other.number, 0) <= 10)
    AND TO_DATE(lasLogin, 'yyyy-MM-dd HH:mm:ss') < NOW()

GROUP BY user.name

HAVING total > 5 OR avg_val < 3

ORDER BY total DESC, userName
```
### Requirements
Java 1.8 or above.

# Get it!

## Maven
To use the package, you need to add following dependency:
```xml
<dependency>
    <groupId>io.github.mnesimiyilmaz</groupId>
    <artifactId>sql4json</artifactId>
    <version>0.0.2</version>
</dependency>
```

## Non-Maven
For non-Maven use cases, you download jars from [packages](https://github.com/mnesimiyilmaz/sql4json/packages).


# Use it!

Usage starts with creation of `SQL4JsonInput` instance. You can create an instance using the following methods:

Serializable Object
```java
SQL4JsonInput input = SQL4JsonInput.fromObject("SELECT * FROM $r", yourObject);
```
or Json String
```java
SQL4JsonInput input = SQL4JsonInput.fromJsonString("SELECT * FROM $r", "jsonString");
```
or `JsonNode` Supplier
```java
SQL4JsonInput input = SQL4JsonInput.fromJsonNodeSupplier("SELECT * FROM $r", () -> new ObjectNode(null));
```
or `JsonNode`
```java
SQL4JsonInput input = new SQL4JsonInput("SELECT * FROM $r", new ObjectNode(null));
```

Than create an instance of `SQL4JsonProcessor` and pass `SQL4JsonInput` as constructor parameter and lastly call `getResult()` method.
```java
SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
JsonNode result = processor.getResult();
```

Voila! Now you can convert `result` to any type you want via using `ObjectMapper`.

# Syntax & Supported Operations

## Selecting Fields
Like in SQL if you want to select all fields, you can simply put an asterisk.
```sql
select * from $r
```
If you want to select specific field, object or array you just need to write path of field. Moreover you can give alias for fields.
```sql
SELECT fullname,
       id AS userId,
       username AS user.username
       some.field.in.some.object AS user.someData
       object.field AS renamedObject
       array.field AS renamed.data.arrayField
FROM $r       
```
The example above will be give output like below
```
[
    {
        fullname: "Mucahit Nesimi YILMAZ",
        userId: 1,
        user: {
            username: "mucahit"
            someData: 123
        },
        renamedObject: {
            ...object fields here...
        },
        renamed: {
            data: {
                arrayField: [1, 2, 3]
            }
        }
    },
    ...other objects...
]
```

## FROM $r
`$r` is reserved keyword for SQL4Json. You must add `$r` to from section of your query. You can expand `$r` for scenarios for like below.

Let's say you have json like this
```
{
    responseStatus: 200,
    errors: null,
    response: {
        data: [...list of objects...]
    }
}
```
You can select `data` directly by typing the path of `data` after the `$r` expression.
```sql
SELECT * FROM $r.response.data
```

## WHERE Clause
You can filter data using complex conditions. You can chain your conditions using `AND` and `OR`, and you can use comparison operators, `IS NULL`, `IS NOT NULL` and `LIKE`.
```sql
SELECT * FROM $r
WHERE ((a > 5 AND x.z < 10) OR c = 20)
AND age IS NOT NULL
AND isActive = true
AND isDeleted IS NULL
AND name LIKE '%cahit%'
AND (some.number >= 20 OR some.other.number <= 10)
```

## GROUP BY and HAVING
You can aggregate your data using aggregation functions such as `COUNT`, `MIN`, `MAX`, `SUM` and `AVG`.
```sql
SELECT
field1, field2,
SUM(value)      AS total,
COUNT(value)    AS cnt,
COUNT(*)        AS count_including_nulls,
MAX(value)      AS max_val,
MIN(value)      AS min_val,
AVG(value)      AS avg_val
FROM $r
GROUP BY field1, field2
HAVING avg_val > 10 AND max_val < 100
```
**Note 1:** All aggregation functions other than `COUNT(*)`, do calculation over non null data.

**Note 2:** Field names in `HAVING`, **must** be aliases that you determined in select.

## ORDER BY
You can sort data by specific fields.
```sql
SELECT * FROM $r
ORDER BY someObject.field, someField DESC, otherField ASC
```
If you want to sort grouped data you should use aliases in `ORDER BY`.
```sql
SELECT field1,
SUM(value) AS total
FROM $r
GROUP BY field1,
HAVING total > 10
ORDER BY total DESC, field1 ASC
```
## Nested Queries
In order to process your data step by step you can write nested queries. Execution order of nested queries is from **bottom** to **top**. You can express nested query with using `>>>` operator.
```sql
SELECT name, COUNT(*) as userCount FROM $r GROUP BY name ORDER BY userCount DESC
    >>>
SELECT name, age FROM $r WHERE id > 0
    >>>
SELECT objectField FROM $r.data WHERE groupName = 'users'
```
Or
```sql
SELECT name, age FROM (SELECT objectField FROM $r.data WHERE groupName = 'users') WHERE id > 0
```

## Functions
### Lower & Upper
You can use `LOWER` and `UPPER` functions for `string` fields. These functions takes optional lang code parameter. JVM default will be used if you don't pass lang code.
```sql
SELECT LOWER(name) AS name
FROM $r 
WHERE UPPER(username, 'en-US') = 'MUCAHIT'
ORDER BY LOWER(name, 'tr-TR')
```

### Coalesce
Returns second parameter value if first one is null.
```sql
SELECT * FROM $r WHERE COALESCE(num, 0) > -1 
```

### TO_DATE
Convert date strings to `LocalDate` or `LocalDateTime` instance to in order to compare or sort. This function takes optional pattern parameter.
```sql
SELECT * FROM $r 
WHERE TO_DATE(lasLogin, 'yyyy-MM-dd HH:mm:ss') > TO_DATE('2023-25-10 03:00:00')
```

### NOW
Returns `LocalDateTime` instance of current date time.
```sql
SELECT * FROM $r WHERE TO_DATE(lasLogin, 'yyyy-MM-dd HH:mm:ss') < NOW()
```
