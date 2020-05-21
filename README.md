## PYSPARK ETL

This application reads input file and transform the fields with defined spark sql functions mentioned in schema metadata.

Schema metadata can have sql functions defined, those will be applied to respetive column

- "function" : Spark SQL function
- "alias"    : Rename the column

### Example Schema:

```json
{
  "type":"struct",
  "fields":[
    {
      "name":"id",
      "nullable":true,
      "type":"integer",
      "metadata":{
      }
    },
    {
      "name":"name",
      "nullable":true,
      "type":"string",
       "metadata":{
         "alias" : "full_name",
         "function" : "upper(concat(name, ' LastName'))"
       }
    },
    {
      "name":"city",
      "nullable":true,
      "type":"string",
       "metadata":{
         "alias" : "Location",
         "function" : "lpad(city, 20, '0')"
       }
    }]
}
```

### Build & Test
py setup.py bdist_egg