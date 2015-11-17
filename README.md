#introduction
Generic importer for importing JSON files into nqm datasets.

##basic usage

```
DEBUG=log node nqm-json-import --credentials 4kClAjTzg:password --sourceFile tests/geoLSOA.json
```

Basic import of new dataset where the schema is inferred from the source JSON and no primary key is defined. The dataset will be created using a name based on the source file. Having no primary key means that it is not possible to update the data and all data will be appended to the dataset.

```
DEBUG=log node nqm-json-import --credentials 4kClAjTzg:password --sourceFile tests/geoLSOA.json --primaryKey properties.LSOA01CD
```

Import new dataset specifying a primary key. Subsequent updates are possible. The dataset will be created using a name based on the source file.

```
DEBUG=log node nqm-json-import --credentials 4kClAjTzg:password --sourceFile tests/geoLSOA.json --primaryKey properties.LSOA01CD --targetDataset 4ybvaLm2zx
```

Import data to an existing dataset. As a primary key is given "upsert" operations will be performed.



