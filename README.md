#introduction
Generic importer for importing JSON files into nqm datasets.

##install

```
npm install -g nqm-json-import
```

Use sudo if you get EACCESS errors.

##basic usage

```
DEBUG=log nqm-json-import --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json --dataPath features
```

Basic import of new dataset where the schema is inferred from the source JSON and no primary key is defined. The dataset will be created using a name based on the source file. Having no primary key means that it is not possible to update the data and all data will be appended to the dataset. The ```dataPath``` option indicates the path to the data in the source file.

```
DEBUG=log nqm-json-import --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json --dataPath features --primaryKey properties.LSOA01CD
```

Import new dataset specifying a primary key. Subsequent updates are possible. The dataset will be created using a name based on the source file.

```
DEBUG=log nqm-json-import --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json  --dataPath features --primaryKey properties.LSOA01CD --targetDataset 4ybvaLm2zx
```

Import data to an existing dataset. As a primary key is given "upsert" operations will be performed.

##advanced usage

It is possible to define the import parameters using a configuration file instead of via the command-line. When using this approach it is possible to specify mappings from the source data to the target dataset schema.

The configuration file will vary depending on the type of data, but at a minimum it will contain details of the data source, the target dataset and the schema mappings. There are example JSON configuration files in the repo.

```
{
  commandHost: "http://cmd.nqminds.com",
  credentials: "<tokenId:secret>",
  sourceFile: "./tests/geoLSOA.json",

  /*
   * The path to the data to be imported. Use dot notation if necessary.
   */
  dataPath: "features",

  /*
   * The schema of the target dataset.
   */
  "schema": {
    "geometryType": "string",
    "coordinates": {},
    "shapeLen": "number",
    "shapeArea": "Number",
    "name": "String",
    "code": "String"
  },
  primaryKey: ["code"],

  /*
   * The schemaMapping is a dictionary mapping the source JSON fields to target schema field.
   * If a JSON field is defined in the schema the data will be copied to the named field in the dataset.
   * If a JSON field is defined as blank in the schema, the column will be skipped.
   * If there is no entry for a given JSON field, the data will be copied as-is.
   */
  "schemaMapping": {
    "properties.LSOA01CD": "code",
    "properties.LSOA01NM": "name",
    "properties.SHAPE_LEN": "shapeLen",
    "properties.SHAPE_AREA": "shapeArea",
    "geometry.type": "geometryType",
    "geometry.coordinates": "coordinates",
    "type": ""  // Skip
  }
}
```

##build
Clone this repository then:

```
cd nqm-json-import
npm install
```



