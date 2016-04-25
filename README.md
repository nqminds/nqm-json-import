#introduction
Generic importer for importing JSON files into nqm datasets.

##install

```
npm install -g nqm-json-import
```

Use sudo if you get EACCESS errors.

##basic usage

```
nqm-json-import --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json --dataPath features --targetFolder <target folder id> --basedOnSchema geojson
```

Basic import of new dataset where the schema is inferred from the source JSON and no primary key is defined. The dataset will be created using a name based on the source file. Having no primary key means that it is not possible to update the data and all data will be appended to the dataset. The ```dataPath``` option indicates the path to the data in the source file.

```
nqm-json-import --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json --dataPath features --targetFolder <target folder id> --basedOnSchema geojson
```

Specifying the schema type using the ```basedOnSchema``` argument - in this case ```geojson```. This ensures that the data is imported in the correct format and will be understood by all applications expecting data conforming to the standard geojson format. 

```
nqm-json-import --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json --dataPath features --targetFolder <target folder id> --basedOnSchema geojson --primaryKey properties.LSOA01CD
```

As above, but with the addition of a primary key specification. This makes subsequent updates possible.

```
nqm-json-import --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json  --dataPath features --primaryKey properties.LSOA01CD --targetDataset <target dataset id> --upsertMode true
```

Import data to an existing dataset. The ```upsertMode``` indicates that the data from ```sourceFile``` will update any existing data that matches the given primary key, and if no data matching the primary key is found the data will be inserted.

##advanced usage

It is possible to define the import parameters using a configuration file instead of via the command-line. 

The configuration file will vary depending on the type of data, but at a minimum it will contain details of the data source, the target dataset/folder. There are example JSON configuration files in the repo.

```
{
  "credentials": "aaaaaaaa:bbbbbbbb",
  "sourceFile": "./tests/geoLSOA.json",
  "dataPath": "features",
  "basedOnSchema": "geojson",
  "targetFolder": "xxxxxxxxxx",
  "upsertMode": false, 

  "schema": {
    "properties": {      
      "LSOA01CD": "string",
      "LSOA01NM": "string"
    }
  },
  "primaryKey": ["properties.LSOA01CD"]    
}
```

#options

```targetFolder``` - the id of the target folder (required)

```bulkCount``` - specify the number of documents to process at once. If your data contains small documents, this can be set to a high number for improved performance.

```upsertMode``` - set upsert mode, requires a primary key to be set

```targetDataset``` - the id of the target dataset (required if updating)

```commandHost``` - the destination TDX command endpoint

```credentials```

```sourceFile```

```primaryKey``` - array of properties that specify the primary key of the data

```schema``` - TODO - document

##build
Clone this repository then:

```
cd nqm-json-import
npm install
```
