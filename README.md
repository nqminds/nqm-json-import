# nqm-json-import #

## introduction 
Generic importer for importing JSON files into nqm datasets.

## install

```
npm install -g nqm-json-import
```

Use sudo if you get EACCESS errors.

## target folder
The TDX currently only supports importing data to a resource folder, i.e. you can not import a dataset to the root of your TDX workspace. Consequently
you must create a folder to receive the imported dataset before you begin.

## access token
You must create an access token with adequate permissions to add/edit a dataset in the target folder. To do this, go to the ```access tokens``` page
on the TDX and create a token. Then go back to your target folder, and add permissions for the newly created access token to write to the folder.

You can do this by clicking on the ```info``` icon of the folder, then clicking the ```share``` icon in the right side-bar. Click ```add trusted person```
and then select the ```share tokens``` tab. In the ```share name``` field type the name of the access token you just created, and then 
make sure you select ```Edit``` in the ```access``` drop-down. 

## basic usage

Basic import of new dataset where the schema is inferred from the source JSON and no primary key is defined. The dataset will be created using a name based on the source file. Having no primary key means that it is not possible to update the data and all data will be appended to the dataset. The ```dataPath``` option indicates the path to the data in the source file.
```
nqm-json-import --targetFolder <target folder id> --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json --dataPath features 
```
### base schema
It is recommended to specify a schema type using the ```basedOnSchema``` argument, for example ```geojson```. This ensures that the data is imported in the correct format and will be understood by all applications expecting data conforming to the standard geojson format. 
```
nqm-json-import --basedOnSchema geojson --targetFolder <target folder id> --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json --dataPath features
```
### primary key
It is also recommended to specify a primary key. This makes subsequent updates possible. Note it is not necessary to specify a primary key if the base schema already has one defined.
```
nqm-json-import --primaryKey properties.LSOA01CD --basedOnSchema geojson --targetFolder <target folder id> --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json --dataPath features 
```
### update data
You can import data to an existing dataset. The ```upsertMode``` indicates that the data from ```sourceFile``` will update any existing data that matches the given primary key, and if no data matching the primary key is found the data will be inserted. You must specify a ```targetDataset``` to use upsert mode.
```
nqm-json-import --upsertMode true --primaryKey properties.LSOA01CD --basedOnSchema geojson --targetDataset <target dataset id> --credentials <tokenId:secret> --sourceFile tests/geoLSOA.json  --dataPath features
```
## advanced usage
It is possible to define the import parameters using a configuration file instead of via the command-line. This is necessary if you need to manually specify a schema rather than have it inferred by the importer. 

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
# schema definition
For advanced use it is possible to specify the schema definition in the configuration file. Most schemas should be defined in the TDX,
but it may be necessary to specify one-off schemas or augment existing schemas using settings in the configuration file.

In the configuration file example above, note that any schema specified in the configuration file will be used to augment the schema identified by the ```basedOnSchema``` parameter. So in the example below,
the ```properties``` object will be merged with the existing ```geojson``` schema defined in the TDX.

The schema definition is similar format to the mongoose schema definition, except the key used to identify the type is ```__tdxType``` rather than ```type```.

For example:
```
  "schema": {
    "age": { "__tdxType": ["string","demographic","ageBand"] },
    "homelessness": { "__tdxType": ["number","e0","persons"] }
  }
```
TODO - document type definition.
# options
```targetFolder``` - the id of the target folder (required)

```bulkCount``` - specify the number of documents to process at once. If your data contains small documents, this can be set to a high number for improved performance.

```upsertMode``` - set upsert mode, requires a primary key to be set

```targetDataset``` - the id of the target dataset (required if updating)

```commandHost``` - the destination TDX command endpoint

```credentials``` - the credentials to use, obtain these from the nqminds toolbox 'access tokens' page.

```sourceFile``` - path to the source data file

```primaryKey``` - array of properties that specify the primary key of the data

```schema``` - TODO - document properly

## build
Clone this repository then:
```
cd nqm-json-import
npm install
```
