/**
 * Created by toby on 17/11/15.
 */

"use strict";

module.exports = {

  commandHost: "http://localhost:3103",
  credentials: "<tokenId:secret>",
  sourceFile: "./tests/geoLSOA.json",
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
    "code": "String",
    "embed": {
      name: "string",
      value: "number",
      array1: [ "number" ],
      array2: [ { sub: "string", obj: "number" } ]
    }
  },
  primaryKey: ["code"],

  /*
   * The schemaMapping is a dictionary mapping the CSV headings to target schema field.
   * If a CSV header column is defined in the schema the data will be copied to the named field in the dataset.
   * If a CSV header column is defined as blank in the schema, the column will be skipped.
   * If there is no entry for a given CSV heading, the data will be copied to a field with the name of the heading.
   */
  "schemaMapping": {
    "properties.LSOA01CD": "code",
    "properties.LSOA01NM": "name",
    "properties.SHAPE_LEN": "shapeLen",
    "properties.SHAPE_AREA": "shapeArea",
    "geometry.type": "geometryType",
    "geometry.coordinates": "coordinates",
    "type": ""  // Skip
  },
};
