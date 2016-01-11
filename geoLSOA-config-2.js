/**
 * Created by toby on 17/11/15.
 */

"use strict";

module.exports = {

  commandHost: "http://localhost:3103",
  credentials: "NJ-yUyZ7Xg:password",
  sourceFile: "./tests/geoLSOA-2.json",
  dataPath: "features",
  bulkCount: 2,

  /*
   * The schema of the target dataset.
   */
  "schema": {
    "type": { type: "string" },
    "properties": {
      "CCG15CD": "string",
      "CCG15NM": "string"
    },
    "geometry": {
      type: { type: "string" },
      coordinates: { type: "object" }
    }
  },
  primaryKey: ["properties.CCG15CD"]
};
