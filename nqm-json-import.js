#! /usr/bin/env node

 /**
 * Created by toby on 16/11/15.
 */

(function() {
  var log = require("debug")("main");
  var argv = require("minimist")(process.argv.slice(2));
  var fs = require("fs");
  var path = require("path");
  var config;
  var JSONImport = require("./lib/jsonImporter");
  var pjson = require("./package.json");

  console.log("nqm-json-import v%s", pjson.version);

  /*
   * config file
   */
  if (!argv.config) {
    log("no config file given - using defaults");
    config = {};
  } else {
    var configFile = path.resolve(argv.config);

    // Get the configuration.
    try {
      config = require(configFile || "./default-config.js");
    } catch (err) {
      console.log("failed to parse config file %s: %s", configFile, err.message);
      process.exit(-1);
    }
  }

  /*
   * credentials
   */
  if (argv.credentials) {
    config.credentials = argv.credentials;
  }

  if (!config.credentials) {
    console.log("no credentials given");
    process.exit(-1);
  }

  /*
   * commandHost
   */
  if (argv.commandHost) {
    config.commandHost = argv.commandHost;
  }

  /*
   * source file
   */
  if (argv.sourceFile) {
    config.sourceFile = argv.sourceFile;
  }

  /*
   * target folder
   */
  if (argv.targetFolder) {
    config.targetFolder = argv.targetFolder;
  }
  
  /*
   * target dataset
   */
  if (argv.targetDataset) {
    config.targetDataset = argv.targetDataset;
  }

  /*
   * dataset name
   */
  if (argv.datasetName) {
    log("dataset name is %s", argv.datasetName);
    config.datasetName = argv.datasetName;
  }

  /*
   * inferSchema
   */
  if (argv.inferSchema || config.inferSchema) {
    log("inferring schema");
    config.inferSchema = true;
  }

  /*
   * primaryKey
   */
  if (argv.primaryKey) {
    log("primary key: ", argv.primaryKey);
    config.primaryKey = argv.primaryKey.split(",");
  }

  /*
   * dataPath
   */
  if (argv.dataPath) {
    log("data path: ", argv.dataPath);
    config.dataPath = argv.dataPath;
  }

  /*
   * upsertMode
   */
  if (argv.upsertMode) {
    log("upsert mode: ", argv.upsertMode);
    config.upsertMode = (argv.upsertMode === "true") ? true : false;
  }

  /*
   * bulkCount
   */
  if (argv.bulkCount) {
    log("bulk count: ", argv.bulkCount);
    config.bulkCount = argv.bulkCount;
  }

  /*
   * startAt
   */
  if (argv.hasOwnProperty("startAt")) {
    log("start at object %d", argv.startAt);
    config.startAt = argv.startAt;
  }

  /*
   * endAt
   */
  if (argv.endAt) {
    log("end at object %d", argv.endAt);
    config.endAt = argv.endAt;
  }

  // Create a JSON importer instance.
  var importer = new JSONImport();

  // Initiate the import with our configuration.
  importer.start(config, function(err) {
    if (err) {
      console.log("failed to import: %s", err.message);
      process.exit(-1);
    } else {
      console.log("import finished");
      process.exit(0);
    }
  });

}());