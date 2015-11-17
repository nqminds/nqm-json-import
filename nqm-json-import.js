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

  /*
   * config file
   */
  if (!argv.config) {
    log("no config file given - using default");
    config = {};
  } else {
    var configFile = path.resolve(argv.config);

    // Get the configuration.
    try {
      config = require(configFile || "./default-config.js");
    } catch (err) {
      console.log("failed to parse config file %s: %s", configFile, err.message);
      process.exit();
    }
  }

  /*
   * credentials
   */
  if (argv.credentials) {
    config.credentials = argv.credentials;
  }

  if (!config.credentials) {
    log("no credentials given");
    process.exit();
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
   * target dataset
   */
  if (argv.targetDataset) {
    config.targetDataset = argv.targetDataset;
  }

  /*
   * dataset name
   */
  if (argv.datasetName) {
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

  if (argv.dataPath) {
    log("data path: ", argv.dataPath);
    config.dataPath = argv.dataPath;
  }
  // Create a JSON importer instance.
  var importer = new JSONImport();

  // Initiate the import with our configuration.
  importer.start(config, function(err) {
    if (err) {
      log("failed to import: %s", err.message);
    } else {
      log("import finished");
    }
  });

}());