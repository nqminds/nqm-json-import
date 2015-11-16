/**
 * Created by toby on 16/11/15.
 */

module.exports = (function() {
  "use strict";

  var log = require("debug")("log");
  var debug = require("debug")("debug");
  var helpers = require("../../nqm-common-import");
  var fs = require("fs");
  var path = require("path");
  var parser = require("mongo-parse");
  var moment = require("moment");
  var flattenJSON = require("@nqminds/nqm-utils").flattenJSON;
  var _ = require("lodash");

  function JSONImporter() {

  }

  var buildSchemaFlat = function(obj) {
    var objFlat = flattenJSON(obj);
    var keys = Object.keys(objFlat);
    var schema = {};
    _.forEach(keys, function(k) {
      var fieldName, fieldType;
      var checkArray = k.indexOf("[");
      if (checkArray >= 0) {
        fieldName = k.substr(0,checkArray);
        fieldType = "Array";
      } else {
        fieldName = k;
        fieldType = typeof objFlat[k];
      }
      schema[fieldName] = fieldType;
    });

    return schema;
  };

  var inferSchema = function(obj) {
    var recurse = function(schemaObj, val, prop) {
      if (Array.isArray(val)) {
        schemaObj[prop] = [];
      } else if (typeof val === "object") {
        schemaObj[prop] = {};
        _.forEach(val, function(v,k) {
          recurse(schemaObj[prop], v, k);
        });
      } else {
        schemaObj[prop] = {
          type: typeof val,
          index: false
        }
      }
    };

    var ds = {};
    recurse(ds, obj, "schema");

    return ds.schema;
  };

  var getAccessToken = function(data, cb) {
    var self = this;
    if (self._config.credentials) {
      helpers.commands.getAccessToken(self._config.commandHost, self._config.credentials, function(err, accessToken) {
        if (err) {
          cb(err);
        } else {
          self._accessToken = accessToken;
          if (!self._config.targetDataset) {
            helpers.commands.createTargetDataset(
              self._config.commandHost,
              self._accessToken,
              self._config.datasetName,
              self._config.schema,
              self._config.primaryKey,
              function(err, ds) {
                if (err) {
                  return cb(err);
                }
                self._config.targetDataset = ds;
                startProcessing.call(self, data, cb);
              });
          } else {
            startProcessing.call(self, data, cb);
          }
        }
      });
    } else {
      startProcessing.call(self, data, cb);
    }
  };

  var startProcessing = function(data, cb) {
    var self = this;
    var count = data.length;

    var send = data.splice(0, this._config.bulkCount);
    if (send.length > 0) {
      var bulkLoad = self._config.upsertMode ? helpers.commands.upsertDatasetDataBulk : helpers.commands.addDatasetDataBulk;
      bulkLoad(self._config.commandHost, self._accessToken, self._config.targetDataset, send, function(err) {
        if (err) {
          // Review - failed to add items, continue with other items or abort?
          log("failed to write line %d [%s]", send.length, err.message);
          self._failedCount += send.length;
        } else {
          debug("added %d data items", send.length);
          self._addedCount += send.length;
        }
        process.nextTick(function() {
          startProcessing.call(self, data, cb);
        })
      });
    } else {
      log("complete");
      cb();
    }
  };

  JSONImporter.prototype.start = function(config, cb) {
    var self = this;
    this._config = config;

    self._config.datasetName = self._config.datasetName || (path.basename(self._config.sourceFile, path.extname(self._config.sourceFile)) + " [" + moment().format("D MMM YYYY H:mm:ss") + "]");
    self._config.bulkCount = self._config.bulkCount || 100;
    self._config.primaryKey = self._config.primaryKey || [];
    self._config.inferSchema = self._config.inferSchema || !self._config.schema;

    var contents = fs.readFileSync(config.sourceFile);
    var data = JSON.parse(contents);

    var pointers = parser.DotNotationPointers(data, config.dataPath);
    if (pointers.length > 0) {
      data = pointers[0].val;
      if (!Array.isArray(data)) {
        data = [data];
      }

      if (self._config.schema) {
        getAccessToken.call(self, data, cb);
      } else if (self._config.inferSchema) {
        self._config.schema = inferSchema(data[0]);
        getAccessToken.call(self, data, cb);
      }
    } else {
      log("failed to get data using path " + config.dataPath);
      cb(new Error("no data found using " + config.dataPath));
    }
  };

  return JSONImporter;
}());