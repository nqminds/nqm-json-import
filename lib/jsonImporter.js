/**
 * Created by toby on 16/11/15.
 */

module.exports = (function() {
  "use strict";

  var log = require("debug")("log");
  var debug = require("debug")("debug");
  var fs = require("fs");
  var path = require("path");
  var moment = require("moment");
  var _ = require("lodash");
  var nqmUtils = require("nqm-utils");
  var es = require("event-stream");
  var JSONStream = require("JSONStream");
  var parser = require("mongo-parse");
  var commands = require("./commands");

  function JSONImporter() {
  }

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
          __tdxType: [typeof val]
        }
      }
    };

    var ds = {};
    recurse(ds, obj, "schema");

    return ds.schema;
  };

  var getAccessToken = function(cb) {
    var self = this;
    if (self._config.credentials) {
      commands.getAccessToken(self._config.commandHost, self._config.credentials, function(err, accessToken) {
        if (err) {
          cb(err);
        } else {
          self._accessToken = accessToken;
          if (!self._config.targetDataset) {
            commands.createTargetDataset(
              self._config.commandHost,
              self._accessToken,
              self._config.targetFolder,
              self._config.datasetName,
              self._config.basedOnSchema,
              self._config.schema,
              self._config.primaryKey,
              function(err, ds) {
                if (err) {
                  return cb(err);
                }
                self._config.targetDataset = ds;
                process.nextTick(cb);
              });
          } else {
            process.nextTick(cb);
          }
        }
      });
    } else {
      process.nextTick(cb);
    }
  };

  var transformForUpsert = function(data) {
    var self = this;
    var obj = {
      __update: []
    };    
    
    // Check there is data for each primary key field.
    _.forEach(this._config.primaryKey, function(keyField) {
      var keyDataPointers = parser.DotNotationPointers(data, keyField);
      if (keyDataPointers.length === 0) {
        log("no data for primary key field '%s' - %j", keyField, data);
        process.exit(-1);
      } else {
        var updatePointers = parser.DotNotationPointers(obj, keyField);
        updatePointers[0].val = keyDataPointers[0].val;
      }
    });

    var flattened = nqmUtils.flattenJSON(data);
    _.forEach(flattened, function (v, k) {
      if (self._config.primaryKey.indexOf(k) >= 0) {
        // This a primary key field => do nothing.
      } else {
        // Do upsert of value.
        var update = {
          m: "r",     // method is replace
          p: k,       // property is key name
          v: v        // value is data
        };
        obj.__update.push(update);                
      }      
    });

    return obj;
  };

  var stripArrayIndices = function(key) {
    var comps = key.split(".");
    var nonNumeric = _.filter(comps, function(k) { return parseInt(k) != k; });
    var numeric = _.filter(comps, function(k) { return parseInt(k) == k; });
    return {
      key: nonNumeric.join("."),
      indices: numeric.join(".")
    }
  };

  var transmitQueue = function(data, cb) {
    var self = this;

    if (self._config.upsertMode) {
      data = _.map(data, function(d) { return transformForUpsert.call(self, d); });
    }

    var bulkLoad = self._config.upsertMode ? commands.upsertDatasetDataBulk : commands.addDatasetDataBulk;
    bulkLoad(self._config.commandHost, self._accessToken, self._config.targetDataset, data, function(err) {
      if (err) {
        // Review - failed to add items, continue with other items or abort?
        log("failed to write %d items [%s]", data.length, err.message);
        self._failedCount += data.length;
      } else {
        debug("added %d data items", data.length);
        self._addedCount += data.length;
      }
      cb(err);
    });
  };

  function processJSONObject() {
    var self = this;

    var writeFunc = function(data) {
      var stream = this;
      stream.pause();

      var queueData = function(err) {
        if (err) {
          log("queueData: " + err.message);
          stream.emit("end");
        } else {
          if (self._seenCount < self._config.startAt) {
            log("ignoring object %d, starting at %d", self._seenCount, self._config.startAt);
            stream.resume();
          } else if (self._config.endAt >= 0 && self._seenCount >= self._config.endAt) {
            log("ignoring object %d, ended at %d", self._seenCount, self._config.endAt);
            stream.emit("end");
          } else {
            debug("object %d is %d bytes", self._seenCount, JSON.stringify(data).length);
            self._bulkQueue.push(data);
            if (self._bulkQueue.length >= self._config.bulkCount) {
              transmitQueue.call(self, self._bulkQueue, function(err) {
                if (err) {
                  log("processJSONObject error: " + err.message);
                  stream.emit("end");
                } else {
                  self._bulkQueue = [];
                  stream.resume();                  
                }
              });
            } else {
              process.nextTick(function() { stream.resume(); });
            }
          }
        }
        self._seenCount++;
      };

      if (self._seenCount === 0) {
        // If a schema is defined, or we are updating an existing dataset, proceed to acquiring the access token.
        if (self._config.schema || (self._config.targetDataset && !self._config.upsertMode)) {
          getAccessToken.call(self, queueData);
        } else if (self._config.inferSchema) {
          self._config.schema = inferSchema(data);
          log("inferred schema is %j", self._config.schema);
          getAccessToken.call(self, queueData);
        }
      } else {
        queueData(null);
      }
    };

    return es.through(writeFunc);
  }

  JSONImporter.prototype.start = function(config, cb) {
    var self = this;
    this._config = config;

    this._seenCount = this._addedCount = this._failedCount = 0;
    this._bulkQueue = [];

    self._config.commandHost = self._config.commandHost || "https://cmd.nqminds.com";
    self._config.datasetName = self._config.datasetName || (path.basename(self._config.sourceFile, path.extname(self._config.sourceFile)) + " [" + moment().format("D MMM YYYY H:mm:ss") + "]");
    self._config.bulkCount = self._config.bulkCount || 100;
    self._config.primaryKey = self._config.primaryKey || [];
    self._config.inferSchema = self._config.inferSchema || !self._config.schema;
    self._config.upsertMode = self._config.upsertMode || false;
    self._config.startAt = self._config.startAt || 0;
    self._config.endAt = self._config.endAt || -1;
    self._config.basedOnSchema = self._config.basedOnSchema || "dataset";

    if (self._config.upsertMode && self._config.primaryKey.length === 0) {
      log("forcing non-upsert mode since there is no primary key defined");
      self._config.upsertMode = false;
    }

    var parsePath = config.dataPath ? (config.dataPath + ".*") : "*";
    fs.createReadStream(config.sourceFile, { encoding: 'utf-8' })
      .pipe(JSONStream.parse(parsePath))
      .pipe(processJSONObject.call(self))
      .pipe(es.through(null, function() {
        // Transmit any remaining data in the queue.
        transmitQueue.call(self, self._bulkQueue, function(err) {
          if (err) {
            log("error: " + err.message);
          } else {
            log("complete");            
          }
          log("added %d items, %d failures",self._addedCount, self._failedCount);
          cb(err);
        });
      }));
  };

  return JSONImporter;
}());