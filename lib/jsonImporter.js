/**
 * Created by toby on 16/11/15.
 */

module.exports = (function() {
  "use strict";

  var log = require("debug")("log");
  var debug = require("debug")("debug");
  var helpers = require("nqm-common-import");
  var fs = require("fs");
  var path = require("path");
  var moment = require("moment");
  var _ = require("lodash");
  var nqmUtils = require("nqm-utils");
  var es = require("event-stream");
  var JSONStream = require("JSONStream");

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
      helpers.commands.getAccessToken(self._config.commandHost, self._config.credentials, function(err, accessToken) {
        if (err) {
          cb(err);
        } else {
          self._accessToken = accessToken;
          if (!self._config.targetDataset) {
            helpers.commands.createTargetDataset(
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

    var setField = function(field, data, arrIndices) {
      // If the target is part of the primary key add it as a property.
      if (self._config.primaryKey.indexOf(field) >= 0) {
        // TODO - handle array indices?
        var isArray = false; 
        helpers.utils.setData(obj, field, data, isArray);
      } else {
        // Do upsert of value.
        var update = {
          m: "r",             // replace
          p: field + (arrIndices ? "." + arrIndices : ""),
          v: data
        };
        obj.__update.push(update);
      }
    };

    var flattened = nqmUtils.flattenJSON(data);

    if (this._config.schemaMapping) {
      // Use the configured schema mapping to copy data from the source data.
      _.forEach(flattened, function(val, key) {
        var cleanKey = stripArrayIndices(key);
        if (this._config.schemaMapping.hasOwnProperty(cleanKey.key)) {
          var target = this._config.schemaMapping[cleanKey.key];
          if (target.length > 0) {
            // Dataset field target is defined in the configuration.
            setField(target, val, cleanKey.indices);
          } else {
            // Dataset field target is blank => skip this column.
          }
        } else {
          // No target defined => use existing field.
          setField(key, val);
        }
      }, this);
    } else {
      _.forEach(flattened, function (v, k) {
        setField(k, v);
      });
    }

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

    var bulkLoad = self._config.upsertMode ? helpers.commands.upsertDatasetDataBulk : helpers.commands.addDatasetDataBulk;
    bulkLoad(self._config.commandHost, self._accessToken, self._config.targetDataset, data, function(err) {
      if (err) {
        // Review - failed to add items, continue with other items or abort?
        log("failed to write %d items [%s]", data.length, err.message);
        self._failedCount += data.length;
      } else {
        debug("added %d data items", data.length);
        self._addedCount += data.length;
      }
      cb();
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
              transmitQueue.call(self, self._bulkQueue, function() {
                self._bulkQueue = [];
                stream.resume();
              });
            } else {
              process.nextTick(function() { stream.resume(); });
            }
          }
        }
        self._seenCount++;
      };

      if (self._seenCount === 0) {
        if (self._config.schema) {
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
        transmitQueue.call(self, self._bulkQueue, function() {
          log("complete");
          log("added %d items, %d failures",self._addedCount, self._failedCount);
          cb();
        });
      }));
  };

  return JSONImporter;
}());