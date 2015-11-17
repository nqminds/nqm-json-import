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
  var parser = require("mongo-parse");
  var moment = require("moment");
  var _ = require("lodash");
  var nqmUtils = require("nqm-utils");

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

  var transformForUpsert = function(data) {
    var self = this;
    var obj = {
      __update: []
    };

    var setField = function(field, data, arrIndices) {
      var sanitised;
      if (self._fieldMap.hasOwnProperty(field)) {
        sanitised = helpers.utils.sanitiseData(self._fieldMap[field].type, data);
      } else {
        sanitised = data;
      }
      // If the target is part of the primary key add it as a property.
      if (self._config.primaryKey.indexOf(field) >= 0) {
        helpers.utils.setData(obj, field, sanitised, self._fieldMap[field].isArray);
      } else {
        // Do upsert of value.
        var update = {
          m: "r",             // replace
          p: field + (arrIndices ? "." + arrIndices : ""),
          v: sanitised
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

  var startProcessing = function(data, cb) {
    var self = this;

    var send = data.splice(0, this._config.bulkCount);
    if (send.length > 0) {
      send = _.map(send, function(i) { return transformForUpsert.call(self, i); });
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
      log("parsed %d items, %d failures",self._addedCount, self._failedCount);
      cb();
    }
  };

  JSONImporter.prototype.start = function(config, cb) {
    var self = this;
    this._config = config;

    this._addedCount = this._failedCount = 0;

    self._config.commandHost = self._config.commandHost || "http://cmd.nqminds.com";
    self._config.datasetName = self._config.datasetName || (path.basename(self._config.sourceFile, path.extname(self._config.sourceFile)) + " [" + moment().format("D MMM YYYY H:mm:ss") + "]");
    self._config.bulkCount = self._config.bulkCount || 100;
    self._config.primaryKey = self._config.primaryKey || [];
    self._config.inferSchema = self._config.inferSchema || !self._config.schema;
    self._config.upsertMode = self._config.upsertMode !== false;

    if (self._config.upsertMode && self._config.primaryKey.length === 0) {
      log("forcing non-upsert mode since there is no primary key defined");
      self._config.upsertMode = false;
    }

    var contents = fs.readFileSync(config.sourceFile);
    var data = JSON.parse(contents);

    var pointers = parser.DotNotationPointers(data, config.dataPath);
    if (pointers.length > 0) {
      data = pointers[0].val;
      if (!Array.isArray(data)) {
        data = [data];
      }

      if (self._config.schema) {
        self._fieldMap = helpers.utils.createFieldMap(self._config.schema);
        getAccessToken.call(self, data, cb);
      } else if (self._config.inferSchema) {
        self._config.schema = inferSchema(data[0]);
        self._fieldMap = helpers.utils.createFieldMap(self._config.schema);
        getAccessToken.call(self, data, cb);
      }
    } else {
      log("failed to get data using path " + config.dataPath);
      cb(new Error("no data found using " + config.dataPath));
    }
  };

  return JSONImporter;
}());