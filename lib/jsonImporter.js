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
      log("requesting access token");
      commands.getAccessToken(self._config.commandHost, self._config.credentials, function(err, accessToken) {
        if (!err) {
          self._accessToken = accessToken;
        }
        cb(err);
      });
    } else {
      log("getAccessToken: no credentials");
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

  var transmitQueue = function(data, cb, retrying) {
    var self = this;

    if (self._config.upsertMode) {
      data = _.map(data, function(d) { return transformForUpsert.call(self, d); });
    }

    var bulkLoad = self._config.upsertMode ? commands.upsertDatasetDataBulk : commands.addDatasetDataBulk;
    bulkLoad(self._config.commandHost, self._accessToken, self._config.targetDataset, data, function(err) {
      if (err) {
        if (err.statusCode === 401 && !retrying) {
          // Possible token expiry - attempt to re-acquire a token and try again.
          log("failed with 401 (authorisation) - retrying");
          self._accessToken = "";
          getAccessToken.call(self, function(err) {
            if (err) {
              return cb(err);
            }                                   
            // Got a new access token => try to transmit again.
            transmitQueue.call(self, data, cb, true);
          });          
          // Return here, i.e. don't fall-through and callback until getAccessToken has completed.
          return;
        } else {
          // Review - failed to add items, continue with other items or abort?
          log("failed to write %d items [%s]", data.length, err.message);
          self._failedCount += data.length;          
        }
      } else {
        debug("added %d data items", data.length);
        self._addedCount += data.length;
      }
      cb(err);
    });
  };

  function processJSONObject() {
    var self = this;

    var queueData = function(err, stream, data) {
      if (err) {
        log("queueData: " + err.message);
        process.exit(-1);
      } else {
        if (self._seenCount < self._config.startAt) {
          log("ignoring object %d, starting at %d", self._seenCount, self._config.startAt);
        } else if (self._config.endAt >= 0 && self._seenCount >= self._config.endAt) {
          log("ignoring object %d, ended at %d", self._seenCount, self._config.endAt);
          stream.emit("end");
        } else {
          debug("object %d is %d bytes", self._seenCount, JSON.stringify(data).length);
          self._bulkQueue.push(data);
          if (self._bulkQueue.length >= self._config.bulkCount) {
            stream.pause();
            transmitQueue.call(self, self._bulkQueue, function(err) {
              if (err) {
                log("processJSONObject error: " + err.message);
                process.exit(-1);
              } else {
                self._bulkQueue = [];
                stream.resume();                  
              }
            });
          } 
        }
      }
      self._seenCount++;
    };
    
    var getTargetDataset = function(err, stream, data) {
      if (err) {
        return queueData(err, stream, data);
      } else {
        if (!self._config.targetDataset) {
          // No target dataset given => create one.
          commands.createTargetDataset(
            self._config.commandHost,
            self._accessToken,
            self._config.targetFolder,
            self._config.datasetName,
            self._config.basedOnSchema,
            self._config.schema || {},
            self._config.primaryKey,
            function(err, ds) {
              if (err) {
                return queueData(err, stream, data);
              }
              // Add delay to allow time for projection to catch up with event.
              setTimeout(function() {
                // Cache the newly created target dataset.
                self._config.targetDataset = ds;
                // Now get the full dataset object from the TDX - this will include the full schema and primary key.
                getTargetDataset(err, stream, data);
              },10000);
            });
        } else {
          // Get target dataset details.
          commands.getDataset(self._config.queryHost, self._accessToken, self._config.targetDataset, function(err, ds) {
            if (err) {
              return queueData(err, stream, data);
            }
            // Cache dataset schema and primary key.
            self._config.schema = ds.schemaDefinition.dataSchema;
            self._config.primaryKey = ds.schemaDefinition.uniqueIndex.map(function(idx) { return idx.asc || idx.desc });

            if (self._config.upsertMode && self._config.primaryKey.length === 0) {
              log("forcing non-upsert mode since there is no primary key defined");
              self._config.upsertMode = false;
            }
            
            queueData(null, stream, data);
            stream.resume();                  
          });
        }                  
      }
    };

    var writeFunc = function(data) {
      var stream = this;

      if (self._seenCount === 0) {
        stream.pause();        
        // If a schema is defined, or we are updating an existing dataset, proceed to acquiring the access token.
        if (!self._config.inferSchema) {
          getAccessToken.call(self, function(err) {
            getTargetDataset(err, stream, data);
          });
        } else {
          self._config.schema = inferSchema(data);
          log("inferred schema is %j", self._config.schema);
          getAccessToken.call(self, function(err) {
            getTargetDataset(err, stream, data); 
          });
        }
      } else {
        queueData(null, stream, data);
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
    self._config.queryHost = self._config.queryHost || "https://q.nqminds.com/v1";
    self._config.datasetName = self._config.datasetName || (path.basename(self._config.sourceFile, path.extname(self._config.sourceFile)) + " [" + moment().format("D MMM YYYY H:mm:ss") + "]");
    self._config.bulkCount = self._config.bulkCount || 100;
    self._config.primaryKey = self._config.primaryKey || [];
    self._config.inferSchema = self._config.inferSchema === true;
    self._config.upsertMode = self._config.upsertMode || false;
    self._config.startAt = self._config.startAt || 0;
    self._config.endAt = self._config.endAt || -1;
    self._config.basedOnSchema = self._config.basedOnSchema || "dataset";

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