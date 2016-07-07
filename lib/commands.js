/**
 * Created by toby on 16/11/15.
 */

(function(exports) {
  "use strict";

  var util = require("util");
  var request = require("request");
  var _ = require("lodash");

  var handleError = function(err, response, log, cb) {
    if (err || response.statusCode !== 200 || (response.body && response.body.error)) {
      if (!err) {
        err = new Error("[status code " + response.statusCode + "] " + ((response.body && response.body.error) || "unknown error"));
      }
      err.statusCode = response ? response.statusCode : 500;
      log("failure [%s]", err.message);
      cb(err);
      // Error handled.
      return true;    
    } else {
      // No error.
      return false;
    }
  };

  exports.getAccessToken = (function() {
    var log = require("debug")("getAccessToken");

    function getToken(commandHost, credentials, cb) {
      var url = util.format("%s/token", commandHost);
      request({ url: url, method: "post", headers: { "authorization": "Basic " + credentials }, json: true, body: { grant_type: "client_credentials" } }, function(err, response, content) {
        if (!handleError(err, response, log, cb)) {
          log("result from server: %j", response.body);
          cb(null, response.body.access_token);
        }
      });
    }

    return getToken;
  }());

  exports.createTargetDataset = (function() {
    var log = require("debug")("createTargetDataset");

    function createTargetDataset(commandHost, accessToken, targetFolder, name, basedOn, schema, primaryKey, cb) {
      var url = util.format("%s/commandSync/resource/create", commandHost);
      var data = {};
      data.parentId = targetFolder;
      data.name = name;
      data.basedOnSchema = basedOn;
      data.schema = { dataSchema: schema };
      if (primaryKey && primaryKey.length > 0) {
        data.schema.uniqueIndex = _.map(primaryKey, function(key) { return { asc: key }; } );
      }
      log("sending create dataset [%j]",data);
      request({ url: url, method: "post", headers: { authorization: "Bearer " + accessToken }, json: true, body: data }, function(err, response, content) {
        if (!handleError(err, response, log, cb)) {
          log("result from server: %j", response.body);
          cb(null, response.body.response.id);
        }
      });
    }

    return createTargetDataset;
  }());

  /*
   * Helper to get a dataset using the nqmHub query api.
   */
  exports.getDataset = (function() {
    var log = require("debug")("getDataset");

    function getDS(queryHost, accessToken, datasetId, cb) {
      var url = util.format("%s/datasets/%s", queryHost, datasetId);
      request({ method: "get", url: url, headers: { authorization: "Bearer " + accessToken }, json: true }, function(err, response, content) {
        if (!handleError(err, response, log, cb)) {
          cb(null, content);
        }
      });
    }

    return getDS;
  }());

  /*
   * Helper to add data to a dataset using the nqmHub command api.
   */
  exports.addDatasetDataBulk = (function() {
    var log = require("debug")("addDatasetDataBulk");

    function addDataBulk(commandHost, accessToken, datasetId, data, cb) {
      var url = util.format("%s/commandSync/dataset/data/createMany", commandHost);
      var bulk = {};
      bulk.datasetId = datasetId;
      bulk.payload = data;
      var requestOptions =  { url: url, timeout: 3600000, method: "post",  headers: { authorization: "Bearer " + accessToken }, json: true, body: bulk };
      log("sending createMany [%d - %d bytes] to %s using token %s",data.length, JSON.stringify(data).length, url, accessToken);
      request(requestOptions, function(err, response, content) {
        if (!handleError(err, response, log, cb)) {
          log("result from server: %j", response.body);
          cb(null);
        }
      });
    }

    return addDataBulk;
  }());

  /*
   * Helper to upsert data to a dataset using the nqmHub command api.
   */
  exports.upsertDatasetDataBulk = (function() {
    var log = require("debug")("upsertDatasetDataBulk");

    function upsertDataBulk(commandHost, accessToken, datasetId, data, cb) {
      var url = util.format("%s/commandSync/dataset/data/upsertMany", commandHost);
      var bulk = {};
      bulk.datasetId = datasetId;
      bulk.payload = data;
      log("sending upsertMany [%d - %d bytes]",data.length, JSON.stringify(data).length);
      request({ url: url, timeout: 3600000, method: "post", headers: { authorization: "Bearer " + accessToken }, json: true, body: bulk }, function(err, response, content) {
        if (!handleError(err, response, log, cb)) {
          log("result from server: %j", response.body);
          cb(null);
        }
      });
    }

    return upsertDataBulk;
  }());

}(module.exports));

