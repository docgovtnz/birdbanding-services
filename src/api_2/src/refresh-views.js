'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;

const promiseTimeout = (timeout) => {
  return new Promise((resolve, reject) => {
    setTimeout(() => { console.log('returning'); resolve('matview refresh kicked off'); }, timeout);
  });
};

// SEARCH
module.exports.handler = (event, context, cb) => {
  // ----------------------------------------------------------------------------
  context.callbackWaitsForEmptyEventLoop = false;

  if (typeof containerCreationTimestamp === 'undefined') {
    containerCreationTimestamp = Moment();
  }
  console.info(Moment().diff(containerCreationTimestamp, 'seconds') + ' seconds since container established...')

  console.log(JSON.stringify(event));

  return DBAccess.getDBConnection(db, dbCreationTimestamp)
    .then(dbAccess => {
      console.info('Refreshing materialized views in parallel');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      console.log(db);

      return db.ro_is_matview_refreshing(event.materializedViewFunctionName);
    })
    .then(res => {
      console.log(res);

      if (res[0].ro_is_matview_refreshing) {
        // ---------------------------------
        // Matview already refreshing - skip to avoid unstable queuing
        console.log('Matview already being refreshed... postponing refresh');
        return cb(null, {
          status: 'postponed'
        });
      }
      else {
        db[event.materializedViewFunctionName]();
        console.log('Matview refreshed triggered, returning...');
      }      

      // Delay to allow for function to kickoff reliably
      return promiseTimeout(2000)
        .then(res => {
          return cb(null, {
            status: 'triggered'
          });
        });
    })
};