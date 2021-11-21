'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));



module.exports.action = (event, context, cb) => {

  console.log('Attempting reboot!');

  var rds = new AWS.RDS({apiVersion: '2014-10-31'});

  var params = {
    DBInstanceIdentifier: 'da1sey82ck7vqyf', /* required */
  };

  rds.rebootDBInstance(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(data);           // successful response
  });
}