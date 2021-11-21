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

const RESOURCE_NAME = "species-groups";

const search = (db, id='0') => {
// ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".search()"); 

  let idOperation = (id !== '0') ? 'id =' : 'id >';

  // Build this up when we get significantly fancier with the 
  // breadth of querystring parameters we support. Right now,
  // we just return the whole lot.
  let criteria = {};
  criteria[idOperation] = parseInt(id);

  // THIS RETURNS A PROMISE
  return db.species_group.find(criteria, { order: [{field: 'name', direction: 'asc'}]});
}

 
const formatResponse = (res) => {
// ----------------------------------------------------------------------------    
  return new Promise((resolve, reject) => {

    if(res){
      var promises = [];
      res.forEach(ar =>{            
        promises.push(ar.flop('SHORT'));
      });

      return Promise.all(promises)
        .then(res => {
          resolve(res);
        })
        .catch(err => {
          return reject(err);
        });
    }
  })
};

// ============================================================================
// API METHODS
// ============================================================================

// GET
module.exports.get = (event, context, cb) => {
// ----------------------------------------------------------------------------
  context.callbackWaitsForEmptyEventLoop = false;

  if (typeof containerCreationTimestamp === 'undefined') {
    containerCreationTimestamp = Moment();
  }
  console.info(Moment().diff(containerCreationTimestamp, 'seconds') + ' seconds since container established...')

  // Respond to a ping request 
  if ('source' in event && event.source === 'serverless-plugin-warmup') {
    console.log('Lambda PINGED.');
    return cb(null, 'Lambda PINGED.');
  }

  // Get the schema details for parameter validation
  var parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  // JSON Schemas
  var paramSchema = {};

  // Do the actual work
  return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version)
    // Validate Path parameters
    .then( schema => { 
      paramSchema = schema;
      console.info('Group membership authorisation OK. Validating path parameters...');
      return BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, paramSchema); })
    // Handle errors / Validate querystring      
    .then( errors => { 
      if(errors.length > 0){ 
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
      }
      console.info('Path parameters OK. Validating querysting parameters...');
      return BoilerPlate.validateJSON(event.queryStringParameters ? event.queryStringParameters : {}, paramSchema);
    })
    // Handle errors / Validate Claims
    .then( errors => { 
      if(errors.length > 0){ 
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('Querystring parameters OK. Connecting to DB...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Getting species group');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return search(db, event.pathParameters.speciesGroupId); })  
    .then(res => {
      if (res.length === 0) throw new BoilerPlate.NotFoundError('SpeciesGroupNotFound', { type: 'NOT_FOUND', message: `Species group cannot be found with this id`, data: { path: `pathParameters.speciesGroupId` }, value: event.pathParameters.speciesGroupId , schema: null });

      console.info("Returning with no errors");
        cb(null, {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin" : "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials" : true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(res[0]),
        "isBase64Encoded": false        
      });
    })
    .catch(err => {
      console.error(err);
      cb(null, {
        "statusCode": err.statusCode || 500,
        "headers": {
          "Access-Control-Allow-Origin" : "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials" : true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(err),
        "isBase64Encoded": false  
      });
    });
  };

// SEARCH
module.exports.search = (event, context, cb) => {
// ----------------------------------------------------------------------------
  context.callbackWaitsForEmptyEventLoop = false;

  if (typeof containerCreationTimestamp === 'undefined') {
    containerCreationTimestamp = Moment();
  }
  console.info(Moment().diff(containerCreationTimestamp, 'seconds') + ' seconds since container established...')

  // Respond to a ping request 
  if ('source' in event && event.source === 'serverless-plugin-warmup') {
    console.log('Lambda PINGED.');
    return cb(null, 'Lambda PINGED.');
  }

  // Get the schema details for parameter validation
  var parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  // JSON Schemas
  var paramSchema = {};

  // Do the actual work
  return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version)
    // Validate Path parameters
    .then( schema => { 
      paramSchema = schema;
      console.info('Group membership authorisation OK. Validating path parameters...');
      return BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, paramSchema); })
    // Handle errors / Validate querystring      
    .then( errors => { 
      if(errors.length > 0){ 
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
      }
      console.info('Path parameters OK. Validating querysting parameters...');
      return BoilerPlate.validateJSON(event.queryStringParameters ? event.queryStringParameters : {}, paramSchema);
    })
    // Handle errors / Validate Claims
    .then( errors => { 
      if(errors.length > 0){ 
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('Querystring parameters OK. Connecting to DB...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Searching projects');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return search(db); })  
    .then(res => {
      console.info("Returning with no errors");
        cb(null, {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin" : "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials" : true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(res),
        "isBase64Encoded": false        
      });
    })
    .catch(err => {
      console.error(err);
      cb(null, {
        "statusCode": err.statusCode || 500,
        "headers": {
          "Access-Control-Allow-Origin" : "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials" : true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(err),
        "isBase64Encoded": false  
      });
    });
};