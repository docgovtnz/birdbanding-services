'use strict';

const Promise = require('bluebird');
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

const RESOURCE_NAME = "characteristic";

const search = async (db, event) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".search()"); 

  let id = (event.pathParameters && 'characteristicId' in event.pathParameters && typeof event.pathParameters.characteristicId !== 'undefined') ?
          event.pathParameters.characteristicId : '0';
  let idOperation = (id !== '0') ? 'id =' : 'id >';



  // Build this up when we get significantly fancier with the 
  // breadth of querystring parameters we support. Right now,
  // we just return the whole lot.
  let criteria = {};
  criteria[idOperation] = parseInt(id);

  console.log(JSON.stringify(criteria));

  let resultSet = await db.characteristic.find(criteria, { order: [{field: 'name', direction: 'asc'}]});

  // Filter the resultset if the grouping search query parameter has been submitted
  if (event.queryStringParameters && 'group' in event.queryStringParameters && typeof event.queryStringParameters.group !== 'undefined') { 
    switch(event.queryStringParameters.group) {
      case 'ancillary': {
        resultSet = resultSet.filter(char => ![
            'bird name', 'age', 'sex', 'in status code',
            'out status code', 'in condition code', 'out condition code',
            'status details'
          ].includes(char.name))
        break;
      }
      default: {
        break;
      }
    }
  }

  console.log(JSON.stringify(resultSet));

  return resultSet;
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

// GET Characteristic
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
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Getting characteristic');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return search(db, event); 
    })
    // .then(res => { return formatResponse(res); }) 
    .then(res => {
      if (res.length === 0) throw new BoilerPlate.NotFoundError('CharacteristicNotFound', { type: 'NOT_FOUND', message: `Characteristic cannot be found with this id`, data: { path: `pathParameters.characteristicId` }, value: event.pathParameters.characteristicId , schema: null });

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

// SEARCH Characteristics
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
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Seaching characteristics');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return search(db, event); 
    })
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