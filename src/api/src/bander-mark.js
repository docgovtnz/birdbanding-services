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

const getPaginatedManyToManySubresourceFromPGFn = (db, pathParameters = null, queryStringParameters = null, resourceDefinition) => {
  // ----------------------------------------------------------------------------   
  // This method is for the following resource structure:
  //        /<RESOURCE>/<RESOURCE-IDENTIFIER>/<SUB-RESOURCE>
  // Typical resourceDefinition structure:
  // { resources: ['<resource-name>', '<sub-resource-name>'], identifiers: ['<resource-identifier-name>'] }
  // Log the resource/subresource with the method call
  console.info(resourceDefinition.resources[0] + "->" + resourceDefinition.resources[1] + ".getManyToManySubresource()");

  let idValue = (pathParameters && pathParameters[resourceDefinition.identifiers[0]]) ? pathParameters[resourceDefinition.identifiers[0]] : null;
  // Initial pagination limit being set at 100
  let limit = (queryStringParameters && queryStringParameters.limit && parseInt(queryStringParameters.limit) <= 100) ? parseInt(queryStringParameters.limit) : 100;
  let paginationToken = (queryStringParameters && queryStringParameters.paginationToken) ? parseInt(queryStringParameters.paginationToken) : null;
  
  let functionName = `ro_${resourceDefinition.resources[0]}_${resourceDefinition.resources[1]}`;

  // THIS RETURNS A PROMISE
  return db[functionName](idValue, limit, paginationToken);
}

const updateManyToManyResource = (db, event, relationName, resourceDefinition) => {
  // ----------------------------------------------------------------------------   
  console.info(resourceDefinition.resources[0] + "->" + resourceDefinition.resources[1] + ".updateManyToManySubresource()");
  
  let primaryResourceIdName = `${resourceDefinition.resources[0]}_id`;
  let secondaryResourceIdName = `${resourceDefinition.resources[1]}_id`;
  let primaryIdValue = event.pathParameters[resourceDefinition.identifiers[0]];
  let body = JSON.parse(event.body);
  let params = body.map(secondaryResource => {
    return ({ [primaryResourceIdName]: primaryIdValue, [secondaryResourceIdName]: secondaryResource.id  });
  })

  // Complete insert -> if conflict with uniqueness constraint
  // A bander is already part of this project and therefore no further action required
  // We want to continue here because the desired outcome is already the case
  return db[relationName].insert(params, { onConflictIgnore: true, });
}

const formatManyToManySubresourceFromPGFn = (resultSet, resourceDefinition) => {
  // ----------------------------------------------------------------------------   
  console.info(resourceDefinition.resources[0] + "->" + resourceDefinition.resources[1] + ".formatManyToManySubresource()");

  // Postgres Functions given us flexibility to construct more complex queries than MassiveJS
  // The other benefit is that we also have control over the shape of the resultset, in most cases we expect minimal formatting to be required...
  return resultSet;
}

const validateBusinessRules = (body, resourceDefinition = null) => {
  // ----------------------------------------------------------------------------    
  console.info(resourceDefinition.resources[0] + "->" + resourceDefinition.resources[1] + ".validateBusinessRules()");
  return new Promise((resolve, reject) => {
    console.log('[TODO] VALIDATE BANDER<->MARK BUSINESS RULES');
    resolve([]);
  });
}

// GET PROJECTS FOR A GIVEN BANDER
module.exports.getMarks = (event, context, cb) => {
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

  console.log(JSON.stringify(event));

  // JSON Schemas
  var paramSchema = {};

  // Resources
  var resourceDefinition = [];

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(() => { return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version); })
    // Validate Path parameters
    .then(schema => {
      paramSchema = schema;
      console.info('Group membership authorisation OK. Validating path parameters...');
      return BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, paramSchema);
    })
    // Handle errors / Validate querystring      
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
      }
      console.info('Path parameters OK. Validating querysting parameters...');
      return BoilerPlate.validateJSON(event.queryStringParameters ? event.queryStringParameters : {}, paramSchema);
    })
    // Handle errors / Connect to DB
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('Querystring parameters OK. Analysing resource path...');
      return BoilerPlate.getResourcesAndIdentifiers(process.env.SERVICE_NAME, event);
    })
    .then(res => {
      resourceDefinition = res;
            // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Getting bander marks');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return getPaginatedManyToManySubresourceFromPGFn(db, event.pathParameters, event.queryStringParameters, resourceDefinition); })
    .then(res => { return formatManyToManySubresourceFromPGFn(res, resourceDefinition); })
    .then(res => { 
      let params = {
        data: res, path: event.path,
        queryStringParameters: event.queryStringParameters,
        multiValueQueryStringParameters: event.multiValueQueryStringParameters,
        paginationPointerArray: ['row_creation_idx'],
        maxLimit: 100, order: 'desc',
        count: -1, countType: 'TOTAL',
        countFromPageToEnd: false,
        isLastPage: false, prevPaginationToken: null
      }
      return BoilerPlate.generateIntegerPaginationFromArrayData(params);
    })
    .then(res => {
      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
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
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(err),
        "isBase64Encoded": false
      });
    });
};


// PUT (ASSIGN) PROJECTS TO A BANDER
module.exports.putMarkAllocations = (event, context, cb) => {
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
  var payloadSchemaParams = {
    table: process.env.PAYLOAD_SCHEMA_TABLE,
    id: process.env.PAYLOAD_SCHEMA_ID,
    version: Number(process.env.PAYLOAD_SCHEMA_VERSION)
  }

  // JSON Schemas
  var paramSchema = {};
  var payloadSchema = {};

  // Resources
  var resourceDefinition = [];

  // Do the actual work
  BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(() => { return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version); })
    // Validate Path parameters
    .then(schema => {
      paramSchema = schema;
      console.info('Group membership authorisation OK. Validating path parameters...');
      return BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, paramSchema);
    })
    // Handle errors / Validate querystring      
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
      }
      console.info('Path parameters OK. Validating querysting parameters...');
      return BoilerPlate.validateJSON(event.queryStringParameters ? event.queryStringParameters : {}, paramSchema);
    })
    // Handle errors / Validate payload
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('Path parameters OK. Validating payload...');
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      return BoilerPlate.validateJSON(JSON.parse(event.body), payloadSchema);
    })
    // Handle errors / Validate business rules
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body schema validation error(s)!`, errors);
      }
      console.info('Payload structure OK. Analysing resource path...');
      return BoilerPlate.getResourcesAndIdentifiers(process.env.SERVICE_NAME, event);
    })
    .then(res => {
      resourceDefinition = res;
      console.info('Resource path analysed. Validating business rules...');
      return validateBusinessRules(event.body, resourceDefinition);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      console.info('Business validation OK. Connecting to DB...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Adding projects to bander');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return updateManyToManyResource(db, event, process.env.RELATION_NAME, resourceDefinition);
    })
    .then(res => {
      console.info('Updated database - returning updated projects assigned to bander');
      return getPaginatedManyToManySubresourceFromPGFn(db, event.pathParameters, event.queryStringParameters, resourceDefinition); })
    .then(res => { return formatManyToManySubresourceFromPGFn(res, resourceDefinition); })
    .then(res => {
      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(res),
        "isBase64Encoded": false
      });
    })
    .catch(err => {
      // ----------------------------------------------
      console.error(err);
      cb(null, {
        "statusCode": err.statusCode || 500,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(err),
        "isBase64Encoded": false
      });
    });
};