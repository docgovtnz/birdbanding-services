'use strict';

// Modules
const Promise = require('bluebird');
const Util = require('util');
const _ = require('lodash')
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBSSHelpers = require('bb-spreadsheet-helpers');
const BBHelpers = require('bb-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
const Helpers = require('helpers.js')
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "event-validation";

// Imports - TODO REPLACE WITH LAMBDA LAYER
const validatePayloadStructure = require('event-validation-lib').validatePayloadStructure;
const validateUploadAccess = require('event-validation-lib').validateUploadAccess;
const validateUpdateAccess = require('event-validation-lib').validateUpdateAccess;
const validateGetAccess = require('event-validation-lib').validateGetAccess;
const validateEventExists = require('event-validation-lib').validateEventExists;
const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;
const getPostLookupData = require('event-validation-lib').getPostLookupData;
const getPutLookupData = require('event-validation-lib').getPutLookupData;
const supplementPostEventWithLookupData = require('event-validation-lib').supplementPostEventWithLookupData;
const supplementPutEventWithLookupData = require('event-validation-lib').supplementPutEventWithLookupData;
const validatePostBusinessRules = require('event-validation-lib').validatePostBusinessRules;
const validatePutBusinessRules = require('event-validation-lib').validatePutBusinessRules;


// The getDB method has been extracted from the searchDB method because of complications with pagination
// Pagination will require the separation of paginated entities (conveniently all 1:1 relations)
const getDB = (db, event) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getDB()");

  // Default pagination to the most recent records and submits empty criteria object
  let criteria = {};
  let idOperation = 'event.id =';
  criteria[idOperation] = event.pathParameters.eventId;

  console.log('Criteria: ', criteria);

  // THIS RETURNS A PROMISE
  return db.event
          .join({
            pk: 'id',
            bird: {
              type: 'LEFT OUTER',
              pk: 'id',
              on: { 'id': 'event.bird_id' },
              decomposeTo: 'object',
              species: {
                type: 'LEFT OUTER',
                pk: 'id',
                on: { 'id': 'bird.species_id' },
                decomposeTo: 'object'
              }
            },
            project: {
              type: 'LEFT OUTER',
              pk: 'id',
              on: { 'id': 'event.project_id' },
              decomposeTo: 'object'
            },
            event_owner: {
              type: 'LEFT OUTER',
              relation: 'bander',
              pk: 'id',
              on: { 'id': 'event.event_owner_id'},
              decomposeTo: 'object'
            },
            event_provider: {
              type: 'LEFT OUTER',
              relation: 'bander',
              pk: 'id',
              on: { 'id': 'event.event_provider_id'},
              decomposeTo: 'object'
            },
            event_reporter: {
              type: 'LEFT OUTER',
              relation: 'bander',
              pk: 'id',
              on: { 'id': 'event.event_reporter_id'},
              decomposeTo: 'object'
            },
            characteristic_measurement: {
              type: 'LEFT OUTER',
              relation: 'vw_labelled_characteristic_measurments',
              pk: 'id',
              on: { 'event_id': 'event.id' }
            },
            mark_configuration: {
              type: 'LEFT OUTER',
              pk: 'id',
              on: { 'event_id': 'event.id' }
            },
            mark_state: {
              type: 'LEFT OUTER',
              pk: 'id',
              on: { 'event_id': 'event.id' },
              mark: {
                type: 'LEFT OUTER',
                pk: 'id',
                on: { 'id': 'mark_state.mark_id' },
                decomposeTo: 'object'
              }
            },
            mark_allocation: {
              type: 'LEFT OUTER',
              pk: 'id',
              on: { 'event_id': 'event.id' }
            }
          })
          .find(criteria);
}


const formatResponse = (response) => {
  // --------------------------------------------------------
  console.info('Formatting response');
  // REMOVE VALIDATION PROPS FROM RESPONSE
  response.data = response.data.map(record => {
    // -------------------------------------

    // Loop through each sub-object
    for(const prop in record) {
      // Loop through the props in each sub-object
      for(const innerProp in record[prop]) {
        // ------------------------------------
        if(innerProp.includes('validation')) {
          delete record[prop][innerProp];
        }
      }

      // Map over each sub array
      if(Array.isArray(record[prop])) {
        record[prop] = record[prop].map(value => {
          // Delete any inner props used for validation
          for(const innerProp in value) {
            // ------------------------------------
            if(innerProp.includes('validation')) {
              delete value[innerProp];
            }
          }
          console.log(value);
          return value;
        });
      }
    }

    console.log(JSON.stringify(record));
    return record;
  });

  console.log(response);
  return response;
}

// POST
module.exports.post = (event, context, cb) => {
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

  console.log('DEBUG: ', JSON.stringify(event));

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

  var customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
  }

  // JSON Schemas
  var paramSchema = {};
  var mappingSchema = {};
  var payloadSchema = {};

  // Custom Errors
  var customErrorsList = {};

  // Payload
  var payload = JSON.parse(event.body);

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  // Validations
  let validations = [];

  // Lookup Data
  var lookupData = null;

  // Response Object
  var response = {
    errors: [],
    data: payload
  };  

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => {
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
      return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version); 
    })
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
    // Handle errors / Validate Claims
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('Querystring parameters OK. Processing validation names from query string before validating payload...');
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Connected to DB. Validating upload access...');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return validateUploadAccess(customErrorFactory, db, event.pathParameters.projectId, claims, governingCognitoGroup);
    })
    // Handle errors / Read workbook
    .then(errors => {
      if (errors.length > 0) {
        response.errors = errors;
        throw new BoilerPlate.ForbiddenError(`${errors.length} Authorisation validation error(s)!`, errors);
      }
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      let missingStructuralProps = validatePayloadStructure(payload);
      if(missingStructuralProps.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`Payload missing expected properties!`, `Missing required props: ${missingStructuralProps.join(', ')}`);
      }
      return BoilerPlate.validateJSON(payload, payloadSchema);
    })
    // Handle errors / Validate Claims
    .then(errors => {
      if (errors.length > 0) {
        // Format the errors and throw at this point to signify we aren't ready for business validation
        response.errors = formatSchemaErrors(customErrorFactory, errors, payload);
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload schema validation error(s)!`, errors);
      }
      console.info('Event batch payload OK. Validating business rules...');
      validations = ('multiValueQueryStringParameters' in event 
                      && event.multiValueQueryStringParameters 
                      && 'validationName' in event.multiValueQueryStringParameters 
                      && event.multiValueQueryStringParameters.validationName) 
                      ? event.multiValueQueryStringParameters.validationName
                      : [];

      return getPostLookupData(db, validations, response.data);
    })
    .then(res => {
      lookupData = res;
      response.data = supplementPostEventWithLookupData(validations, response.data, lookupData);
      console.log(response.data);
      return validatePostBusinessRules(customErrorFactory, claims, governingCognitoGroup, validations, response.data);
    })
    .then(errors => {
      response = formatResponse(response);
      response.errors = [...response.errors, ...errors];
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload business validation error(s)!`, errors);
      }
      return response;
    })
    .then(res => {
      response = res;

      console.info("Returning with no errors in validation process");
      cb(null, {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(response),
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
        "body": (response.errors.length > 0) ? JSON.stringify(response) : JSON.stringify(err),
        "isBase64Encoded": false
      });
    });
};


// PUT
module.exports.put = (event, context, cb) => {
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

  console.log('DEBUG: ', JSON.stringify(event));

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

  var customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
  }

  // JSON Schemas
  var paramSchema = {};
  var mappingSchema = {};
  var payloadSchema = {};

  // Payload
  var payload = JSON.parse(event.body);

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  // Validations
  let validations = ['event', 'markConfiguration', 'bird', 'bander', 'location'];

  // Lookup Data
  var lookupData = null;

  // Response Object
  var response = {
    errors: [],
    data: payload
  };  

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => {
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
      return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version); 
    })
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
    // Handle errors / Validate Claims
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('Querystring parameters OK. Processing validation names from query string before validating payload...');
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Connected to DB. Validating upload access...');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return validateUpdateAccess(customErrorFactory, db, event, JSON.parse(event.body)[0]['event']['project_id'], claims, governingCognitoGroup);
    })
    // Handle errors / Read workbook
    .then(errors => {
      if (errors.length > 0) {
        response.errors = errors;
        throw new BoilerPlate.ForbiddenError(`${errors.length} Authorisation validation error(s)!`, errors);
      }
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      return BoilerPlate.validateJSON(payload, payloadSchema);
    })
    // Handle errors / Validate Claims
    .then(errors => {
      if (errors.length > 0) {
        // Format the errors and throw at this point to signify we aren't ready for business validation
        response.errors = formatSchemaErrors(customErrorFactory, errors, payload);
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload schema validation error(s)!`, errors);
      }
      console.info('Event batch payload OK. Validating business rules...');
      return validateEventExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Event not found', error);
      return validateGetAccess(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      console.log(error);
      if (error) {
        throw new BoilerPlate.ForbiddenError(`Authorisation validation error(s)!!`, error);
      }
      return getDB(db, event); 
    })
    .then(res => {
      console.log(JSON.stringify(res));
      console.info('Getting remaining lookup data...');
      return getPutLookupData(db, validations, event, payload, res);
    })
    .then(res => {
      lookupData = res;
      response.data = supplementPutEventWithLookupData(validations, response.data, lookupData);
      return validatePutBusinessRules(customErrorFactory, claims, governingCognitoGroup, validations, response.data, lookupData.currentEvent);
    })
    .then(errors => {
      response = formatResponse(response);
      response.errors = [...response.errors, ...errors];
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload business validation error(s)!`, errors);
      }
      return response;
    })
    .then(res => {
      // -------------------------
      response = res;

      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(response),
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
        "body": (response.errors.length > 0) ? JSON.stringify(response) : JSON.stringify(err),
        "isBase64Encoded": false
      });
    });
}
