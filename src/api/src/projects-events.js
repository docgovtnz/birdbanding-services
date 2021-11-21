'use strict';

// Modules
const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const MomentTimezone = require('moment-timezone');
const _ = require('lodash');
const uuidv4 = require('uuid/v4');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBSSHelpers = require('bb-spreadsheet-helpers');
const BBHelpers = require('bb-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));


// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;


const RESOURCE_NAME = "project-event";

// Imports - TODO REPLACE WITH LAMBDA LAYER
const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;


const validateEventProjectUpdateAccess = (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // ---------------------------------------------------------------------------- 
  console.info("validateEventProjectUpdateAccess()");

  var errors = [];

  // The Authorisation logic is fairly complex, and contained 
  // almost entirely in the database, therefore a complex stored
  // procedure does the work for us. Alternatively, we could make multiple
  // calls to the DB, get all the data we need, then perform the logic
  // here. That'd be more straightforward (and flexible), but also less
  // optimal.

  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
    // Admin group, permitted to update projectIds
    console.log(`User has admin group permissions - continuing update process`)
    return [];
  } 
  else {
    errors.push(customErrorFactory.getError('UpdateAuthorisationError', [governingCognitoGroup, 'userGroup']));
    // Return errors array
    console.log(`Found ${errors.length} metadata validation error(s).`);
    return errors;
  };
};


const validateEventProjectBusinessRules = async (customErrorFactory, db, event) => {
  // ---------------------------------------------------------------------------- 
  console.info("validateEventProjectBusinessRules()");

  const projectEventUpdateLimit = 500;

  var errors = [];
  let payload = JSON.parse(event.body);

  // 1) Validate that projectId exists
  let projectExistsResultset = await db.ro_is_project(event.pathParameters.projectId);
  if (!projectExistsResultset[0].ro_is_project) { 
    errors.push(customErrorFactory.getError('NotFoundError', ['projectId', event.pathParameters.projectId, 'pathParameters.projectId']));
  }

  // 2) Validate that the eventIds submitted all exist
  let eventsExistResultset = await db.ro_are_events([payload.events]);
  if (!eventsExistResultset[0].ro_are_events) { 
    errors.push(customErrorFactory.getError('NotFoundError', ['One of eventIds', `[${payload.events}] duplicated or`, `/events`]));
  }

  // 3) Validate that the maximum number of events to update is not exceeded
  if (payload.events.length > projectEventUpdateLimit) {
    errors.push(customErrorFactory.getError('LimitExceedanceError', [`Number of events to update exceeds project-event-update api limit of ${projectEventUpdateLimit}`, '', payload.events.length, 'events.length']));
  }

  return errors;
};


const putEventToDB = (db, event) => {
  // ----------------------------------------------------------------------------   
  console.info('Putting record to DB');

  let payload = JSON.parse(event.body);

  return db.event.update({
    or: payload.events.map(eventId => { return { 'id =': eventId }; })
  },
  {
    project_id: event.pathParameters.projectId
  });
}


// ============================================================================
// API METHODS
// ============================================================================

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
  var payloadSchema = {};

  // Payload
  var payload = JSON.parse(event.body);

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

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
      // Migrate to bb helpers
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return validateEventProjectUpdateAccess(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    // Handle errors / Read workbook
    .then(errors => {
      if (errors.length > 0) {
        let formattedErrors = errors;
        throw new BoilerPlate.ForbiddenError(`${errors.length} Authorisation validation error(s)!`, formattedErrors);
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
        let formattedErrors = formatSchemaErrors(customErrorFactory, errors, payload);
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload schema validation error(s)!`, formattedErrors);
      }
      console.info('Event batch payload OK. Validating business rules...');
      return validateEventProjectBusinessRules(customErrorFactory, db, event, claims);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload business validation error(s)!`, errors);
      }
      return putEventToDB(db, event);
    })
    .then(res => {
      // Complete a fresh pull of the new events from the database
      console.log(JSON.stringify(res));

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
}
