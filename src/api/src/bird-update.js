'use strict';

// Modules
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBHelpers = require('bb-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "bird";

const validateIsAuthorisedForBird = async (governingCognitoGroup, claims, customErrorFactory, lookupData) => {
  // -----------------------------------------
  console.info(RESOURCE_NAME + ".validateIsAuthorisedForBird()");

  // -----------------------------------------
  // IF THE USER IS NOT ADMIN AND NOT ONE OF THE PREVIOUS PMs, OWNER/REPORTER/PROVIDERS FOR THIS BIRD, RETURN UNATHORISED ERROR
  // -----------------------------------------
  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME
      && !lookupData.bird.vw_distinct_bird_banders.project_managers.includes(claims.sub)
      && !lookupData.bird.vw_distinct_bird_banders.event_owners.includes(claims.sub)
      && !lookupData.bird.vw_distinct_bird_banders.event_providers.includes(claims.sub)
      && !lookupData.bird.vw_distinct_bird_banders.event_reporters.includes(claims.sub)) {
    return [customErrorFactory.getError('ForbiddenError', [`executing this action`, claims.sub, 'claims.sub'])];
  }
  else {
    return [];
  }
}

const getPutLookupData = async (customErrorFactory, event, claims, governingCognitoGroup, payload) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".getPutLookupData()");

  console.log(event.pathParameters.birdId);

  let birdLookupResultset = await db.bird
    .join({
      vw_distinct_bird_banders: {
        type: 'INNER',
        pk: 'id',
        on: { 'id': 'bird.id' },
        decomposeTo: 'object'
      }
    })
    .find({
      'id =': event.pathParameters.birdId
    });

  console.log(birdLookupResultset);

  if (birdLookupResultset.length === 0) {
    throw new BoilerPlate.NotFoundError('Bird not found', customErrorFactory.getError('NotFoundError', ['birdId', event.pathParameters.birdId, 'pathParameters.birdId']));
  }

  let proposedSpeciesCode = JSON.parse(event.body).species_code_nznbbs;
  console.log(proposedSpeciesCode);

  let speciesLookupResultset = await db.species
    .find({
      'species_code_nznbbs =': proposedSpeciesCode
    });

  return {
    bird: birdLookupResultset[0],
    species: (speciesLookupResultset.length > 0) ? speciesLookupResultset[0] : null
  };
}

const validatePutBusinessRules = async (customErrorFactory, event, lookupData, claims, governingCognitoGroup, payload) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".validatePutBusinessRules()");

  let errors = [];

  console.log(JSON.stringify(lookupData));

  if (!lookupData.species) {
    let proposedSpeciesCode = JSON.parse(event.body).species_code_nznbbs;
    errors.push(customErrorFactory.getError('InvalidSpeciesCode', [proposedSpeciesCode, '/species_code_nznbbs', 'this record']));
  }

  return errors;
}

const putBirdToDB = (db, payload, lookupData, event) => {
  // ----------------------------------------------------------------------------   
  console.info('.putBirdToDB');

  console.log(JSON.stringify(payload));

  let birdId = event.pathParameters.birdId;

  return db.withTransaction(async tx => {
    // -----------------------------------

    // ------------------------------------------------------------------------------------------------------
    // 1) UPDATE THE BIRD WITH THE PROPOSED FRIENDLY_NAME AND SPECIES_ID
    // ------------------------------------------------------------------------------------------------------
    let birdUpdate = await tx.bird.update({
      'id =': birdId
    },
    {
      'friendly_name': payload.friendly_name,
      'species_id': lookupData.species.id
    })

    return birdUpdate;
  })
}

const getUpdateFromDB = async (db, payload, lookupData, event) => {
  // ----------------------------------------------------------------------------   
  console.info('.getUpdateFromDB');

  let returnSet = await db.bird.find({
      'id =': event.pathParameters.birdId
    });

  return returnSet[0];
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
      // Migrate to bb helpers
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return getPutLookupData(customErrorFactory, event, claims, governingCognitoGroup, payload);
    })
    .then(res => {
      lookupData = res;
      return validateIsAuthorisedForBird(governingCognitoGroup, claims, customErrorFactory, lookupData);
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
      return validatePutBusinessRules(customErrorFactory, event, lookupData, claims, governingCognitoGroup, payload);
    })
    .then(errors => {
      response.errors = [...response.errors, ...errors];
      if (errors.filter(error => error.severity === 'CRITICAL').length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload business validation error(s)!`, errors);
      }
      return putBirdToDB(db, payload, lookupData, event);
    })
    .then(res => {
      return getUpdateFromDB(db, res, lookupData, event)
    })
    .then(res => {
      response.data = res;
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
