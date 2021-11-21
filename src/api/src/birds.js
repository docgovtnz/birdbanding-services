'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
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

const RESOURCE_NAME = "bird";

const putEventToDB = (db, payload, pathParams, id = null) => {
  // ----------------------------------------------------------------------------   
  return new Promise((resolve, reject) => {

    console.info(RESOURCE_NAME + ".putEventToDB()");

    // // We've either got a definite ID to find, or we want all of them
    // let idOperation = id ? 'id =' : 'id BETWEEN';
    // let idValue = id ? id : ['00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff'];
    let response = {};

    // Insert the component records within a transaction so we can rollback on failure
    return db.withTransaction(tx => {
      // Save the event object. This will return a new
      // event with a UUID.
      let txPromise = tx.event.save(payload.event);

      // Bird
      // -----------------------------------
      txPromise = txPromise.then(event => {
        response.event = event;
      
        // Create a Bird Entity if appropriate
        if(payload.event.event_type == '1'){
          
          // We need to create a new bird entity. If we
          // don't have details about at least the
          // bird species, we can't.
          if(!payload.bird){
            return Promise.reject(new Error("First marking event does not include bird data!"));
          }

          // Update the bird entity to reference the event ID
          payload.bird.first_marking_event_id = response.event.id;
          return tx.bird.insert(payload.bird);
        }
        else{
          return Promise.resolve(null);
        }
      })
      .then(bird => {
      // Mark Configuration
      // -----------------------------------
        response.bird = bird;

        if(payload.mark_configuration){
          // Update the Event ID
          payload.mark_configuration.forEach(mark => {
            mark.event_id = response.event.id;
          })

          // Write the Mark Configuration records to the DB
          return tx.mark_configuration.insert(payload.mark_configuration);
        }
        else{
          return Promise.resolve([]);
        }
      })
      .then(mark_configuration => {
      // Characteristic Measurements
      // -----------------------------------
        response.mark_configuration = mark_configuration;

        if(payload.characteristic_measurements){
          // Update the Event ID
          payload.characteristic_measurements.forEach(measurement => {
            measurement.event_id = response.event.id;
          })

          // Write the Characteristic Measurements
          return tx.characteristic_measurement.insert(payload.characteristic_measurements);
        }
        else{
          return Promise.resolve([]);
        }
      })
      .then(characteristic_measurements => {
        // Finally Update the Event
        // -----------------------------------        
        response.characteristic_measurements = characteristic_measurements;

        // Update the event with the bird ID of the bird we created earlier (if present)
        response.event.bird_id = (response.bird && response.bird.id) ? response.bird.id : null;
        return tx.event.save(response.event);
      })
      .then(event => {
        // Finish
        // -----------------------------------          
        response.event = event;
        return resolve(response);
      })
      .catch(err => {
        console.info("Catch02");
        return reject(err);
      });
    })
  });
}

const searchDB = (db, pathParameters = null, queryStringParameters = null) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".searchDB()");

  // We've either got a definite ID to find, or we want all of them
  let id = (pathParameters && pathParameters.birdId) ? pathParameters.birdId : null;
  // Initial pagination limit being set at 100
  let limit = (queryStringParameters && queryStringParameters.limit && parseInt(queryStringParameters.limit) <= 100) ? parseInt(queryStringParameters.limit) : 100;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
                             parseInt(queryStringParameters.paginationToken) : 0;

  // QueryString Parameters

  // All QueryStrings are ANDed together. It's up to the caller
  // to provide a combination of parameters that makes sense.

  // Band Prefix and Short Number
  // Need to get the BandId

  // BandId
  // SELECT * from bands where Band ID = bandID
  let idValue = id ? id : paginationToken;

  // Default pagination to the most recent records and submits empty criteria object
  let criteria = {};
  let idOperation;
  if (id) {
    idOperation = 'bird.id =';
    criteria[idOperation] = idValue;
  }
  else {
    // Pagination on a join is slightly trickier (cannot use limit due to amalgamation of resultset)
    idOperation = 'bird.row_creation_idx BETWEEN';
    criteria[idOperation] = [(idValue + 1), (idValue + limit)];
  }

  // THIS RETURNS A PROMISE
  return db.bird
    .join({
      pk: 'id',
      event: {
        type: 'LEFT OUTER',
        pk: 'id',
        on: { 'bird_id': 'bird.id' },
        mark_configuration: {
          type: 'LEFT OUTER',
          pk: 'id',
          on: { 'event_id': 'event.id' },
        },
        characteristic_measurement: {
          type: 'LEFT OUTER',
          pk: 'id',
          on: { 'event_id': 'event.id' },
          characteristic: {
            type: 'LEFT OUTER',
            pk: 'id',
            on: { 'id': 'characteristic_measurement.characteristic_id' },
            decomposeTo: 'object',
          }
        }
      },
      species: {
        type: 'LEFT OUTER',
        pk: 'id',
        on: { 'id': 'bird.species_id' },
        decomposeTo: 'object',
        species_group_membership: {
          type: 'LEFT OUTER',
          pk: 'id',
          on: { 'species_id': 'species.id' },
          decomposeTo: 'object',
          species_group: {
            type: 'LEFT OUTER',
            pk: 'id',
            on: { 'id': 'species_group_membership.group_id' },
            decomposeTo: 'object',
          }
        }
      },
    })
    .find(criteria, 
    { 
      order: [{ field: 'bird.row_creation_idx', direction: 'asc' }],
    })
}

const getBirdSupportingCalculations = async (db, event) => {
  // -----------------------------------------------
  console.info(RESOURCE_NAME + ".getBirdDistanceSummary()");

  let birdId = (event.pathParameters && event.pathParameters.birdId) ? event.pathParameters.birdId : null;

  let travelSummary = await db.ro_bird_travel_timeline(birdId);

  // 1) linear distance between first and last sighting (regardless of how far it went in between)
  //  Could have flown all the way to Alaska and back, but if banded and resighted at Miranda, distance may be <1km)
  let deltaFirstAndLastSightingKmResultSet = (await db.ro_bird_location_delta_first_to_last(birdId));
  let deltaFirstAndLastSightingKm = (deltaFirstAndLastSightingKmResultSet[0].ro_bird_location_delta_first_to_last !== null) ? deltaFirstAndLastSightingKmResultSet[0].ro_bird_location_delta_first_to_last / 1000 : null;

  // 2) linear distance moved since the last sighting
  //  just the distance between the second-last and last sightings
  let deltaMostRecentSightingKmResultSet = await db.ro_bird_location_delta_most_recent(birdId);
  let deltaMostRecentSightingKm = (deltaMostRecentSightingKmResultSet[0].ro_bird_location_delta_most_recent !== null) ? deltaMostRecentSightingKmResultSet[0].ro_bird_location_delta_most_recent / 1000 : null;

	//  3) cumulative distance tallied up between all successive movements for all sightings
  //   e.g. to Alaska and back, giving 22,000 km for the same bird
  //   would need date calculation
  let cumulativeDistanceKm = (travelSummary.filter(travelDetail => travelDetail.distance !== null).length > 0) ? travelSummary.reduce((accumulator, current) => {
    // -----------------------------
    console.log(JSON.stringify(current));
    if (current.distance) {
      accumulator += current.distance;
    }
    return accumulator;
  }, 0) / 1000 : null;

	
	// 4) dispersal/max distance: the distance between the furthest two points plotted for a bird
	//    regardless of how many points plot for a bird, give the distance of the two that are the furthest apart
	//    you'd need to calculate the distance from every point to every other point = n(n-1)/2 calculations and then return the max of these
  let dispersalDistanceKm = (travelSummary.filter(travelDetail => travelDetail.distance !== null).length > 0) ? travelSummary.reduce((accumulator, current) => {
    // -----------------------------
    console.log(JSON.stringify(current));
    if (current.distance && (!accumulator || current.distance > accumulator)) {
      accumulator = current.distance;
    }
    return accumulator;
  }, null) / 1000 : null;


  // 5) Longevity (time difference between first and last events in days)
  let longevity = null;
  if (travelSummary.length > 1) {
    // -----------------------------------
    longevity = Moment(travelSummary[(travelSummary.length -1)].event_timestamp).diff(Moment(travelSummary[0].event_timestamp), 'days');
  }
  else {
    // -----------------------------------
    longevity = null;
  }



  // 6) Inferred Bird Status (DEAD if any dead or DEAD if last is bandonly PRESUMED DEAD and PRESUMED ALIVE in all other cases)
  let inferredBirdStatus = null;

  let statusSummary = await db.ro_bird_status_summary(birdId);

  if (statusSummary && statusSummary.filter(statusObj => statusObj.out_status_code.includes('DEAD')).length > 0) {
    inferredBirdStatus = 'DEAD';
  }
  else if (statusSummary && statusSummary[(statusSummary.length - 1)].out_status_code === 'UNKNOWN_BAND_ONLY') {
    inferredBirdStatus = 'UNKNOWN BAND RECOVERED';
  }
  else if (Moment(statusSummary[statusSummary.length - 1].event_timestamp).isAfter(Moment().subtract(1, 'year'))) {
    inferredBirdStatus = 'PRESUMED ALIVE';
  }
  else {
    inferredBirdStatus = 'UNKNOWN';
  }

  return {
    delta_first_and_last_sighting_km: deltaFirstAndLastSightingKm,
    delta_most_recent_sighting_km: deltaMostRecentSightingKm,
    cumulative_distance_km: cumulativeDistanceKm,
    dispersal_distance_km: dispersalDistanceKm,
    longevity: longevity,
    inferred_bird_status: inferredBirdStatus
  };
}

const validateBusinessRules = (birdObj) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".validateBusinessRules()");
  return new Promise((resolve, reject) => {
    console.log('[TODO] VALIDATE BIRD BUSINESS RULES');
    resolve([]);
  });
}

const addCalculatedFields = (dbResult) => {
  // ----------------------------------------------------------------------------   
  return new Promise((resolve, reject) => {

    console.info(RESOURCE_NAME + ".addCalculatedFields()");

    console.log(dbResult.length);
    console.log(dbResult);

    let res = dbResult.map(dbResultItem => {
      // ---------------------------------------
      console.log(dbResultItem.event);
      // Sort events by earliest to latest
      dbResultItem.event.sort((eventA, eventB) => {
        if (Moment(eventA.event_timestamp).isBefore(Moment(eventB.event_timestamp))) {
          return -1;
        }
        if (Moment(eventB.event_timestamp).isBefore(Moment(eventA.event_timestamp))) {
          return 1;
        }
        return 0;
      });

      // Construct the response which should only include the earliest/latest events
      let result = { ...dbResultItem }
      delete result.event;
      result.earliest_event = dbResultItem.event[0];
      result.latest_event = dbResultItem.event[dbResultItem.event.length - 1];

      return result;
    });

    resolve(res);
  });
}

const formatResponse = (method = 'search', res, supportingData=null) => {
  // ----------------------------------------------------------------------------    

  // Flatten subobjects (in-line with event-spreadsheet response for easy consumption)
  let response = res.map(bird => {
    return Object.keys(bird).reduce((objectBuilder, birdKey) => {
      // If key exists, it is not a timestamp and it of type 'object' - add this as a flattened subobject to the event
      if (bird[birdKey] && ! birdKey.includes('timestamp') && typeof bird[birdKey] === 'object') {
        objectBuilder[birdKey] = bird[birdKey];
      }
      else {
        objectBuilder.bird[birdKey] = bird[birdKey];
      }
      return objectBuilder;
    }, { bird: {} })
  });

  console.log(response);
  
  switch(method) {
    case 'get': {
      response = response.map(birdResponseObj => {
        // Splice in the supporting travel summary data into the bird subobject
        birdResponseObj.bird = { ...birdResponseObj.bird, ...supportingData}
        return birdResponseObj;
      });
      return (response.length > 0) ? response[0] : {};
    }
    case 'search':
    default: {
      return response;
    }
  }
}


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

  console.log(JSON.stringify(event));

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

  var customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
  }
  
  // JSON Schemas
  var paramSchema = {};

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  // Hoisting search result and travel summary
  let searchResult = null;
  let travelSummary = null;

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => { 
      // Store highest claimed group for reference further on in the function
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
    // Handle errors / Connect to DB
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      console.info('Querystring parameters OK. Connecting to DB...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Getting bird');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Validate bander is not suspended
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return searchDB(db, event.pathParameters, event.queryStringParameters); 
    })
    .then(res => {
      searchResult = res;
      return getBirdSupportingCalculations(db, event)
    })
    .then(res => {
      travelSummary = res;
      return addCalculatedFields(searchResult); 
    })
    .then(res => { 
      if (res.length === 0) throw new BoilerPlate.NotFoundError('BirdNotFound', { type: 'NOT_FOUND', message: `Bird cannot be found with this id`, data: { path: `pathParameters.birdId` }, value: event.pathParameters.birdId , schema: null });
      return formatResponse('get', res, travelSummary); 
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

  console.log('DEBUG: ', JSON.stringify(event));

  // Get the schema details for parameter validation
  var parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  var customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
  }

  // JSON Schemas
  var paramSchema = {};

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => { 
      // Store highest claimed group for reference further on in the function
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
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      console.info('Querystring parameters OK. Connecting to DB...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Searching birds');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Validate bander is not suspended
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return searchDB(db, null, event.queryStringParameters); 
    })
    .then(res => { return formatResponse('search', res); })
    .then(res => { 
      console.log(res);
      let params = {
        data: res, path: event.path,
        queryStringParameters: event.queryStringParameters,
        multiValueQueryStringParameters: event.multiValueQueryStringParameters,
        paginationPointerArray: ['bird', 'row_creation_idx'],
        maxLimit: 100, order: 'asc',
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

// POST
// POSTing a bird is equivalent to a first marking event
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

  // Response Object
  var response = {};  

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => { 
      // Store highest claimed group for reference further on in the function
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
      console.info('Path parameters OK. Validating payload...');
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      return BoilerPlate.validateJSON(payload, payloadSchema);
    })
    // Handle errors / Validate business rules
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body schema validation error(s)!`, errors);
      }
      console.info('Payload structure OK (schema). Validating business rules...');
      return validateBusinessRules(payload);
    })
    // Handle errors / marshall and Put
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      console.info('Payload structure OK (business). Connecting to DB...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Creating bird');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Validate bander is not suspended
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return putEventToDB(db, payload, event.pathParameters, null); 
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