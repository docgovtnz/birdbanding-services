'use strict';

// Modules
const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const _ = require('lodash');
const uuidv4 = require('uuid/v4');
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

const RESOURCE_NAME = "bird-events";

const searchDB = async(db, event = null, claims, governingCognitoGroup) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".searchDB()");

  let pathParameters = event.pathParameters;
  let queryStringParameters = event.queryStringParameters;
  let multiValueQueryStringParameters = event.multiValueQueryStringParameters;

  // Initial pagination limit being set at 100
  let limit = (queryStringParameters && queryStringParameters.limit && parseInt(queryStringParameters.limit) <= 100) ? parseInt(queryStringParameters.limit) : 100;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
                             parseInt(queryStringParameters.paginationToken) : null;

  // ------------------------------------
  // QUERY STRING SORT COLUMN AND ORDER
  // ------------------------------------
  // Set sort order
  let sortOrder = 'desc';

  let count = null;
  let maximumPaginationIdxCount = 0;
  let prev = null;

  // Check if a sort order has been requested
  if (queryStringParameters && 'order' in queryStringParameters && ['ASC', 'DESC'].includes(queryStringParameters.order)) {
    sortOrder = queryStringParameters.order.toLowerCase();
  }

  let criteria = {
    'bird_id =': pathParameters.birdId
  }

  let lookAheadCount = null;

  // ---------------------------------------------------------
  // Complete standard search
  let initialSearchResultset = await db.event.find(criteria, 
    { 
      fields: ['id', 'event_timestamp'],
      order: [{ field: 'event_timestamp', direction: sortOrder }],
      limit: limit + 1,
      offset: (paginationToken) ? paginationToken : 0
    });

  // If there is a preliminary look ahead query result, we need to factor this into our query criteria
  console.log(JSON.stringify(initialSearchResultset));
  console.log(initialSearchResultset.length);

  lookAheadCount = initialSearchResultset.length;

  if (initialSearchResultset.length > 0) {
    // ------------------------------------
    let criteria = {
      or: []
    };
    let idOperation = `event.id =`;

    initialSearchResultset.map((event, idx) => {
      criteria.or.push({ [idOperation]: event.id });
    });

    console.log(JSON.stringify(criteria));

    let joinDefinition = {
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
    };

    let finalResultSet = await db.event.join(joinDefinition)
      .find(criteria, 
        { 
          order: [{ field: 'event.event_timestamp', direction: sortOrder }],
        });

    console.log(JSON.stringify(finalResultSet));

    finalResultSet = finalResultSet.map((record, idx) => {
      record.pagination_idx = paginationToken ? (paginationToken + idx + 1) : (idx + 1)
      return record;
    });

    return {
      data: (lookAheadCount > limit) ? finalResultSet.slice(0, finalResultSet.length - 1) : finalResultSet,
      count: (lookAheadCount > limit) ? (lookAheadCount - 1) : lookAheadCount, // Lookahead to check if last page or not
      sortOrder: sortOrder,
      isLastPage: lookAheadCount <= limit,
      current: paginationToken,
      prev: (paginationToken && (paginationToken - limit) >= 0) ? paginationToken - limit : 0
    }
  }
  else {
    console.info('Returning from 1:N joins. No results from combination of search and filter criteria!')
    return {
      data: [],
      count: 0,
      sortOrder: sortOrder,
      isLastPage: true,
      current: paginationToken,
      prev: null
    };
  }
}


const addCalculatedFields = (method = 'search', dbResult) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".addCalculatedFields()");

  console.log(dbResult);

  let res = dbResult.map(dbResultItem => {
    let event = (method === 'search') ? dbResultItem.event : dbResultItem;
    event = { ...event, pagination_idx: (method === 'search-bird') ? dbResultItem.pagination_idx : dbResultItem.row_creation_idx };

    switch(event.event_type) {
      case 'FIRST_MARKING_IN_HAND':
      case 'SIGHTING_BY_PERSON':
      case 'IN_HAND':
      case 'RECORDED_BY_TECHNOLOGY':
      case 'IN_HAND_PRE_CHANGE':
      case 'IN_HAND_POST_CHANGE':
        // ----------------------------
        // If this is a bird event, add some calculated fields including: colour_bands (bool, true if colour bands present) and nznbbs_code
        let mark_config_colour_search = event.mark_configuration.filter(markConfig => typeof markConfig.colour !== 'undefined' && markConfig.colour);
        return {
          ...event,
          calculated_fields: {
            colour_bands: (mark_config_colour_search.length > 0),
            historic_nznbbs_code: BBHelpers.deriveNznbbsCode(event)
          }
        }
      default:
        return {
          ...event,
          calculated_fields: {
            colour_bands: null,
            historic_nznbbs_code: BBHelpers.deriveNznbbsCode(event)
          }
        }
    }
  });

  return res;
}


const formatResponse = (method = 'search', res) => {
  // ----------------------------------------------------------------------------    

    // Flatten subobjects (in-line with event-spreadsheet response for easy consumption)
    let response = res.map(event => {
      event.bird = ('bird' in event) ? event.bird : {};
      return Object.keys(event).reduce((objectBuilder, eventKey) => {
        // If key exists, it is not a timestamp and it of type 'object' - add this as a flattened subobject to the event
        if (event[eventKey] && ! eventKey.includes('timestamp') && typeof event[eventKey] === 'object') {
          objectBuilder[eventKey] = event[eventKey];
        }
        else {
          objectBuilder.event[eventKey] = event[eventKey];
        }
        return objectBuilder;
      }, { event: {} })
    });

    switch(method) {
      case 'get': {
        return (response.length > 0) ? response[0] : {};
      }
      case 'search':
      case 'post':
      case 'put':
      default: {
        return response;
      }
    }
}

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

  // Pagination parameters stored at function-wide-scope
  let count = 0;
  let countType = 'LOOKAHEAD';
  let countFromPageToEnd = false;
  let isLastPage = false;
  let prev = null;

  // Sort ordering parameter also at function-wide-scope
  let sortOrder = 'desc';

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
      return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version); })
    .then(schema => {
      paramSchema = schema;
      console.info('Group membership authorisation OK. Validating path parameters...');
      // Validate Path parameters
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
      console.info('Querystring parameters OK. Connecting to DB...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Searching events');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return searchDB(db, event, claims, governingCognitoGroup); 
    })
    .then(res => { 
      console.log('1, ', JSON.stringify(res));
      count = res.count;
      countFromPageToEnd = res.countFromPageToEnd;
      sortOrder = res.sortOrder;
      isLastPage = res.isLastPage;
      prev = res.prev;
      return addCalculatedFields('search-bird', res.data); 
    })
    .then(res => { console.log('2, ', JSON.stringify(res)); return formatResponse('search', res); })
    .then(res => { 
      // -------------------
      console.log('3, ', JSON.stringify(res));
      let params = {
        data: res, path: event.path,
        queryStringParameters: event.queryStringParameters,
        multiValueQueryStringParameters: event.multiValueQueryStringParameters,
        paginationPointerArray: ['event', 'pagination_idx'],
        maxLimit: 100, order: sortOrder,
        count: null,
        countFromPageToEnd: isLastPage,
        isLastPage: isLastPage, prevPaginationToken: prev
      }
      console.log(params);
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
