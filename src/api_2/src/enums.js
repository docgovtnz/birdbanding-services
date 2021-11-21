'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

const USER_MARK_STATE_ACTIONS = require('bb-helpers').USER_MARK_STATE_ACTIONS;

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;

const RESOURCE_NAME = "enum";

const search = (db, pathParameters, queryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".search()");

  // Get the resource-name, remove the plurality and append 'Id'
  let resourceIdName = RESOURCE_NAME.slice(0, -1) + 'Id';
  let id = (pathParameters && pathParameters[resourceIdName]) ? pathParameters[resourceIdName] : null;
  let limit = (queryStringParameters && queryStringParameters.limit) ? parseInt(queryStringParameters.limit) : 20;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
    parseInt(queryStringParameters.paginationToken) : null;

  let count = null;

  // We've either got a definite ID to find, 
  // or we want a paginated subset (ordered from 
  // the most recent record to the oldest)
  // We base the pagination subset on a specific 
  // previous row_creation_idx which was a oldest record 
  // in the previous paginated set
  let idValue = id ? id : paginationToken;

  // Default pagination to the most recent records and submits empty criteria object
  let criteria = {};
  let idOperation;
  if (id) {
    idOperation = 'id =';
    criteria[idOperation] = idValue;
  } else if (paginationToken) {
    // Pagination on a join is slightly trickier (cannot use limit due to amalgamation of resultset)
    idOperation = 'row_creation_idx BETWEEN';
    criteria[idOperation] = [(idValue - limit), (idValue - 1)];
  } else {
    // Need the results ascending so want to know the total count prior to querying
    return db.mark.count({})
      .then(res => {
        console.log(res);
        count = res;

        idOperation = 'row_creation_idx BETWEEN';
        criteria[idOperation] = [(res - limit), res];
        console.log(criteria);
        return db.mark.find(criteria, {
          order: [{
            field: 'row_creation_idx',
            direction: 'desc'
          }]
        });
      })
      .then(res => {
        return {
          data: res,
          count: parseInt(count),
          countFromPageToEnd: true
        }
      })
  }

  // Build this up when we get significantly fancier with the 
  // breadth of querystring parameters we support. Right now,
  // we just return the whole lot.

  // THIS RETURNS A PROMISE
  return db.mark.count({})
    .then(res => {
      console.log(res);
      count = res;
      return db.mark.find(criteria, {
        order: [{
          field: 'row_creation_idx',
          direction: 'desc'
        }]
      });
    })
    .then(res => {
      return {
        data: res,
        count: parseInt(count),
        countFromPageToEnd: true
      }
    })
}

const get = async(db, pathParameters, queryStringParameters) => {
    let enumId = (pathParameters && pathParameters.enumId) ? pathParameters.enumId : null;
    let enumIdArray = [];
    let enums = {};
    console.log('enumId:' + enumId);

    let enum_event_code = db.enums.enum_event_code;
    console.log('enum_event_code:' + enum_event_code);

    switch (enumId) {

      case 'viewData':
        enumIdArray.push({
          enumId: 'enum_event_code',
          fieldId: 'EVENT_CODE'
        });
        enumIdArray.push({
          enumId: 'enum_event_code',
          fieldId: 'EVENT_CODE_BIRD'
        });
        enumIdArray.push({
          enumId: 'enum_event_banding_scheme',
          fieldId: 'EVENT_BANDING_SCHEME'
        });
        enumIdArray.push({
          enumId: 'enum_mark_type',
          fieldId: 'MARK_TYPE'
        });
        break;

      case 'managePeople':
        enumIdArray.push({
          enumId: 'enum_competency_level',
          fieldId: 'BANDER_COMPETENCY_LEVEL'
        });
        enumIdArray.push({
          enumId: 'enum_bander_state',
          fieldId: 'BANDER_STATE'
        });
        enumIdArray.push({
          enumId: 'enum_bander_state',
          fieldId: 'BANDER_STATE_FROM_ADMIN'
        });
        enumIdArray.push({
          enumId: 'enum_endorsement',
          fieldId: 'BANDER_ENDORSEMENT'
        });
        break;

      case 'viewProjects':
        enumIdArray.push({
          enumId: 'enum_project_state',
          fieldId: 'PROJECT_STATE'
        });
        break;

      case 'publicSighting':
        enumIdArray.push({
          enumId: 'enum_media_upload_status',
          fieldId: 'PUBLIC_SIGHTING_MEDIA_UPLOAD_STATUS'
        });
        enumIdArray.push({
          enumId: 'enum_public_event_status',
          fieldId: 'PUBLIC_SIGHTING_EVENT_STATUS'
        });
        enumIdArray.push({
          enumId: 'enum_condition_code',
          fieldId: 'PUBLIC_SIGHTING_CONDITION_CODE'
        });
        enumIdArray.push({
          enumId: 'enum_status_code',
          fieldId: 'PUBLIC_SIGHTING_STATUS_CODE'
        });
        enumIdArray.push({
          enumId: 'enum_region',
          fieldId: 'PUBLIC_SIGHTING_REGION'
        });
        break;

      case 'manageStock':
        enumIdArray.push({
          enumId: 'enum_supported_prefix_numbers',
          fieldId: 'PREFIX_NUMBERS'
        })
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_SEARCH'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_CAN_TRANSFER'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_NEW'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_ALLOCATED'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_RETURNED'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_LOST'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_ATTACHED'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_DETACHED'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_PRACTICE'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_RETURNED_USED'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_FROM_OTHER'
        });
        enumIdArray.push({
          enumId: 'enum_mark_state',
          fieldId: 'MARK_STATUS_CAN_DELETE'
        });
        break;

      case 'dataUpload':
        enumIdArray.push({
          enumId: 'enum_region',
          fieldId: 'REGION',
        });
        enumIdArray.push({
          enumId: 'enum_event_type',
          fieldId: 'EVENT_TYPE_BIRD_DISPLAY',
        });
        enumIdArray.push({
          enumId: 'enum_event_type',
          fieldId: 'EVENT_TYPE_BIRD',
        });
        enumIdArray.push({
          enumId: 'enum_event_type',
          fieldId: 'EVENT_TYPE_BIRD_FIRST_MARKING',
        });
        enumIdArray.push({
          enumId: 'enum_event_type',
          fieldId: 'EVENT_TYPE_BIRD_REMARKING',
        });
        enumIdArray.push({
          enumId: 'enum_event_type',
          fieldId: 'EVENT_TYPE_BIRD_RESIGHTING_RECOVERY',
        });
        enumIdArray.push({
          enumId: 'enum_event_type',
          fieldId: 'EVENT_TYPE',
        });
        enumIdArray.push({
          enumId: 'enum_event_bird_situation',
          fieldId: 'WILD_CAPTIVE'
        });
        enumIdArray.push({
          enumId: 'enum_event_capture_type',
          fieldId: 'CAPTURE_CODE'
        });
        enumIdArray.push({
          enumId: 'enum_status_code',
          fieldId: 'STATUS_CODE'
        });
        enumIdArray.push({
          enumId: 'enum_condition_value',
          fieldId: 'CONDITION_CODE'
        });
        enumIdArray.push({
          enumId: 'enum_event_user_coordinate_system',
          fieldId: 'COORDINATE_SYSTEM'
        });
        enumIdArray.push({
          enumId: 'enum_event_event_timestamp_accuracy',
          fieldId: 'EVENT_TIMESTAMP_ACCURACY'
        });
        enumIdArray.push({
          enumId: 'enum_mark_type',
          fieldId: 'MARK_CONFIGURATION_TYPE'
        });
        enumIdArray.push({
          enumId: 'enum_mark_material',
          fieldId: 'MARK_CONFIGURATION_MATERIAL'
        });
        enumIdArray.push({
          enumId: 'enum_mark_material',
          fieldId: 'MARK_CONFIGURATION_MATERIAL'
        });
        enumIdArray.push({
          enumId: 'enum_mark_colour',
          fieldId: 'MARK_CONFIGURATION_COLOUR'
        });
        enumIdArray.push({
          enumId: 'enum_mark_form',
          fieldId: 'MARK_CONFIGURATION_FORM'
        });
        enumIdArray.push({
          enumId: 'enum_bird_age',
          fieldId: 'BIRD_AGE',
        });
        enumIdArray.push({
          enumId: 'enum_bird_sex',
          fieldId: 'BIRD_SEX',
        });
        enumIdArray.push({
          enumId: 'enum_upload_status',
          fieldId: 'UPLOAD_STATUS',
        });
        enumIdArray.push({
          enumId: 'enum_status_detail',
          fieldId: 'STATUS_DETAILS',
        });
        break;

      default:
        break;
    }

    enumIdArray.map(enumObj => {
      let result = db.enums[enumObj.enumId];
      // Additional handling for filtration of enums
      switch (enumObj.fieldId) {
        case 'BANDER_STATE_FROM_ADMIN': {
          result = result.filter(value => ![
            "INACTIVE"
          ].includes(value));
          break;
        }
        case 'EVENT_CODE_BIRD': {
          result = result.filter(value => [
            "1 - First marking",
            "Z - Foreign Scheme band/mark",
            "2A - Resighted without being caught",
            "2B - Recaptured (but not re-marked)",
            "2C - Technology assisted retrap",
            "2D - Translocation release",
            "3 - Add/Change/Remove mark",
            "X - Dead: Recovery",
            "C - Captive/rehab history"
          ].includes(value))
          .map(value => {
            return {
              id: value.split(' ')[0],
              display: value
            }
          });
          break;
        }
        case 'EVENT_TYPE_BIRD_DISPLAY': {
          result = result.filter(value => [
            "FIRST_MARKING_IN_HAND",
            "SIGHTING_BY_PERSON",
            "IN_HAND",
            "RECORDED_BY_TECHNOLOGY"
          ].includes(value));
          break;
        }
        case 'EVENT_TYPE_BIRD':
          result = result.filter(value => [
            "FIRST_MARKING_IN_HAND",
            "SIGHTING_BY_PERSON",
            "IN_HAND",
            "IN_HAND_PRE_CHANGE",
            "IN_HAND_POST_CHANGE",
            "RECORDED_BY_TECHNOLOGY"
          ].includes(value));
        break;
        case 'EVENT_TYPE_BIRD_FIRST_MARKING':
          result = result.filter(value => [
            "FIRST_MARKING_IN_HAND"
          ].includes(value));
        break;
        case 'EVENT_TYPE_BIRD_REMARKING':
          result = result.filter(value => [
            "IN_HAND"
          ].includes(value));
        break;
        case 'EVENT_TYPE_BIRD_RESIGHTING_RECOVERY':
          result = result.filter(value => [
            "SIGHTING_BY_PERSON",
            "IN_HAND",
            "RECORDED_BY_TECHNOLOGY"
          ].includes(value));
        break;
        case 'PREFIX_NUMBERS':
          result = result.filter(prefixNumber => !['pit'].includes(prefixNumber)).sort();
          break;
        case 'MARK_STATUS_CAN_TRANSFER':
          result = result.filter(value => [
            "NEW",
            "ALLOCATED",
            "RETURNED"
          ].includes(value));
        break;
        case 'MARK_STATUS_FROM_NEW':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.NEW.includes(value));
        break;
        case 'MARK_STATUS_FROM_ALLOCATED':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.ALLOCATED.includes(value));
        break;
        case 'MARK_STATUS_FROM_RETURNED':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.RETURNED.includes(value));
        break;
        case 'MARK_STATUS_FROM_LOST':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.LOST.includes(value));
        break;
        case 'MARK_STATUS_FROM_ATTACHED':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.ATTACHED.includes(value));
        break;
        case 'MARK_STATUS_FROM_DETACHED':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.DETACHED.includes(value));
        break;
        case 'MARK_STATUS_FROM_PRACTICE':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.PRACTICE.includes(value));
        break;
        case 'MARK_STATUS_FROM_RETURNED_USED':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.RETURNED_USED.includes(value));
        break;
        case 'MARK_STATUS_FROM_OTHER':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.OTHER.includes(value));
        break;
        case 'MARK_STATUS_CAN_DELETE':
          result = result.filter(value => USER_MARK_STATE_ACTIONS.DELETE.includes(value));
        break;
        case 'MARK_CONFIGURATION_FORM':
          result = result
          .map(value => {
            return {
              id: value,
              display: (["WRAPAROUND_1_5", "WRAPAROUND_1_75"].includes(value)) ? `${value.split('_')[0]} ${value.split('_')[1]}.${value.split('_')[2]}` : value.split('_').join(' ')
            }
          });
          break
      }

      enums[enumObj.fieldId] = result;
    });


    return { data: enums}
}

const formatResponse = (method = 'get', res) => {
  // ----------------------------------------------------------------------------    
  return new Promise((resolve, reject)  => {

    console.log('res passed in from ' + method + '():' + JSON.stringify(res));
    // console.log('count:'+res.length);

    switch (method) {
      case 'get': {
        return (res) ? resolve(res) : resolve({});
      }
      case 'search':
      default: {
        return resolve(res);
      }
    }
  })
}

// ============================================================================
// API METHODS
// ============================================================================

// GET
module.exports.get = (event, context, cb) => {
  console.info(RESOURCE_NAME + ".get()");

  console.log('DEBUG: ', event);

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
      console.info('Querystring parameters OK. Connecting to DB...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.log('dbAccess' + dbAccess) // for future reference, you can't stringify the database or get circular reference issues
      console.info('Getting enums');

      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return get(db, event.pathParameters, event.queryStringParameters);
    })
    .then(res => {
      console.log('get() res:' + res);
      return formatResponse('get', res.data);
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

  let count = null;
  let countType = 'TOTAL';
  let countFromPageToEnd = true;

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
      console.info('Querystring parameters OK. Connecting to DB...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Searching marks');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return search(db, event.pathParameters, event.queryStringParameters);
    })
    .then(res => {
      count = res.count;
      countFromPageToEnd = res.countFromPageToEnd;
      console.log(res);
      return formatResponse('search', res.data);
    })
    .then(res => {
      let params = {
        data: res,
        path: event.path,
        queryStringParameters: event.queryStringParameters,
        multiValueQueryStringParameters: event.multiValueQueryStringParameters,
        paginationPointerArray: ['row_creation_idx'],
        maxLimit: 20,
        order: 'desc',
        count: count,
        countType: countType,
        countFromPageToEnd: countFromPageToEnd,
        isLastPage: false,
        prevPaginationToken: null
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