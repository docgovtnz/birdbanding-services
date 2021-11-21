'use strict';

// Modules
const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBSSHelpers = require('bb-spreadsheet-helpers');
const BBHelpers = require('bb-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
const Helpers = require('helpers.js');
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "event";

// This function is used to validate a user is allowed to access a particular event_transfer
const validateGetAccess = async(customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // ---------------------------------------------------------------------------- 
  console.info("validateGetAccess()");

  // The Authorisation logic is fairly complex, we call a number of functions to capture each component
  //   of the process
  let eventTransferId = event.pathParameters.eventTransferId;

  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
    return null;
  }

  let banderCanAccessEventTransferResultset = await db.ro_bander_can_access_event_transfer(eventTransferId, claims.sub);

  console.log(banderCanAccessEventTransferResultset);

  if (!banderCanAccessEventTransferResultset[0].ro_bander_can_access_event_transfer) {
    return customErrorFactory.getError('ForbiddenError', [`accessing transfer event: /view-data/event/${eventTransferId}`, claims.sub, 'claims.sub']);
  } 
  else {
    console.log('No errors');
    return null;
  }
};

const validateEventTransferExists = async (customErrorFactory, db, event, claims) => {
  // -----------------------------------------
  console.info(".validateEventTransferExists()");

  let eventTransferId = event.pathParameters.eventTransferId;

  let eventExistsResultset = await db.ro_is_event(eventTransferId);

  console.log(eventExistsResultset);
  if (!eventExistsResultset[0].ro_is_event) {
    return customErrorFactory.getError('NotFoundError', ['eventId', eventTransferId, 'pathParameters.eventId']);
  } 
  else {
    console.log('No errors');
    return null;
  }
}

// The getDB method has been extracted from the searchDB method because of complications with pagination
// Pagination will require the separation of paginated entities (conveniently all 1:1 relations)
const getDB = (db, event, claims, governingCognitoGroup) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getDB()");

  // Default pagination to the most recent records and submits empty criteria object
  let criteria = {};
  let idOperation = 'event.id =';
  criteria[idOperation] = event.pathParameters.eventTransferId;
  criteria['vw_transfer_prefix_number_counts.event_id ='] = event.pathParameters.eventTransferId;

  // User access control handled earlier in the promise chain

  // -----------------------------------
  console.info(criteria);

  // THIS RETURNS A PROMISE
  return db.event
          .join({
            pk: 'id',
            vw_transfer_prefix_number_counts: {
              type: 'INNER',
              pk: ['event_id', 'prefix_number'],
              on: { 'event_id': 'event.id' }
            }
          })
          .find(criteria, 
            { 
              order: [{ field: 'event.event_timestamp', direction: 'desc' }],
            })
            .then(res => {
              return {
                data: res
              }
            });
}

const generateUserAccessControlFilterCriteria = (claims) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateUserAccessControlFilterCriteria()");

  let criteria = { 'or':
    [
      { [`transfer_provider_id =`]: claims.sub },
      { [`transfer_recipient_id =`]: claims.sub }
    ]
  };

  return criteria;
}


const searchDB = async (db, event = null, claims, governingCognitoGroup) => {
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
  // QUERY STRING ORDER
  // ------------------------------------
  // Set standard sort order
  let sortOrder = 'desc';

  let lookAheadCount = null;

  // Check if a sort order has been requested
  if (queryStringParameters && 'order' in queryStringParameters && ['ASC', 'DESC'].includes(queryStringParameters.order)) {
    sortOrder = queryStringParameters.order.toLowerCase();
  }

  let criteria = { and: [{'event_type': 'TRANSFER'}] };

  // -----------------------
  // USER ACCESS CONTROL
  // -----------------------
  // If a user is anything besides admin we need to manage their access to the banding records
  let userAccessControlCriteria = null
  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    // ------------------------------------------------
    userAccessControlCriteria = generateUserAccessControlFilterCriteria(claims);
    criteria.and.push(userAccessControlCriteria);
  }

  // ---------------------------------------------
  // ADMIN ACCESS TO FILTER OTHER BANDER TRANFERS
  // ---------------------------------------------
  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME
      && queryStringParameters && 'banderId' in queryStringParameters) {
    // ------------------------------------------------
    userAccessControlCriteria = generateUserAccessControlFilterCriteria({ sub: queryStringParameters.banderId });
    criteria.and.push(userAccessControlCriteria);
  }
  console.info(JSON.stringify(criteria));

  // ---------------------------------------------------------
  // Complete initial search
  let finalResultSet = await db.vw_transfer_summary
    .find(criteria, 
      { 
        limit: (limit + 1),
        offset: (paginationToken) ? paginationToken : 0
      });

  console.log(finalResultSet);

  lookAheadCount = finalResultSet.length;
  // If there is a any events in the resultset we need to factor this into our query criteria
  if (finalResultSet.length > 0) {
    // ------------------------------------
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
    return {
      data: [],
      count: 0,
      sortOrder: sortOrder,
      isLastPage: true,
      current: null,
      prev: null
    };
  }
}


const checkMarkDuplication = async(customErrorFactory, marks) => {
  // --------------------------------------------
  let markDuplicates = marks.filter((mark, idx) => marks.indexOf(mark) != idx);

  console.log(markDuplicates);

  if (markDuplicates.length > 0) {
    return customErrorFactory.getError('DuplicateMarks', [markDuplicates.length, markDuplicates, 'marks']);
  }
  else {
    return null;
  }
}


const validateTransferProvider = async(customErrorFactory, db, event_provider_id, governingCognitoGroup, marks) => {
  // --------------------------------------------
  // Short circuit the validation if the admin group governs
  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
    return null;
  }

  let criteria = {
    and: [
      { or: marks.map(mark_id => { return { 'mark_id =': mark_id } }) },
      { 'bander_id !=': event_provider_id },
      { 'is_current =': true }
    ]
  };

  let options = {
    limit: 1
  };

  let invalidTransferProviderCheck = await db.mark_allocation.find(criteria, options);
  if (invalidTransferProviderCheck.length > 0) {
    return customErrorFactory.getError('UnauthorisedTransferProvider', [event_provider_id, 'event_provider_id']);
  }
  else {
    return null;
  }  
}


const validateTransferMarkState = async(customErrorFactory, db, marks) => {
  // --------------------------------------------

  let criteria = {
    and: [
      { or: marks.map(mark_id => { return { 'mark_id =': mark_id } }) },
      { and: [{ 'state !=': 'NEW' }, { 'state !=': 'ALLOCATED' }, { 'state !=': 'RETURNED' }] },
      { 'is_current =': true }
    ]
  };

  let options = {
    limit: 1
  };

  let invalidPreTransferStatusCheck = await db.mark_state.find(criteria, options);
  console.log(invalidPreTransferStatusCheck);
  if (invalidPreTransferStatusCheck.length > 0) {
    let states = invalidPreTransferStatusCheck.map(mark => mark.state);
    let uniqueStates = states.filter((state, idx) => states.indexOf(state) === idx);
    return customErrorFactory.getError('MarkStateLifecycleError', ['Some of the marks in this request are in an invalid state to be transferred', 'Must be NEW, ALLOCATED OR RETURNED prior to transfer', uniqueStates, 'mark_state', 'CRITICAL']);
  }
  else {
    return null;
  }
}


const validateBusinessRules = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------------------
  let eventTransferLimit = 500;
  let payload = JSON.parse(event.body);

  // ------------------------------------
  let promises = [];

  let event_provider_id = claims.sub;
  let marks = payload.marks;

  // -------------------------------------
  // Performance control, ensure no more than 500 marks being transferred
  // -------------------------------------
  if (marks.length > eventTransferLimit) {
    promises.push(Promise.resolve(customErrorFactory.getError('LimitExceedanceError', [`Number of marks exceeds event-transfer api limit of ${eventTransferLimit}`, '', marks.length, 'marks.length'])));
  }

  // -------------------------------------
  // Validate no duplicate marks have been submitted
  // -------------------------------------
  promises.push(checkMarkDuplication(customErrorFactory, marks));

  // -------------------------------------
  // Validate bander is admin or the current allocatee of the mark in question
  // -------------------------------------
  promises.push(validateTransferProvider(customErrorFactory, db, event_provider_id, governingCognitoGroup, marks));

  // -------------------------------------
  // Validate marks are in a mark state that can be transferred from
  // -------------------------------------
  promises.push(validateTransferMarkState(customErrorFactory, db, marks));

  // -------------------------------------
  // Validate transfer_recipient is qualified to receive the bands
  // -------------------------------------
  // TODO

  let errors = await Promise.all(promises);

  return errors.filter(error => error);
}


const generateMarkTransferEvent = (event, claims) => {
  // -----------------------------------------------------
  let payload = JSON.parse(event.body);

  let event_provider_id = claims.sub;
  let transfer_recipient_id = payload.transfer_recipient_id;
  let marks = payload.marks;

  let event_timestamp = null;

  if (payload.event_date) {
    console.log(`${payload.event_date}T00:00:00+1200`);
    event_timestamp = Moment(`${payload.event_date}T00:00:00+1200`).format();
  }

  // EVENT
  let event_body = {
    "event_banding_scheme": "NZ_NON_GAMEBIRD",
    "event_owner_id": process.env.BANDING_OFFICE_ID,
    "event_provider_id": event_provider_id,
    "event_reporter_id": event_provider_id,
    "event_state": "VALID",
    "event_timestamp": (event_timestamp) ? event_timestamp : Moment().format(),
    "event_timestamp_accuracy": "D",
    "event_type": "TRANSFER",
    "project_id": process.env.STOCK_PROJECT_ID,
    "transfer_recipient_id": transfer_recipient_id,
    "mark_count": marks.length
  }

  // MARK_ALLOCATION
  // note we will use a postgres function to ensure the allocation_idx and is_current are properly reconciled
  let mark_allocation = marks.map(mark_id => {
    return {
      "mark_id": mark_id,
      "bander_id": transfer_recipient_id,
      "allocation_idx": -1,
      "is_current": false
    }
  });

  // MARK_STATE 
  // note we will use a postgres function to ensure the state_idx and is_current are properly reconciled
  let mark_state = marks.map(mark_id => {
    return {
      "mark_id": mark_id,
      "state": "ALLOCATED",
      "state_idx": -1,
      "is_current": false
    }
  });
  
  return {
    event: event_body,
    mark_allocation: mark_allocation,
    mark_state: mark_state
  }
}


const getDistinctMarkPrefixesFromMarks = (marks) => {
  // --------------------------------------------------------
  console.info('Getting distinct mark prefixes from mark batch');

  let distinctPrefixes = new Set([]);

  marks.map(mark => {
        // ---------------------------
    distinctPrefixes.add(mark.prefix_number);
  });

  return Array.from(distinctPrefixes);
}


const postMarkTransferEventToDB = (db, payload) => {
  // -----------------------------------------------------
  console.log(JSON.stringify(payload));

  return db.withTransaction(async tx => {
    // -----------------------------------

    // ----------------------------------
    // 1) INSERT THE NEW DATA
    // ----------------------------------
    let event = await tx.event.insert(payload.event);

    let mark_allocation = await tx.mark_allocation.insert(payload.mark_allocation.map(mark_allocation => {
      // ----------------------------------------------------------
      console.log(mark_allocation);
      return {
        event_id: event.id,
        mark_id: mark_allocation.mark_id,
        bander_id: mark_allocation.bander_id,
        allocation_idx: mark_allocation.allocation_idx,
        is_current: mark_allocation.is_current
      }
    }));

    let mark_state = await tx.mark_state.insert(payload.mark_state.map(mark_state => {
      // ----------------------------------------------------------
      console.log(mark_state);
      return {
        event_id: event.id,
        mark_id: mark_state.mark_id,
        state: mark_state.state,
        state_idx: mark_state.state_idx,
        is_current: mark_state.is_current
      }
    }));

    // ----------------------------------
    // 2) RESET THE CURRENT MARK_ALLOCATION AND MARK_STATE
    // ----------------------------------
    let reset_current_mark_allocation = await tx.mark_allocation.update(
      { 
        'or': payload.mark_allocation.map(mark_allocation => { return { 'mark_id =': mark_allocation.mark_id } })
      },
      {
        'is_current': false
      }
    );

    let reset_current_mark_state = await tx.mark_state.update(
      { 
        'or': payload.mark_state.map(mark_state => { return { 'mark_id =': mark_state.mark_id } })
      },
      {
        'is_current': false
      }
    );

    // ----------------------------------
    // 3) UPDATE THE CURRENT MARK_ALLOCATION AND MARK_STATE
    // ----------------------------------
    let update_current_mark_allocation_and_state = await tx.rw_update_latest_mark_allocation_and_state([payload.mark_allocation.map(mark_allocation => mark_allocation.mark_id)]);
    
    // ----------------------------------
    // 4) REFRESH THE STOCK ROLLUP TABLE
    // ----------------------------------
    let marks = await tx.mark.find({ 
      'or': payload.mark_state.map(mark_state => { return { 'id =': mark_state.mark_id } })
    });
    
    console.log(marks);
    let prefixesToUpdate = getDistinctMarkPrefixesFromMarks(marks);
    console.log(`prefixes to update: ${prefixesToUpdate}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixesToUpdate.length > 0) {
      // -----------------------------------
      let transferRecipientId = payload.mark_allocation[0].bander_id;
      
      let prefixRowsExisting = await tx.mark_stock_aggregation_rollup.find({
        'bander_id =': transferRecipientId
      }, 
      {
        fields: ['prefix_number']
      });

      console.log(`Prefix rows existing: ${JSON.stringify(prefixRowsExisting)}`);

      let prefixesToInsert = prefixesToUpdate.filter(prefix_number => prefixRowsExisting.filter(record => record.prefix_number === prefix_number).length === 0);

      console.log(`Prefixes to insert: ${JSON.stringify(prefixesToInsert)}`);
      

      if (prefixesToInsert.length > 0) {
        let stockRowsAdded = await tx.mark_stock_aggregation_rollup.insert(prefixesToInsert.map(prefix_number => {
          return {
            bander_id: transferRecipientId,
            prefix_number: prefix_number,
            number_of_bands: 0,
            last_short_number: '-'
          }
        }));

        let banderPrefixUpdate = await tx.rw_update_bander_stock_by_prefix([ process.env.BANDING_OFFICE_ID, prefixesToUpdate ]);
        let bandingOfficePrefixUpdate = await tx.rw_update_banding_office_stock([ [process.env.BANDING_OFFICE_ID], [prefixesToUpdate] ]);
      }
      else {
        let banderPrefixUpdate = await tx.rw_update_bander_stock_by_prefix([ process.env.BANDING_OFFICE_ID, prefixesToUpdate ]);
        let bandingOfficePrefixUpdate = await tx.rw_update_banding_office_stock([ [process.env.BANDING_OFFICE_ID], [prefixesToUpdate] ]);
      }
    }

    return {
      event: event,
      mark_allocation: mark_allocation,
      mark_state: mark_state
    }
  });
}


const formatResponse = (method = 'search', res, pagination_idx) => {
  // ----------------------------------------------------------------------------    
  let response = null;

    // Map over the response to add mark_allocation count and transfer_recipient
    if (method === 'search') {

      response = res.data.map((event, idx) => {
        return {
          event: {
            id: event.id,
            event_type: event.event_type,
            event_timestamp: event.event_timestamp,
            pagination_idx: pagination_idx ? (pagination_idx + idx + 1) : (idx + 1)
          },
          event_provider: {
            id: event.transfer_provider_id,
            person_name: event.transfer_provider_person_name,
            maximum_certification_level: event.transfer_provider_max_cert_level
          },
          transfer_recipient: {
            id: event.transfer_recipient_id,
            person_name: event.transfer_recipient_person_name,
            maximum_certification_level: event.transfer_recipient_max_cert_level
          },
          mark_allocation_count: event.mark_allocation_count
        }
      });
    }
    else if (method === 'get') {
      // Flatten subobjects (in-line with event-spreadsheet response for easy consumption)
      response = res.data.map(event => {
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

      let mark_aggregation_sum = response[0].vw_transfer_prefix_number_counts.reduce((prev, current) => {
        return prev + current.count
      }, 0);

      response = {
          event: {
            id: response[0].event.id,
            event_timestamp: response[0].event.event_timestamp
          },
          mark_aggregation: response[0].vw_transfer_prefix_number_counts.map(mark => { return {
            prefix_number: mark.prefix_number,
            number_of_marks: mark.count,
            min_short_number: mark.min_short_number,
            max_short_number: mark.max_short_number
          }; }),
          mark_aggregation_sum: mark_aggregation_sum
        };
      }

    switch(method) {
      case 'post': {
        return {
          event: res.event
        };
      }
      case 'get': {
        return response;
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

  console.debug(JSON.stringify(event));

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

  var customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
  }

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
      console.info('Getting event');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return validateEventTransferExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Transfer event not found', error);
      return validateGetAccess(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      console.log(error);
      if (error) {
        throw new BoilerPlate.ForbiddenError(`Authorisation validation error(s)!!`, error);
      }
      return getDB(db, event, claims, governingCognitoGroup); 
    })
    .then(res => { 
      if (res.data.length === 0) throw new BoilerPlate.NotFoundError('TransferEventNotFound', { type: 'NOT_FOUND', message: `Transfer event cannot be found with this id`, data: { path: `pathParameters.transferEventId` }, value: event.pathParameters.transferEventId , schema: null });
      return formatResponse('get', res); 
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

  // Pagination parameters stored at function-wide-scope
  let count = 0;
  let isLastPage = false;
  let current = null;
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
      count = res.count;
      sortOrder = res.sortOrder;
      isLastPage = res.isLastPage;
      current = res.current;
      prev = res.prev;
      return formatResponse('search', res, current);
    })
    .then(res => { 
      // -------------------
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
      return BoilerPlate.generateIntegerPaginationFromArrayData(params);
    })
    .then(res => {
      // ------------------------
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
  console.info(JSON.stringify(event));

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
  let payload = JSON.parse(event.body);

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;
  
  // Do the actual work
  BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => {
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
      return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
    })
    // Validate Path parameters
    .then(schema => {
      paramSchema = schema;
      console.info('Schema retrieved. Validating path parameters...');
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
    // Handle errors / Validate request payload
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('QueryString parameters OK. Validating payload...');
      // Get Custom Errors schema from Dynamo
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      return BoilerPlate.validateJSON(payload, payloadSchema);
    })
    // Handle errors / Validate business rules
    .then(errors => {
      if (errors.length > 0) {
        // Format the errors and throw at this point to signify we aren't ready for business validation
        let responseErrors = formatSchemaErrors(customErrorFactory, errors, payload);
        throw new BoilerPlate.ParameterValidationError(`${responseErrors.length} Payload Body schema validation error(s)!`, responseErrors);
      }
      console.info('Payload parameters OK. Connecting to DB to validate business rules...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      // DB connection (container reuse of existing connection if available)      
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      console.info('Validating business rules...');
      return validateBusinessRules(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      return generateMarkTransferEvent(event, claims);
    })
    .then(res => {
      return postMarkTransferEventToDB(db, res);
    })
    .then(res => {
      return formatResponse('post', res);
    })
    .then(res => {
      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 201,
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