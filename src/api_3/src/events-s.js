'use strict';

// Modules
const Promise = require('bluebird');
const Moment = require('moment');
const MomentTimezone = require('moment-timezone');
const _ = require('lodash');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBSSHelpers = require('bb-spreadsheet-helpers');
const BBHelpers = require('bb-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
const Helpers = require('helpers.js')
var AWSXRay = require('aws-xray-sdk');
const { filter } = require('bluebird');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "event-simple";
const ALLOWABLE_SORT_ORDERS = BBHelpers.ALLOWABLE_EVENT_S_SORT_ORDERS;
const ALLOWABLE_EVENT_FILTERS = BBHelpers.ALLOWABLE_EVENT_S_FILTERS;
const ALLOWABLE_EVENT_SEARCHES = BBHelpers.ALLOWABLE_EVENT_S_SEARCHES;


const generateUserProjectCriteria = (projectList, prefix = '') => {
  // ----------------------------------------------------------------------------   
  return projectList.map(projectId => {
    return {
      [`${prefix}project_id =`]: projectId
    }
  });
}


const generateQueryStringFilterCriteria = (multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringFilterCriteria()");

  let criteria = {
    'and': []
  };

  let nonFilterQueryStringParameters = [ ...['limit', 'paginationToken'], ...Object.keys(ALLOWABLE_EVENT_SEARCHES)];
  
  // If there are any query string parameters, we need to process them, otherwise, continue
  if (multiValueQueryStringParameters && Object.keys(multiValueQueryStringParameters).length > 0) {
    // ----------------------------------------------------------------------------   
    let filterQueryStringParameterNames = Object.keys(multiValueQueryStringParameters).filter(queryStringParam => !nonFilterQueryStringParameters.includes(queryStringParam) );

    filterQueryStringParameterNames.map(filterQueryStringParameterName => {
      // --------------------------
      // If a recognised search parameter, add corresponding criteria
      if (filterQueryStringParameterName in ALLOWABLE_EVENT_FILTERS) {
        // ---------------------------------------
        let innerCriteria = {
          'or': []
        };

        multiValueQueryStringParameters[filterQueryStringParameterName].map(filterQueryStringParameterValue => {
          // ---------------------------
          if(filterQueryStringParameterName === 'nznbbsCode') {
            // NZNBBS Code is an amalgamation of schema fields (i.e. it is not stored at-rest) - special handling required
            let nznbbsCodeCriteria = BBHelpers.convertNznbbsCodeToFilterCriteria(filterQueryStringParameterValue);
            if (nznbbsCodeCriteria) innerCriteria.or.push(nznbbsCodeCriteria);
          }
          else if (filterQueryStringParameterName === 'eventDate') {
            // Event timestamp filters need the New Zealand timezone applied to them
            ALLOWABLE_EVENT_FILTERS[filterQueryStringParameterName].filter_name.map(filterName => {
              innerCriteria.or.push( { and: [
                { [`${filterName} >=`]: MomentTimezone.tz(`${filterQueryStringParameterValue} 00:00:00`, 'NZ').format() },
                { [`${filterName} <=`]: MomentTimezone.tz(`${filterQueryStringParameterValue} 23:59:59`, 'NZ').format() }
              ] });
              return;
            });
          }
          else if (filterQueryStringParameterName === 'eventDateGte') {
            // Event timestamp range filters need the New Zealand timezone applied to them as well as the beginning/end of the day
            ALLOWABLE_EVENT_FILTERS[filterQueryStringParameterName].filter_name.map(filterName => {
              innerCriteria.or.push({ [filterName]: MomentTimezone.tz(`${filterQueryStringParameterValue} 00:00:00`, 'NZ').format() });
              return;
            });
          }
          else if (filterQueryStringParameterName === 'eventDateLte') {
            // Event timestamp range filters need the New Zealand timezone applied to them as well as the beginning/end of the day
            ALLOWABLE_EVENT_FILTERS[filterQueryStringParameterName].filter_name.map(filterName => {
              innerCriteria.or.push({ [filterName]: MomentTimezone.tz(`${filterQueryStringParameterValue} 23:59:59`, 'NZ').format() });
              return;
            });
          }
          else {
            ALLOWABLE_EVENT_FILTERS[filterQueryStringParameterName].filter_name.map(filterName => {
              innerCriteria.or.push({ [filterName]: filterQueryStringParameterValue});
              return;
            });
          }
          return;
        });

        if (innerCriteria.or.length > 0) criteria.and.push(innerCriteria);
      }
    });
  }
  return (criteria.and.length > 0) ? criteria : null;
}


const processWildcardSearch = (paramName, paramValue, searchCase = 'retain') => {
  // ----------------------------------------------------------------------------
  if (searchCase === 'lower') {
    paramValue = paramValue.toLowerCase();
  }

  if ( paramValue.includes('*')) {
    // ------------------------------------------------
    paramValue = paramValue.split('*').join('%');
    return { [ALLOWABLE_EVENT_SEARCHES[paramName].wildcard_filter_name]: paramValue };
  }
  else {
    // ------------------------------------------------
    return { [ALLOWABLE_EVENT_SEARCHES[paramName].filter_name]: paramValue };
  }
}


const generateQueryStringSearchCriteria = (multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringSearchCriteria()");

  let criteria = {
    'and': []
  };

  let nonSearchQueryStringParameters = [ ...['limit', 'paginationToken'], ...Object.keys(ALLOWABLE_EVENT_FILTERS)];
  
  // If there are any query string parameters, we need to process them, otherwise, continue
  if (multiValueQueryStringParameters && Object.keys(multiValueQueryStringParameters).length > 0) {
    // -----------------------------------------------------------
    let searchQueryStringParameterNames = Object.keys(multiValueQueryStringParameters).filter(queryStringParam => !nonSearchQueryStringParameters.includes(queryStringParam) );

    searchQueryStringParameterNames.map(searchQueryStringParameterName => {
      // --------------------------
      // If a recognised search parameter, add corresponding criteria
      if (searchQueryStringParameterName in ALLOWABLE_EVENT_SEARCHES) {
        multiValueQueryStringParameters[searchQueryStringParameterName].map(searchQueryStringParameterValue => {
          // ---------------------------
          criteria.and.push(processWildcardSearch(searchQueryStringParameterName, searchQueryStringParameterValue, 'retain'));
        });
      }
    });
  }
  
  return (criteria.and.length > 0) ? criteria : null;
}


const generateUserAccessControlFilterCriteria = (userProjectList, claims, prefix = '') => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateUserAccessControlFilterCriteria()");

  let criteria = { 'or': [] };
  
  // 1) ANY PROJECT THEY BELONG TO
  let userProjectCrtieria = generateUserProjectCriteria(userProjectList, prefix);
  if (userProjectCrtieria.length > 0) {
    console.log('User project list criteria: ', { 'or': userProjectCrtieria });
    criteria.or.push({ 'or': userProjectCrtieria });
  }

  // 2) ANY DATA THEY HAVE SUBMITTED
  criteria.or.push({ 'or':
    [
      { [`${prefix}event_reporter_id =`]: claims.sub },
      { [`${prefix}event_provider_id =`]: claims.sub },
      { [`${prefix}event_owner_id =`]: claims.sub }
    ]
  });

  // 3) ANY PUBLIC/NON-MORATORIUM DATA
  criteria.or.push({
    'or': [
      { [`${prefix}default_moratorium_expiry <`]: Moment().format() },
      { [`${prefix}default_moratorium_expiry IS`]: null },
    ]
  });
  
  return criteria;
}


const searchDB = async (db, event = null, claims, governingCognitoGroup, userProjectList) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".searchDB()");

  let queryStringParameters = event.queryStringParameters;
  let multiValueQueryStringParameters = event.multiValueQueryStringParameters;

  let promises = [];

  // Initial pagination limit being set at 100
  let limit = (queryStringParameters && queryStringParameters.limit && parseInt(queryStringParameters.limit) <= 5000) ? parseInt(queryStringParameters.limit) : 5000;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
                             parseInt(queryStringParameters.paginationToken) : null;
  let criteria = {
    'id IS NOT': null
  }

  let options = {
    limit: limit + 1
  }

  if (paginationToken) {
    options.offset = paginationToken;
  }

  // ------------------------------------
  // QUERY STRING SORT COLUMN AND ORDER
  // ------------------------------------
  // Set standard materialized view name and sort order
  let viewName = 'vw_events_simple';
  let sortOrder = 'desc';

   options.order = [{
    field: 'event_timestamp',
    direction: 'desc'
  }];

  // SORTING
  // Check if a sort order has been requested
  if (queryStringParameters && 'sortBy' in queryStringParameters && Object.keys(ALLOWABLE_SORT_ORDERS).includes(queryStringParameters.sortBy)) {
    console.log(queryStringParameters.sortBy);
    options.order[0].field = ALLOWABLE_SORT_ORDERS[queryStringParameters.sortBy].sort_field;
  }

  // SORTING DIRECTION
  // Check if a sort order has been requested
  if (queryStringParameters && 'order' in queryStringParameters && ['ASC', 'DESC'].includes(queryStringParameters.order)) {
    options.order[0].direction = queryStringParameters.order.toLowerCase();
  }

  // -----------------------
  // USER ACCESS CONTROL
  // -----------------------
  // If a user is anything besides admin we need to manage their access to the banding records
  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    // ------------------------------------------------
    let userAccessControlCriteria = generateUserAccessControlFilterCriteria(userProjectList, claims);
    criteria.or = userAccessControlCriteria.or;
  }

  console.log(JSON.stringify(criteria));

  // If a bird record is requested, add additional filter criteria
  // if (typeof queryStringParameters !== 'undefined' && queryStringParameters && 'birdRecord' in queryStringParameters && queryStringParameters.birdRecord === 'true') {
  //   lookAheadSearchQueryCriteria.and.push({ 'bird_id IS NOT': null });
  //   if (lookBehindSearchQueryCriteria) lookBehindSearchQueryCriteria.and.push({ 'bird_id IS NOT': null });
  // }

  // -----------------------
  // QUERY STRING SEARCHING TODO
  // -----------------------
  let searchCriteria = generateQueryStringSearchCriteria(multiValueQueryStringParameters);
  console.log(searchCriteria)
  if (searchCriteria) {
    // Add to existing or criteria
    criteria.and = searchCriteria.and;
  }

  console.log(JSON.stringify(criteria));

  // -----------------------
  // QUERY STRING FILTERING TODO
  // -----------------------
  let filterCriteria = generateQueryStringFilterCriteria(multiValueQueryStringParameters);
  console.log(filterCriteria);
  if ('and' in criteria && filterCriteria) {
    // Add to existing or criteria
    criteria.and = [...criteria.and, ...filterCriteria.and];
  }
  else if (filterCriteria) {
    criteria.and = filterCriteria.and;
  }

  console.log(JSON.stringify(criteria));
  console.log(JSON.stringify(options));

  console.log(viewName);

  let initialResultSet = (criteria) ? await db[viewName].find(criteria, options) : await db[viewName].find(options);

  console.log(initialResultSet.length);

  let joinCriteria = null;

  if (initialResultSet.length > 0) {
    joinCriteria = {
        or: initialResultSet.map(eventHeader => {
        return { 'event_id =': eventHeader.id };
      })
    };
  }

  console.log('Join criteria length: ', (joinCriteria ? joinCriteria.or.length : 0));

  let markConfigurationResultSet = (joinCriteria && joinCriteria.or.length > 0) ? 
        await db.mark_configuration.find({ and: [ joinCriteria, { 'alphanumeric_text IS NOT': null } ]}, { order: [{field: 'event_id'}] }) 
        : [];

  console.log('Mark configuration length: ', markConfigurationResultSet.length);

  return {
    event_metadata: (initialResultSet.length > limit) ? initialResultSet.slice(0, initialResultSet.length - 1) : initialResultSet,
    mark_configuration: markConfigurationResultSet,
    count: (initialResultSet.length > limit) ? (initialResultSet.length - 1) : initialResultSet.length,
    sortOrder: sortOrder,
    isLastPage: initialResultSet.length <= limit,
    prev: (paginationToken && (paginationToken - limit) >= 0) ? (paginationToken - limit) : null
  }
}


const addCalculatedFields = (event, method = 'search', res) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".addCalculatedFields()");

  let queryStringParameters = event.queryStringParameters;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
  parseInt(queryStringParameters.paginationToken) : null;

  // Best guess of the primary mark + calculation of the event_code for each event
  console.log('Starting event calculated fields');
  let events = res.event_metadata.map((event, idx) => {
    // --------------------------------------
    let result = { ...event };
    // 1) Add pagination_idx:
    result.pagination_idx = paginationToken ? (paginationToken + idx + 1) : (idx + 1);

    // 2) Attempt to add a primary mark
    let primaryMarkConfig = res.mark_configuration.find(markConfig => {
      return (event.id === markConfig.event_id && markConfig.mark_id);
    })
    result.prefix_number = (typeof primaryMarkConfig !== 'undefined') ? primaryMarkConfig.alphanumeric_text.split('-')[0].toUpperCase() : null;
    result.short_number =  (typeof primaryMarkConfig !== 'undefined') ? primaryMarkConfig.alphanumeric_text.split('-')[1].toUpperCase() : null;

    // 3) Attempt to add nznbbs_code
    result.historic_nznbbs_code = BBHelpers.deriveNznbbsCode({
      ...event,
      characteristic_measurement: [{ characteristic_id: event.characteristic_id, value: event.characteristic_measurement_value}]
    });

    // 4) Attempt to add colour_bands field
    result.other_bands = (res.mark_configuration.filter(markConfig => { return event.id === markConfig.event_id && !markConfig.mark_id; }).length > 0) ? true : false;

    // Remove temporary properties
    delete result.event_reporter_id;
    delete result.event_provider_id;
    delete result.event_owner_id;
    delete result.characteristic_id;
    delete result.characteristic_measurement_value;
    delete result.characteristic_measurement_id;
    delete result.species_group_id;
    delete result.species_id;

    return result;
  })
  console.log('Ending event calculated fields');

  return events;
}


// ============================================================================
// API METHODS
// ============================================================================

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

  // Pagination parameters stored at function-wide-scope
  let count = 0;
  let countType = 'PAGE';
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
      console.info('Searching events');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Migrate to bb helpers
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return BBHelpers.getUserProjects(db, claims);
    })
    .then(userProjectList=> {
      return searchDB(db, event, claims, governingCognitoGroup, userProjectList); 
    })
    .then(res => { 
      count = res.count;
      sortOrder = res.sortOrder;
      isLastPage = res.isLastPage;
      prev = res.prev;
      return addCalculatedFields(event, 'search', res); 
    })
    .then(res => { 
      // -------------------
      let params = {
        data: res, path: event.path,
        queryStringParameters: event.queryStringParameters,
        multiValueQueryStringParameters: event.multiValueQueryStringParameters,
        paginationPointerArray: ['pagination_idx'],
        maxLimit: 100, order: sortOrder,
        count: count, countType: countType,
        countFromPageToEnd: countFromPageToEnd,
        isLastPage: isLastPage, prevPaginationToken: prev
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
