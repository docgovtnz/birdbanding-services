'use strict';

// Modules
const Promise = require('bluebird');
const Moment = require('moment');
const MomentTimezone = require('moment-timezone');
const _ = require('lodash');
const sqlString = require('sqlstring');
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

const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "event-advanced";
const ALLOWABLE_SORT_ORDERS = BBHelpers.ALLOWABLE_EVENT_S_SORT_ORDERS;
const ALLOWABLE_EVENT_FILTERS = BBHelpers.ALLOWABLE_EVENT_S_FILTERS;
const ALLOWABLE_EVENT_SEARCHES = BBHelpers.ALLOWABLE_EVENT_S_SEARCHES;

const SIMPLE_FILTERS = {
  projectId: {
    paramName: 'projectId',
    columnName: 'project_id'
  },
  speciesId: {
    paramName: 'speciesId',
    columnName: 'species_id'
  },
  speciesGroup: {
    paramName: 'searchSpeciesGroupId',
    columnName: 'species_group_id'
  },
  bandingScheme: {
    paramName: 'bandingScheme',
    columnName: 'event_banding_scheme'
  },
  reporterId: {
    paramName: 'reporterId',
    columnName: 'event_reporter_id'
  },
  providerId: {
    paramName: 'providerId',
    columnName: 'event_provider_id'
  },
  ownerId: {
    paramName: 'ownerId',
    columnName: 'event_owner_id'
  }
};
const SPATIAL_FILTERS = {
  latitudeLte: {
    paramName: 'latitudeLte',
    columnName: 'latitude'
  },
  latitudeGte: {
    paramName: 'latitudeGte',
    columnName: 'latitude'
  },
  longitudeLte: {
    paramName: 'longitudeLte',
    columnName: 'longitude'
  },
  longitudeGte: {
    paramName: 'longitudeGte',
    columnName: 'longitude'
  },
}


const deriveWhereNznbbsCodeCriteria = (nznbbsCode) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".deriveWhereNznbbsCodeCriteria()");

  switch (nznbbsCode) {
    case '1':
      return `( event_type = 'FIRST_MARKING_IN_HAND' AND event_banding_scheme != 'FOREIGN' `
        + `AND characteristic_measurement_value != 'DEAD_UNSPECIFIED' AND characteristic_measurement_value != 'DEAD_RECENT' `
        + `AND characteristic_measurement_value != 'DEAD_NOT_RECENT' )`;
    case 'Z':
      return `( ( event_type = 'FIRST_MARKING_IN_HAND' OR event_type = 'IN_HAND' ) AND event_banding_scheme = 'FOREIGN' `
        + `AND characteristic_measurement_value != 'DEAD_UNSPECIFIED' AND characteristic_measurement_value != 'DEAD_RECENT' `
        + `AND characteristic_measurement_value != 'DEAD_NOT_RECENT' )`;
    case 'C':
      return `( ( event_type != 'FIRST_MARKING_IN_HAND' ) AND event_banding_scheme != 'FOREIGN' AND event_bird_situation = 'CAPTIVE' `
        + `AND characteristic_measurement_value != 'DEAD_UNSPECIFIED' AND characteristic_measurement_value != 'DEAD_RECENT' `
        + `AND characteristic_measurement_value != 'DEAD_NOT_RECENT' )`;
    case '2A':
      return `( event_type = 'SIGHTING_BY_PERSON' `
        + `AND characteristic_measurement_value != 'DEAD_UNSPECIFIED' AND characteristic_measurement_value != 'DEAD_RECENT' `
        + `AND characteristic_measurement_value != 'DEAD_NOT_RECENT' )`;
    case '2B':
      return `( event_type = 'IN_HAND' AND event_bird_situation != 'RELEASE_SITE' AND event_banding_scheme != 'FOREIGN'`
        + `AND characteristic_measurement_value != 'DEAD_UNSPECIFIED' AND characteristic_measurement_value != 'DEAD_RECENT' `
        + `AND characteristic_measurement_value != 'DEAD_NOT_RECENT' )`;
    case '3':
      return `( ( event_type = 'IN_HAND_PRE_CHANGE' OR event_type = 'IN_HAND_POST_CHANGE' ) AND event_banding_scheme != 'FOREIGN' AND event_bird_situation != 'RELEASE_SITE'`
        + `AND characteristic_measurement_value != 'DEAD_UNSPECIFIED' AND characteristic_measurement_value != 'DEAD_RECENT' `
        + `AND characteristic_measurement_value != 'DEAD_NOT_RECENT' )`;
    case '2C':
      return `( event_type = 'RECORDED_BY_TECHNOLOGY' `
        + `AND characteristic_measurement_value != 'DEAD_UNSPECIFIED' AND characteristic_measurement_value != 'DEAD_RECENT' `
        + `AND characteristic_measurement_value != 'DEAD_NOT_RECENT' )`;
    case '2D':
      return `( ( event_type = 'IN_HAND_PRE_CHANGE' OR event_type = 'IN_HAND_POST_CHANGE' OR event_type = 'IN_HAND') AND event_banding_scheme != 'FOREIGN' AND event_bird_situation = 'RELEASE_SITE' `
        + `AND characteristic_measurement_value != 'DEAD_UNSPECIFIED' AND characteristic_measurement_value != 'DEAD_RECENT' `
        + `AND characteristic_measurement_value != 'DEAD_NOT_RECENT' )`;
    case 'X':
      return `( characteristic_measurement_value = 'DEAD_UNSPECIFIED' `
        + `OR characteristic_measurement_value = 'DEAD_RECENT' `
        + `OR characteristic_measurement_value = 'DEAD_NOT_RECENT' )`
    default:
      return null;
  }
}


const genereateSearchQuery = (event) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".genereateSearchQuery()");

  let criteria = [];
  let queryParameters = {};

  let searchPayload = JSON.parse(event.body);

  let simpleFilters = Object.keys(searchPayload).filter(key => Object.keys(SIMPLE_FILTERS).includes(key));
  let spatialFilters = Object.keys(searchPayload).filter(key => Object.keys(SPATIAL_FILTERS).includes(key));

  // --------------------
  // Simple filter pattern
  // --------------------
  simpleFilters.map(filter => {
    // -------------------------------
    if (searchPayload[filter].length > 0) {
      // --------------------------------------------------------------------
      criteria.push(`( ${searchPayload[filter].map((value, idx) => {
        // -------------------------------------------------------------
        // Add the prepared statement parameter for each simple filter
        queryParameters[`${SIMPLE_FILTERS[filter].paramName}${idx}`] = value;
        // Return the prepared statement clause for joining up with other components
        return `${SIMPLE_FILTERS[filter].columnName} = \${${SIMPLE_FILTERS[filter].paramName}${idx}}`
      }).join(' OR ')} )`);
    }
  });

  // --------------------
  // UserId
  // --------------------
  if ('userId' in searchPayload && searchPayload.userId.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.userId.map((user, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each user ID
      queryParameters[`userId${idx}`] = user;
      // Return the prepared statement clause for joining up with other components
      return `event_reporter_id = \${userId${idx}} OR event_provider_id = \${userId${idx}} OR event_owner_id = \${userId${idx}}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // EventDate
  // --------------------
  if ('eventDate' in searchPayload && searchPayload.eventDate.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.eventDate.map((eventDate, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each user ID
      // Tested with timezone - MomentTimezone.tz(eventDate, 'NZ').utc().format('YYYY-MM-DD');
      queryParameters[`eventDateLower${idx}`] = MomentTimezone.tz(`${eventDate} 00:00:00`, 'NZ').format();
      queryParameters[`eventDateUpper${idx}`] = MomentTimezone.tz(`${eventDate} 23:59:59`, 'NZ').format();
      // Return the prepared statement clause for joining up with other components
      return `( event_timestamp >= \${eventDateLower${idx}} AND event_timestamp <= \${eventDateUpper${idx}} )`
    }).join(' OR ')} )`);
  }

  // --------------------
  // EventDateLte
  // --------------------
  if ('eventDateLte' in searchPayload && searchPayload.eventDateLte.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.eventDateLte.map((eventDateLte, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each user ID
      // Tested with timezone - MomentTimezone.tz(eventDate, 'NZ').utc().format('YYYY-MM-DD');
      queryParameters[`eventDateLte${idx}`] = MomentTimezone.tz(`${eventDateLte} 23:59:59`, 'NZ').format();
      // Return the prepared statement clause for joining up with other components
      return `event_timestamp <= \${eventDateLte${idx}}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // EventDateGte
  // --------------------
  if ('eventDateGte' in searchPayload && searchPayload.eventDateGte.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.eventDateGte.map((eventDateGte, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each user ID
      // Tested with timezone - MomentTimezone.tz(eventDate, 'NZ').utc().format('YYYY-MM-DD');
      queryParameters[`eventDateGte${idx}`] = MomentTimezone.tz(`${eventDateGte} 00:00:00`, 'NZ').format();
      // Return the prepared statement clause for joining up with other components
      return `event_timestamp >= \${eventDateGte${idx}}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // Latitude + Longitude LTE
  // --------------------
  spatialFilters.map(filter => {
    // -------------------------------
    if (searchPayload[filter].length > 0) {
      // --------------------------------------------------------------------
      let sign = (filter.includes('Gte')) ? '>=' : '<=';
      criteria.push(`( ${searchPayload[filter].map((value, idx) => {
        // -------------------------------------------------------------
        // Add the prepared statement parameter for each simple filter
        queryParameters[`${SPATIAL_FILTERS[filter].paramName}${idx}`] = value;
        // Return the prepared statement clause for joining up with other components
        return `${SPATIAL_FILTERS[filter].columnName} ${sign} \${${SPATIAL_FILTERS[filter].paramName}${idx}}`
      }).join(' OR ')} )`);
    }
  });

  // --------------------
  // Location Description
  // --------------------
  if ('locationDescription' in searchPayload && searchPayload.locationDescription.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.locationDescription.map((locationDesc, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each project
      queryParameters[`locationDescription${idx}`] = locationDesc.toLowerCase().split('*').join('%');
      // Return the prepared statement clause for joining up with other components
      return `location_description ILIKE \${locationDescription${idx}}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // Comments
  // --------------------
  if ('comments' in searchPayload && searchPayload.comments.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.comments.map((comment, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each project
      queryParameters[`comment${idx}`] = comment.toLowerCase().split('*').join('%');
      // Return the prepared statement clause for joining up with other components
      return `comments ILIKE \${comment${idx}}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // FRIENDLY NAME
  // --------------------
  if ('friendlyName' in searchPayload && searchPayload.friendlyName.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.friendlyName.map((friendlyName, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each project
      queryParameters[`friendlyName${idx}`] = friendlyName.toLowerCase().split('*').join('%');
      // Return the prepared statement clause for joining up with other components
      return `friendly_name ILIKE \${friendlyName${idx}}`
    }).join(' OR ')} )`);
  }
  // --------------------
  // NZNBBS Code
  // --------------------
  if ('nznbbsCode' in searchPayload && searchPayload.nznbbsCode.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.nznbbsCode.map((nznbbsCode, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each project
      let nznbbsCodeCriteria = deriveWhereNznbbsCodeCriteria(nznbbsCode);
      console.log(JSON.stringify(nznbbsCodeCriteria));
      return nznbbsCodeCriteria;
    }).join(' OR ')} )`);
  }
  // --------------------
  // Band Number and Other alphanumeric
  // --------------------
  // If there are already bandNumbers searched we want to OR this with that so we don't exclude results)
  if ('bandNumber' in searchPayload && 'otherAlphanumeric' in searchPayload
    && searchPayload.bandNumber.length > 0 && searchPayload.otherAlphanumeric.length > 0) {
    // ----------------------------------------------------------
    criteria.push(`( ${[...searchPayload.bandNumber.map((bandNumber, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each project
      queryParameters[`bandNumber${idx}`] = bandNumber.toLowerCase().split('*').join('%');
      // Return the prepared statement clause for joining up with other components
      // Note - mark_id not null required to capture tracked stock only
      return `value->>'alphanumeric_text' LIKE \${bandNumber${idx}} AND value->>'mark_id' IS NOT NULL`
    }), ...searchPayload.otherAlphanumeric.map((otherAlpha, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each project
      queryParameters[`otherAlpha${idx}`] = otherAlpha.toLowerCase().split('*').join('%');
      // Return the prepared statement clause for joining up with other components
      // Note - mark_id not null required to capture tracked stock only
      return `value->>'alphanumeric_text' LIKE \${otherAlpha${idx}}`
    })].join(' OR ')} )`);

  }
  else {
    // ----------------------------------------------------------
    // --------------------
    // Band Number
    // --------------------
    if ('bandNumber' in searchPayload && searchPayload.bandNumber.length > 0) {
      // --------------------------------------------------------------------
      criteria.push(`( ${searchPayload.bandNumber.map((bandNumber, idx) => {
        // -------------------------------------------------------------
        // Add the prepared statement parameter for each project
        queryParameters[`bandNumber${idx}`] = bandNumber.toLowerCase().split('*').join('%');
        // Return the prepared statement clause for joining up with other components
        // Note - mark_id not null required to capture tracked stock only
        return `value->>'alphanumeric_text' LIKE \${bandNumber${idx}} AND value->>'mark_id' IS NOT NULL`
      }).join(' OR ')} )`);
    }

    // --------------------
    // Other alphanumeric
    // --------------------
    if ('otherAlphanumeric' in searchPayload && searchPayload.otherAlphanumeric.length > 0) {
      // --------------------------------------------------------------------
      criteria.push(`( ${searchPayload.otherAlphanumeric.map((otherAlpha, idx) => {
        // -------------------------------------------------------------
        // Add the prepared statement parameter for each project
        queryParameters[`otherAlpha${idx}`] = otherAlpha.toLowerCase().split('*').join('%');
        // Return the prepared statement clause for joining up with other components
        // Note - mark_id not null required to capture tracked stock only
        return `value->>'alphanumeric_text' LIKE \${otherAlpha${idx}}`
      }).join(' OR ')} )`);
    }
  }

  // --------------------
  // Mark Configuration
  // --------------------
  if ('markConfiguration' in searchPayload && searchPayload.markConfiguration.length > 0) {
    // --------------------------------------------------------------------
    // -- Remove properties with null values
    let filteredMarkConfiguration = searchPayload.markConfiguration.map((markConfig, idx) => {
      // Add the prepared statement parameter for each project
      Object.keys(markConfig).forEach((key) => (markConfig[key] === null) && delete markConfig[key]);
      if ('alphanumeric_text' in markConfig) {
        markConfig.alphanumeric_text = markConfig.alphanumeric_text.toLowerCase();
      }

      return markConfig;
    });


    console.log(JSON.stringify(filteredMarkConfiguration));

    criteria.push(`( agg_mc @> \${markConfiguration} )`);
    queryParameters[`markConfiguration`] = JSON.stringify(filteredMarkConfiguration);
  }

  return {
    queryCriteria: criteria,
    queryParameters: queryParameters
  }
}


const generateUserAccessControlFilterCriteria = (userProjectList, claims, prefix = '') => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateUserAccessControlFilterCriteria()");

  let criteria = [];
  let queryParameters = {};

  // 1) ANY PROJECT THEY BELONG TO;
  if (userProjectList.length > 0) {
    console.log(`User project list criteria: ${userProjectList.map(projectId => `project_id = ${projectId}`).join(' OR ')}`);
    criteria.push(`( ${userProjectList.map((projectId, idx) => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each project
      queryParameters[`userProjectId${idx}`] = projectId;
      // Return the prepared statement clause for joining up with other components
      return `project_id = \${userProjectId${idx}}`
    }).join(' OR ')} )`);
  }

  // 2) ANY DATA THEY HAVE SUBMITTED
  criteria.push(`( ${[
    `event_reporter_id = \${userReporterId}`,
    `event_provider_id = \${userProviderId}`,
    `event_owner_id = \${userOwnerId}`
  ].join(' OR ')} )`);
  // --
  queryParameters = {
    ...queryParameters,
    userReporterId: claims.sub,
    userProviderId: claims.sub,
    userOwnerId: claims.sub
  }

  // 3) ANY PUBLIC/NON-MORATORIUM DATA
  criteria.push(`( ${[
    `default_moratorium_expiry < \${default_moratorium_expiry_limit}`,
    `default_moratorium_expiry IS NULL`
  ].join(' OR ')} )`);
  // --
  queryParameters = {
    ...queryParameters,
    default_moratorium_expiry_limit: Moment().format()
  }


  console.log(`( ${criteria.join(' OR ')} )`);

  return {
    queryCriteria: [`( ${criteria.join(' OR ')} )`],
    queryParameters: queryParameters
  };
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

  let queryCriteria = [];
  let queryParameters = null;
  let numParams = 0;

  let body = JSON.parse(event.body);

  // Set standard materialized view name and sort order
  let viewName = 'advanced_search_events';

  let options = {
    limit: limit + 1,
    fields: [
      'id', 'event_type', 'bird_id', 'row_creation_timestamp_', 'event_timestamp',
      'event_reporter_id', 'event_provider_id', 'event_owner_id', 'event_banding_scheme',
      'event_bird_situation', 'latitude', 'longitude', 'location_description', 'project_id',
      'project_name', 'default_moratorium_expiry', 'species_id', 'friendly_name', 'common_name_nznbbs', 'scientific_name_nznbbs',
      'species_code_nznbbs', 'species_group_id', 'species_group_name', 'reporter_nznbbs_certification_number',
      'provider_nznbbs_certification_number', 'owner_nznbbs_certification_number',
      'characteristic_measurement_id', 'characteristic_id', 'characteristic_measurement_value',
      'agg_mc'
    ]
  }

  // Initial check to see if more ownerous distinct/view based search is required or whether we can rely solely on the materialized view
  if (('bandNumber' in body && body.bandNumber.length > 0) || ('otherAlphanumeric' in body && body.otherAlphanumeric.length > 0)) {
    // -------------------------------------------------
    // Update view name to include join on individual bands and ensure distinct option is added to prevent duplicates
    viewName = 'vw_advanced_search_events';
    options.distinct = true;
  }

  if (paginationToken) {
    options.offset = paginationToken;
  }

  // ------------------------------------
  // QUERY STRING SORT COLUMN AND ORDER
  // ------------------------------------
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
    let userAccessControlCriteriaAndParams = generateUserAccessControlFilterCriteria(userProjectList, claims);
    queryCriteria = [...queryCriteria, ...userAccessControlCriteriaAndParams.queryCriteria];
    queryParameters = userAccessControlCriteriaAndParams.queryParameters;
  }

  console.log(queryCriteria);
  console.log(queryParameters);

  // -----------------------------------
  // QUERY STRING SEARCHING & FILTERING
  // -----------------------------------
  let searchCriteriaAndParams = genereateSearchQuery(event);
  queryCriteria = [...queryCriteria, ...searchCriteriaAndParams.queryCriteria];
  queryParameters = (queryParameters)
    ? { ...queryParameters, ...searchCriteriaAndParams.queryParameters }
    : searchCriteriaAndParams.queryParameters;

  console.log(JSON.stringify(queryCriteria));
  console.log(JSON.stringify(queryParameters));

  let resultSet = [];

  if (queryCriteria.length === 0) {
    // Open search - i.e. no access control or query string requirements
    console.log(`No search parameters added for this search`);
    console.log(viewName);
    console.log(options);

    resultSet = await db[viewName].find(
      { 'id IS NOT': null },
      options
    );
  }
  else {
    // Parameterised search
    console.log(`Searching based on: ${queryCriteria.join(' AND ')}`);
    console.log(JSON.stringify(queryParameters));
    console.log(viewName);
    console.log(options);
    resultSet = await db[viewName].where(
      queryCriteria.join(' AND '),
      queryParameters,
      options
    );
  }

  console.log(`Resultset length: ${resultSet.length}`);

  // Check project updates have not occurred
  if (resultSet.length > 0) { 
    // --------------------------------------------
    let projectCheckCriteria = {
      or: resultSet.map((event, idx) => {
        return { 'id =': event.id };
      })
    };

    let projectResultSet = await db.vw_events_simple.find(projectCheckCriteria, {
      fields: [
      'id', 'project_name'
      ]
    });

    resultSet = resultSet.map(event => {
      // -----------------------------------------
      let isProjectUpdate = projectResultSet.filter(realtimeEvent => {
        // ---------------------------------------------
        return (realtimeEvent.id === event.id && realtimeEvent.project_name === event.project_name)
      }).length === 0;
      
      if (isProjectUpdate) {
        event.project_name = `${event.project_name}*`
      }
      return event;
    });
  }

  return {
    event_metadata: (resultSet.length > limit) ? resultSet.slice(0, resultSet.length - 1) : resultSet,
    count: (resultSet.length > limit) ? (resultSet.length - 1) : resultSet.length,
    sortOrder: sortOrder,
    isLastPage: resultSet.length <= limit,
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
    let primaryMarkConfig = event.agg_mc.find(markConfig => {
      return markConfig.mark_id;
    })
    result.prefix_number = (typeof primaryMarkConfig !== 'undefined') ? primaryMarkConfig.alphanumeric_text.split('-')[0].toUpperCase() : null;
    result.short_number = (typeof primaryMarkConfig !== 'undefined') ? primaryMarkConfig.alphanumeric_text.split('-')[1].toUpperCase() : null;

    // 3) Attempt to add nznbbs_code
    result.historic_nznbbs_code = BBHelpers.deriveNznbbsCode({
      ...event,
      characteristic_measurement: [{ characteristic_id: event.characteristic_id, value: event.characteristic_measurement_value }]
    });

    // 4) Attempt to add colour_bands field
    result.other_bands = (event.agg_mc.filter(markConfig => { return !markConfig.mark_id; }).length > 0) ? true : false;

    // Remove temporary properties
    delete result.characteristic_id;
    delete result.characteristic_measurement_value;
    delete result.characteristic_measurement_id;
    delete result.species_group_id;
    delete result.species_id;
    delete result.agg_mc;
    delete result.value;

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
  var payloadSchema = {};

  // Payload
  var payload = JSON.parse(event.body);

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
      console.info('Querystring parameters OK. Valiating search payload...');
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
        // Special handling for band number pattern error instead of returning ugly regex
        // Point solution for now - no UI handling of error messages
        formattedErrors = formattedErrors.map(error => {
          if(error.keyword === 'pattern' && error.property.split('/').length > 1 && error.property.split('/')[1] === 'bandNumber') {
            error.message = 'Band number should be in the following format \'(prefix)-(shortnumber)\'. eg: A-1234 or A-* or *-1234';
          }
          return error;
        });
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload schema validation error(s)!`, formattedErrors);
      }
      // Also check that no blank (i.e. all null) markConfig submitted
      else if (payload.markConfiguration.filter(markConfig => {
        return (markConfig.side === null && markConfig.position === null && markConfig.location_idx === null
          && markConfig.mark_type === null && markConfig.mark_form === null && markConfig.mark_material === null
          && markConfig.mark_fixing === null && markConfig.colour === null && markConfig.text_colour === null
          && markConfig.alphanumeric_text === null)
      }).length > 0) {
        let error = customErrorFactory.getError('NullListObjectProperties', ['markConfiguration', JSON.stringify(payload.markConfiguration), `/markConfiguration`]);
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload validation error(s)!`, [error]);
      }
      console.info('Event batch payload OK. Validating business rules...');

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
    .then(userProjectList => {
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
