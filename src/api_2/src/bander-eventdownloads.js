'use strict';

// Modules
const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const MomentTimezone = require('moment-timezone');
const sqlString = require('sqlstring');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBSSHelpers = require('bb-spreadsheet-helpers');
const BBHelpers = require('bb-helpers');
const Helpers = require('helpers.js')
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

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

// +++
// Force manual database connection each time download function is called
let db = undefined;
let containerCreationTimestamp;
let dbCreationTimestamp = undefined;

const RESOURCE_NAME = "bander_event_downloads";

const ALLOWABLE_SORT_ORDERS = BBHelpers.ALLOWABLE_EVENT_SORT_ORDERS;
const ALLOWABLE_FILTERS = BBHelpers.ALLOWABLE_EVENT_S_FILTERS;
const ALLOWABLE_SEARCHES = BBHelpers.ALLOWABLE_EVENT_S_SEARCHES;

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

const generateQueryStringFilterCriteriaToString = (multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringFilterCriteria()");

  let criteria = [];

  let nonFilterQueryStringParameters = [ ...['limit', 'paginationToken'], ...Object.keys(ALLOWABLE_SEARCHES)];
  
  // If there are any query string parameters, we need to process them, otherwise, continue
  if (multiValueQueryStringParameters && Object.keys(multiValueQueryStringParameters).length > 0) {
    // ----------------------------------------------------------------------------   
    let filterQueryStringParameterNames = Object.keys(multiValueQueryStringParameters).filter(queryStringParam => !nonFilterQueryStringParameters.includes(queryStringParam) );

    filterQueryStringParameterNames.map(filterQueryStringParameterName => {
      // --------------------------
      // If a recognised search parameter, add corresponding criteria
      if (filterQueryStringParameterName in ALLOWABLE_FILTERS) {
        // ---------------------------------------
        let innerCriteria = [];

        multiValueQueryStringParameters[filterQueryStringParameterName].map(filterQueryStringParameterValue => {
          // ---------------------------
          // If region name is submitted, convert to the code
          if(filterQueryStringParameterName === 'nznbbsCode') {
            let nznbbsCodeCriteria = deriveWhereNznbbsCodeCriteria(filterQueryStringParameterValue);
            if (nznbbsCodeCriteria) innerCriteria.push(nznbbsCodeCriteria);
          }
          else if (filterQueryStringParameterName === 'eventDate') {
            // Event timestamp filters need the New Zealand timezone applied to them
            ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name.map(filterName => {
              innerCriteria.push(`${filterName} ${sqlString.escape(MomentTimezone.tz(filterQueryStringParameterValue, 'NZ').format('YYYY-MM-DD'))}`);
            });
          }
          else if (['eventDateGte', 'eventDateLte'].includes(filterQueryStringParameterName)) {
            // Event timestamp range filters need the New Zealand timezone applied to them as well as the beginning/end of the day
            let timeFilter = (filterQueryStringParameterName === 'eventDateGte') ? '00:00:00' : '23:59:59';
            ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name.map(filterName => {
              innerCriteria.push(`${filterName} ${sqlString.escape(MomentTimezone.tz(`${filterQueryStringParameterValue} ${timeFilter}`, 'NZ').format())}`);
            });
          }
          else {
            ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name.map(filterName => {
              innerCriteria.push(`${filterName} ${sqlString.escape(filterQueryStringParameterValue)}`);
            });
          }
          return;
        });

        innerCriteria = `(${innerCriteria.join(' OR ')})`;
        criteria.push(innerCriteria);
      }
    });
  }
  // -----------------------------------
  let response = criteria.join(' AND ');
  return response;
}

const processWildcardSearchToString = (paramName, paramValue) => {
  // ----------------------------------------------------------------------------
  paramValue = paramValue.toLowerCase();
  if ( paramValue.includes('*')) {
    // ------------------------------------------------
    paramValue = paramValue.split('*').join('%');
    return  `${ALLOWABLE_SEARCHES[paramName].wildcard_filter_name} ${sqlString.escape(paramValue)}`;
  }
  else {
    // ------------------------------------------------
    return `${ALLOWABLE_SEARCHES[paramName].filter_name} ${sqlString.escape(paramValue)}`;
  }
}

const generateQueryStringSearchCriteriaToString = (multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringSearchCriteria()");

  let criteria = [];

  let nonSearchQueryStringParameters = [ ...['limit', 'paginationToken'], ...Object.keys(ALLOWABLE_FILTERS)];
  
  // If there are any query string parameters, we need to process them, otherwise, continue
  if (multiValueQueryStringParameters && Object.keys(multiValueQueryStringParameters).length > 0) {
    // -----------------------------------------------------------
    let searchQueryStringParameterNames = Object.keys(multiValueQueryStringParameters).filter(queryStringParam => !nonSearchQueryStringParameters.includes(queryStringParam) );

    searchQueryStringParameterNames.map(searchQueryStringParameterName => {
      // --------------------------
      // If a recognised search parameter, add corresponding criteria
      if (searchQueryStringParameterName in ALLOWABLE_SEARCHES) {
        multiValueQueryStringParameters[searchQueryStringParameterName].map(searchQueryStringParameterValue => {
          // ---------------------------
          criteria.push(`${processWildcardSearchToString(searchQueryStringParameterName, searchQueryStringParameterValue)}`)
        });
      }
    });
  }
  
  // -----------------------------------
  let response = criteria.join(' AND ');
  return response;
}

const generateUserProjectCriteriaToString = (projectList, prefix = '') => {
  // ----------------------------------------------------------------------------   
  return projectList.map(projectId => {
    return `${prefix}project_id = ${sqlString.escape(projectId)}`;
  }).join(' OR ');
}

const generateUserAccessControlFilterCriteriaToString = (userProjectList, claims, prefix = '') => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateUserAccessControlFilterCriteriaToString()");

  let criteria = [];
  
  // 1) ANY PROJECT THEY BELONG TO
  let userProjectCrtieria = generateUserProjectCriteriaToString(userProjectList, prefix);
  if (userProjectCrtieria.length > 0) {
    console.log('User project list criteria: ', userProjectCrtieria);
    criteria.push(`(${userProjectCrtieria})`);
  }

  // 2) ANY DATA THEY HAVE SUBMITTED
  criteria.push(
    `(${
      [
        `${prefix}event_reporter_id = ${sqlString.escape(claims.sub)}`,
        `${prefix}event_provider_id = ${sqlString.escape(claims.sub)}`,
        `${prefix}event_owner_id = ${sqlString.escape(claims.sub)}`
      ].join(' OR ')
    })`
  );

  // 3) ANY PUBLIC/NON-MORATORIUM DATA
  criteria.push(
    `(${
      [
        `${prefix}default_moratorium_expiry < ${sqlString.escape(MomentTimezone().tz('NZ').format())}`,
        `${prefix}default_moratorium_expiry IS ${null}`
      ].join(' OR ')
    })`
  );
  
  let response = criteria.join(' OR ');
  console.log('USER ACCESS CRITERIA: ', response);
  return response;
}

const validateExportRequest = (event, banderDownload = null) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".validateExportRequest()");

  // Validate that requestor matches path parameter
  if (!event.requestContext.authorizer.claims['cognito:groups'].includes('admin') && event.requestContext.authorizer.claims.sub !== event.pathParameters.banderId) {
    throw new BoilerPlate.ParameterValidationError(`Requestor does not match banderId path parameter!`, { message: `The requestor ${event.requestContext.authorizer.claims.sub}, and path bander id, ${event.pathParameters.banderId}, do not match.`, path: `pathParameters.banderId`, value: event.pathParameters.banderId, schema: 'API_GATEWAY' });
  }

  // Check that for non-admin that download access is allowed (i.e. no moratorium or admin has updated to download_)
  if (event.httpMethod === 'PUT' && !event.requestContext.authorizer.claims['cognito:groups'].includes('admin') && banderDownload.download_status === 'REQUEST_PENDING_APPROVAL') {
    throw new BoilerPlate.AuthorisationError(`This download can't be started - requires administrator approval!`, { message: `The download ${banderDownload.id}, requires administrator approval because it may contain moratorium data.`, path: 'pathParameters.banderDownloadId', value: event.pathParameters.banderDownloadId, schema: 'API_GATEWAY' });
  }

  // Check for valid status updates
  if (event.httpMethod === 'PUT' && !['START', 'READY_FOR_DOWNLOAD'].includes(JSON.parse(event.body).download_status)) {
    throw new BoilerPlate.AuthorisationError(`Invalid download_status update for bander downloads!`, { message: `Invalid download_status for an update. Valid values are: START and READY_FOR_DOWNLOAD`, path: 'body.download_status', value: event.pathParameters.banderDownloadId, schema: 'API_GATEWAY' });
  }

  return banderDownload;
}


const generateEventBodySearchQuery = (event) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateEventBodySearchQuery()");

  let criteria = [];

  let searchPayload = JSON.parse(event.body).advanced_search;

  let simpleFilters = Object.keys(searchPayload).filter(key => Object.keys(SIMPLE_FILTERS).includes(key));
  let spatialFilters = Object.keys(searchPayload).filter(key => Object.keys(SPATIAL_FILTERS).includes(key));

  // --------------------
  // Simple filter pattern
  // --------------------
  simpleFilters.map(filter => {
    // -------------------------------
    if (searchPayload[filter].length > 0) {
      // --------------------------------------------------------------------
      criteria.push(`( ${searchPayload[filter].map(value => {
        // -------------------------------------------------------------
        // Return the prepared statement clause for joining up with other components
        return `${SIMPLE_FILTERS[filter].columnName} = ${sqlString.escape(value)}`
      }).join(' OR ')} )`);
    }
  });

  // --------------------
  // UserId
  // --------------------
  if ('userId' in searchPayload && searchPayload.userId.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.userId.map(user => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      return `event_reporter_id = ${sqlString.escape(user)} OR event_provider_id = ${sqlString.escape(user)} OR event_owner_id = ${sqlString.escape(user)}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // EventDate
  // --------------------
  if ('eventDate' in searchPayload && searchPayload.eventDate.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.eventDate.map(eventDate => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      return `( event_timestamp >= ${sqlString.escape(MomentTimezone.tz(`${eventDate} 00:00:00`, 'NZ').format())} AND event_timestamp <= ${sqlString.escape(MomentTimezone.tz(`${eventDate} 23:59:59`, 'NZ').format())} )`
    }).join(' OR ')} )`);
  }

  // --------------------
  // EventDateLte
  // --------------------
  if ('eventDateLte' in searchPayload && searchPayload.eventDateLte.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.eventDateLte.map(eventDateLte => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      return `event_timestamp <= ${sqlString.escape(MomentTimezone.tz(`${eventDateLte} 23:59:59`, 'NZ').format())}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // EventDateGte
  // --------------------
  if ('eventDateGte' in searchPayload && searchPayload.eventDateGte.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.eventDateGte.map(eventDateGte => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      return `event_timestamp >= ${sqlString.escape(MomentTimezone.tz(`${eventDateGte} 00:00:00`, 'NZ').format())}`
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
      criteria.push(`( ${searchPayload[filter].map(value => {
        // -------------------------------------------------------------
        // Return the prepared statement clause for joining up with other components
        return `${SPATIAL_FILTERS[filter].columnName} ${sign} ${sqlString.escape(value)}`
      }).join(' OR ')} )`);
    }
  });

  // --------------------
  // Location Description
  // --------------------
  if ('locationDescription' in searchPayload && searchPayload.locationDescription.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.locationDescription.map(locationDesc => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      return `location_description ILIKE ${sqlString.escape(locationDesc.toLowerCase().split('*').join('%'))}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // Comments
  // --------------------
  if ('comments' in searchPayload && searchPayload.comments.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.comments.map(comment => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      return `comments ILIKE ${sqlString.escape(comment.toLowerCase().split('*').join('%'))}`
    }).join(' OR ')} )`);
  }

  // --------------------
  // FRIENDLY NAME
  // --------------------
  if ('friendlyName' in searchPayload && searchPayload.friendlyName.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.friendlyName.map(friendlyName => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      return `friendly_name ILIKE ${sqlString.escape(friendlyName.toLowerCase().split('*').join('%'))}`
    }).join(' OR ')} )`);
  }
  // --------------------
  // NZNBBS Code
  // --------------------
  if ('nznbbsCode' in searchPayload && searchPayload.nznbbsCode.length > 0) {
    // --------------------------------------------------------------------
    criteria.push(`( ${searchPayload.nznbbsCode.map(nznbbsCode => {
      // -------------------------------------------------------------
      // Add the prepared statement parameter for each project
      let nznbbsCodeCriteria = deriveWhereNznbbsCodeCriteria(nznbbsCode);
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
    criteria.push(`( ${[...searchPayload.bandNumber.map(bandNumber => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      // Note - mark_id not null required to capture tracked stock only
      return `value->>'alphanumeric_text' LIKE ${sqlString.escape(bandNumber.toLowerCase().split('*').join('%'))} AND value->>'mark_id' IS NOT NULL`
    }), ...searchPayload.otherAlphanumeric.map(otherAlpha => {
      // -------------------------------------------------------------
      // Return the prepared statement clause for joining up with other components
      // Note - mark_id not null required to capture tracked stock only
      return `value->>'alphanumeric_text' LIKE ${sqlString.escape(otherAlpha.toLowerCase().split('*').join('%'))}`
    })].join(' OR ')} )`);

  }
  else {
    // ----------------------------------------------------------
    // --------------------
    // Band Number
    // --------------------
    if ('bandNumber' in searchPayload && searchPayload.bandNumber.length > 0) {
      // --------------------------------------------------------------------
      criteria.push(`( ${searchPayload.bandNumber.map(bandNumber => {
        // -------------------------------------------------------------
        // Return the prepared statement clause for joining up with other components
        // Note - mark_id not null required to capture tracked stock only
        return `value->>'alphanumeric_text' LIKE ${sqlString.escape(bandNumber.toLowerCase().split('*').join('%'))} AND value->>'mark_id' IS NOT NULL`
      }).join(' OR ')} )`);
    }

    // --------------------
    // Other alphanumeric
    // --------------------
    if ('otherAlphanumeric' in searchPayload && searchPayload.otherAlphanumeric.length > 0) {
      // --------------------------------------------------------------------
      criteria.push(`( ${searchPayload.otherAlphanumeric.map(otherAlpha => {
        // -------------------------------------------------------------
        // Return the prepared statement clause for joining up with other components
        // Note - mark_id not null required to capture tracked stock only
        return `value->>'alphanumeric_text' LIKE ${sqlString.escape(otherAlpha.toLowerCase().split('*').join('%'))}`
      }).join(' OR ')} )`);
    }
  }

  // --------------------
  // Mark Configuration
  // --------------------
  if ('markConfiguration' in searchPayload && searchPayload.markConfiguration.length > 0) {
    // --------------------------------------------------------------------
    // -- Remove properties with null values
    let filteredMarkConfiguration = searchPayload.markConfiguration.map(markConfig => {
      // Add the prepared statement parameter for each project
      Object.keys(markConfig).forEach((key) => (markConfig[key] === null) && delete markConfig[key]);
      if ('alphanumeric_text' in markConfig) {
        markConfig.alphanumeric_text = markConfig.alphanumeric_text.toLowerCase();
      }

      return markConfig;
    });


    console.log(JSON.stringify(filteredMarkConfiguration));

    criteria.push(`( agg_mc @> '${JSON.stringify(filteredMarkConfiguration)}' )`);
  }

  // -----------------------------------
  console.log(`Advanced search and filter criteria: ${JSON.stringify(criteria)}`);
  let response = criteria.join(' AND ');
  return response;  
}

const generateSimpleQuery = (event, claims, governingCognitoGroup, userProjectList) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateSimpleQuery()");

  let criteria = '';
  let moratoriumCheckCriteria = ''

  // Generate search criteria
  let filterCriteria = generateQueryStringFilterCriteriaToString(event.multiValueQueryStringParameters);
  criteria += filterCriteria;

  let searchCriteria = generateQueryStringSearchCriteriaToString(event.multiValueQueryStringParameters);
  criteria += (criteria.length > 0 && searchCriteria.length > 0) ? 
                ` AND ${searchCriteria}`:
                `${searchCriteria}`;

  // -----------------------
  // USER ACCESS CONTROL
  // -----------------------
  // If a user is anything besides admin we need to manage their access to the banding records
  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    // ------------------------------------------------
    let userAccessCriteria = generateUserAccessControlFilterCriteriaToString(userProjectList, claims, '');
    criteria += (criteria.length > 0 && userAccessCriteria.length > 0) ? 
    ` AND (${userAccessCriteria})`:
    `(${userAccessCriteria})`;
  }

  // Modified criteria is used to check against moratorium projects (only one at present)
  moratoriumCheckCriteria = `${criteria} AND default_moratorium_expiry > ${sqlString.escape(MomentTimezone().tz('NZ').format())}`;

  console.log(`Moratorium check criteria: ${moratoriumCheckCriteria}`);

  return  {
    full: (criteria.length > 0) ? `where ${criteria}` : ``,
    count: (criteria.length > 0) ? `with unique_events as (SELECT distinct id FROM vw_events_simple where ${criteria}) SELECT count(id)::int from unique_events;` : `with unique_events as (SELECT distinct id FROM vw_events_simple) SELECT count(id)::int from unique_events;`,
    moratorium_review: (criteria.length > 0) ? `SELECT count(sub.*)::int FROM (SELECT distinct id FROM vw_events_simple AS se where ${moratoriumCheckCriteria} LIMIT 1) AS sub;` : `SELECT count(sub.*)::int FROM (SELECT distinct id FROM vw_events_simple LIMIT 1) AS sub;`
  }
}

const generateAdvancedQuery = (event, claims, governingCognitoGroup, userProjectList) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateAdvancedQuery()");

  let criteria = '';
  let moratoriumCheckCriteria = ''

  // Generate search criteria
  let searchAndFilterCriteria = generateEventBodySearchQuery(event);
  criteria += searchAndFilterCriteria;

  // -----------------------
  // USER ACCESS CONTROL
  // -----------------------
  // If a user is anything besides admin we need to manage their access to the banding records
  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    // ------------------------------------------------
    let userAccessCriteria = generateUserAccessControlFilterCriteriaToString(userProjectList, claims, '');
    criteria += (criteria.length > 0 && userAccessCriteria.length > 0) ? 
    ` AND (${userAccessCriteria})`:
    `(${userAccessCriteria})`;
  }

  // Modified criteria is used to check against moratorium projects (only one at present)
  moratoriumCheckCriteria = `${criteria} AND default_moratorium_expiry > ${sqlString.escape(MomentTimezone().tz('NZ').format())}`;

  console.log(`Moratorium check criteria: ${moratoriumCheckCriteria}`);

  console.log(`Full generated criteria: ${JSON.stringify(criteria)}`);

  return  {
    full: (criteria.length > 0) ? `where ${criteria}` : ``,
    count: (criteria.length > 0) ? `with unique_events as (SELECT distinct id FROM advanced_search_events where ${criteria}) SELECT count(id)::int from unique_events;` : `with unique_events as (SELECT distinct id FROM vw_events_simple) SELECT count(id)::int from unique_events;`,
    moratorium_review: (criteria.length > 0) ? `SELECT count(sub.*)::int FROM (SELECT distinct id FROM advanced_search_events AS se where ${moratoriumCheckCriteria} LIMIT 1) AS sub;` : `SELECT count(sub.*)::int FROM (SELECT distinct id FROM vw_events_simple LIMIT 1) AS sub;`
  }
}

const generateExportQuery = (event, claims, governingCognitoGroup, userProjectList, isAdvanced) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateExportCriteria()");

  if (isAdvanced) {
    return generateAdvancedQuery(event, claims, governingCognitoGroup, userProjectList);
  }

  return generateSimpleQuery(event, claims, governingCognitoGroup, userProjectList);
}

const getRecordCount = (db, event, query) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getRecordCount()");

  console.log(`Query for full count: ${query.count}`);

  return db.ro_search_events_count(query.count);
}

const checkMoratoriumRecords = (db, event, query) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".checkMoratoriumRecords()");

  console.info(`Query for moratorium: ${query.moratorium_review}`);

  return db.ro_search_events_count(query.moratorium_review);
}

const createExportRequest = (event, query, number_of_rows, has_moratorium_data, is_advanced=false) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".createExportRequest()");

  let exportRequest = {
    "download_status":  "READY_FOR_DOWNLOAD", // NOTE: We have removed request pending approval from here because given user access controls, approval won't be required
    "query": query.full,
    "number_of_rows": number_of_rows,
    "file_size_in_bytes": null,
    "moratorium_data_included": has_moratorium_data,
    "object_path": null,
    "object_version": null,
    "bander_id": event.requestContext.authorizer.claims.sub,
    "is_advanced": is_advanced
  };

  return exportRequest;
}

const getBanderDownloadFromDB = (db, event) => {
  // ----------------------------------------------------------------------------   
  let criteria = {};
  let idOperation = 'id =';
  criteria[idOperation] = event.pathParameters.banderDownloadId;

  // -----------------------------------
  console.log(criteria);


  return db.bander_downloads
    .find(criteria, 
      { 
        order: [{ field: 'download_status', direction: 'asc' }],
      });
}

const listBanderDownloadsFromDB = (db, event) => {
  // ----------------------------------------------------------------------------   
  let criteria = {};
  let idOperation = 'bander_id =';
  criteria[idOperation] = event.pathParameters.banderId;

  // Add seven day window to keep downloads fresh (if missing bander can request a new download)
  criteria['row_creation_timestamp_ >='] = Moment().subtract(7, 'day').format();

  // Add a requirement for download_state NOT equal READY_FOR_DOWNLOAD 
  // (bander has not commenced download in this case)
  criteria['download_status !='] = 'READY_FOR_DOWNLOAD';

  // -----------------------------------
  console.log(criteria);


  return db.bander_downloads
    .find(criteria, 
      { 
        order: [{ field: 'download_status', direction: 'asc' }],
      });
}

const getPresignedURL = (payload) => {
  // ----------------------------------------------------------------------------   
  return new Promise((resolve, reject) => { 
    console.info(RESOURCE_NAME + ".getPresignedURL()");

    var s3 = new AWS.S3({ signatureVersion:'v4' });

    let params = {
      Bucket: process.env.USER_ASSETS_BUCKET,
      Key: payload.object_path,
      VersionId: null
    };

    console.log('Presigned url generation params: ', params);

    console.info('[INFO] presigned url request params: ', JSON.stringify(params));

    let url = s3.getSignedUrl('getObject', params);

    let response = payload;

    response.presigned_url = url;

    resolve(response);
  });
}

const pushDownloadToDB = (db, payload) => {
  // ----------------------------------------------------------------------------   

  return db.bander_downloads.save(payload);
}

const updateDownloadToDB = (db, payload) => {
  // ----------------------------------------------------------------------------   

  return db.bander_downloads.update(payload.id, payload);
}

const cleanInactiveDownloads = (db, event) => {
  // ----------------------------------------------------------------------------   
  console.info('[INFO] Cleaning up inactive downloads');

  let criteria = {};

  // Cleanup records over 1 day old that are in 'AVAIALABLE_FOR_DOWNLOAD' status
  criteria['row_creation_timestamp_ <'] = Moment().subtract(1, 'hour').format();
  criteria['download_status ='] = 'READY_FOR_DOWNLOAD';
  criteria['bander_id ='] = event.requestContext.authorizer.claims.sub;

  console.log(criteria);

  return db.bander_downloads.destroy(criteria)
          .then(res => {
            let expiredCriteria = {
              'row_creation_timestamp_ <': Moment().subtract(1, 'week').format(),
              'bander_id =': event.requestContext.authorizer.claims.sub
            }

            return db.bander_downloads.destroy(expiredCriteria)
          });
}

const commenceExport = (db, payload, governingCognitoGroup) => {
  // ----------------------------------------------------------------------------   
  var sqs = new AWS.SQS();

  let params = {
    MessageBody: JSON.stringify({ 
      'bander_download': {
        ...payload,
        governingCognitoGroup: governingCognitoGroup
      }
    }), /* required */
    QueueUrl: process.env.EXPORT_SQS_URL
  };

  console.log(params);

  return sqs.sendMessage(params).promise()
    .then(res => {
      let updatedDownload = {
        ...payload
      };
      updatedDownload.download_status = 'IN_PROGRESS'; 
      
      console.log(updatedDownload)

      return db.bander_downloads.update(updatedDownload.id, updatedDownload);
    })
    .catch(err => {
      throw new BoilerPlate.ServiceError(`Download start failed - SQS trigger failure`, { message: `The download ${payload.id}, could not be added to the SQS queue to commence the download.`, path: null, value: null, schema: 'SNS' });
    });
}

const convertAvailableDownloadsToPresignedUrls = (downloadList) => {
  // ----------------------------------------------------------------------------   
  let promises = [];

  promises = downloadList.map(download => {
    // -------------------------------------------
    if (download.download_status === 'AVAILABLE_FOR_DOWNLOAD') {
      return getPresignedURL(download);
    }
    let updatedDownload = download;
    updatedDownload.presigned_url = null;
    return Promise.resolve(updatedDownload);
  });

  return Promise.all(promises)
    .then(res => {
      console.log(res);

      return res;
    });
}

const getExportQuery = (governingCognitoGroup, downloadPayload, isAdvanced=false) => {
  // -----------------------------------------------------------
  console.info(RESOURCE_NAME + ".getExportCriteria()");

  if (isAdvanced) {
    return getAdvancedExportQuery(governingCognitoGroup, downloadPayload);
  }

  return getSimpleExportQuery(governingCognitoGroup, downloadPayload);
}

const getSimpleExportQuery = (governingCognitoGroup, downloadPayload) => {
  // -----------------------------------------------------------
  console.info(RESOURCE_NAME + ".getSimpleExportQuery()");

  let columnSelection = (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) ? 
  // Standard user selection of columns (this should only contain non-sensitive information)
  `
    select ej.id as event_id, to_char(ej.event_timestamp AT TIME ZONE 'NZ', 'YYYY-MM-DD HH24:MI:SS') AS event_timestamp, 
    ej.event_timestamp_accuracy, ej.event_type, ej.event_banding_scheme, 
    ej.event_bird_situation AS wild_captive, ej.event_capture_type, 
    ej.latitude, ej.longitude, ej.location_description, ej.location_comment, ej.locality_general, ej.locality_accuracy,
    b.id as bird_id, s.species_code_nznbbs, s.scientific_name_nznbbs AS scientific_name, s.common_name_nznbbs AS common_name, 
    sg.name AS species_group_name,
    cmstatus.value AS status_code,
    cmage.value AS age,
    cmsex.value AS sex,
    p.name AS projct_name,
    bar.nznbbs_certification_number as reporter_cert_number,
    bap.nznbbs_certification_number AS provider_cert_number,
    bao.nznbbs_certification_number As owner_cert_number,
    mcat.agg_mc AS alphanumerics,
    mclti.agg_mc AS left_tibia,
    mclta.agg_mc AS left_tarsus,
    mcrti.agg_mc AS right_tibia,
    mcrta.agg_mc AS right_tarsus,
    mco.agg_mc AS other_marks,
    cm.agg_cm AS detail_characteristic_measurements, 
    cmmoult.value AS moult_score,
    ej.comments
  `
  :
  // Admin selection of columns (this includes person names!)
  `
    select ej.id as event_id, to_char(ej.event_timestamp AT TIME ZONE 'NZ', 'YYYY-MM-DD HH24:MI:SS') AS event_timestamp, 
    ej.event_timestamp_accuracy, ej.event_type, ej.event_banding_scheme, 
    ej.event_bird_situation AS wild_captive, ej.event_capture_type, 
    ej.latitude, ej.longitude, ej.location_description, ej.location_comment, ej.locality_general, ej.locality_accuracy,
    b.id as bird_id, s.species_code_nznbbs, s.scientific_name_nznbbs AS scientific_name, s.common_name_nznbbs AS common_name,
    sg.name AS species_group_name,
    cmstatus.value AS status_code,
    cmage.value AS age,
    cmsex.value AS sex,
    p.name AS projct_name,
    bar.nznbbs_certification_number as reporter_cert_number, bar.person_name as reporter_name,
    bap.nznbbs_certification_number AS provider_cert_number, bap.person_name AS provider_name,
    ej.event_other_person_name AS other_name, ej.event_other_contact AS other_email,
    bao.nznbbs_certification_number As owner_cert_number, bao.person_name AS owner_name,
    mcat.agg_mc AS alphanumerics,
    mclti.agg_mc AS left_tibia,
    mclta.agg_mc AS left_tarsus,
    mcrti.agg_mc AS right_tibia,
    mcrta.agg_mc AS right_tarsus,
    mco.agg_mc AS other_marks,
    cm.agg_cm AS detail_characteristic_measurements,
    cmmoult.value AS moult_score,
    ej.comments
  `;

  console.log(`Generating query with criteria: ${downloadPayload.query}`);

  // TODO -> Refactor job - convert to view and submit criteria only...
  let fullExportQuery = `
    ${columnSelection}
    FROM 
	(
        WITH event AS
        (SELECT DISTINCT id FROM vw_events_simple ${downloadPayload.query})
      SELECT event.id
      FROM event
    ) as e 
	LEFT JOIN
    (
        WITH event AS
        (SELECT DISTINCT id FROM vw_events_simple ${downloadPayload.query})
      SELECT event.id, string_agg(char.name::text || ':' || cm.value::text, CHR(10)) AS agg_cm
      FROM event
      LEFT JOIN characteristic_measurement as cm on cm.event_id = event.id
      LEFT JOIN characteristic as char on char.id = cm.characteristic_id
      WHERE cm.characteristic_id NOT IN (38, 39, 40, 43)
      GROUP BY event.id
    ) as cm ON cm.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM vw_events_simple ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
        FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE mc.side = 'LEFT' AND mc.position = 'TIBIA'
      GROUP BY event.id
    ) as mclti ON mclti.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM vw_events_simple ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE mc.side = 'LEFT' AND mc.position = 'TARSUS'
      GROUP BY event.id
    ) as mclta ON mclta.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM vw_events_simple ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE mc.side = 'RIGHT' AND mc.position = 'TIBIA'
      GROUP BY event.id
    ) as mcrti ON mcrti.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM vw_events_simple ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE mc.side = 'RIGHT' AND mc.position = 'TARSUS'
      GROUP BY event.id
    ) as mcrta ON mcrta.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM vw_events_simple ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE (mc.side != 'RIGHT' AND mc.side != 'LEFT') OR (mc.position != 'TIBIA' AND mc.position != 'TARSUS')
      GROUP BY event.id
    ) as mco ON mco.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM vw_events_simple ${downloadPayload.query})
      SELECT event.id, string_agg(
        COALESCE(mc.alphanumeric_text::text, 'null'), ',') AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
	    WHERE mc.alphanumeric_text IS NOT NULL
      GROUP BY event.id
    ) as mcat ON mcat.id = e.id
    INNER JOIN event as ej ON ej.id = e.id 
    INNER JOIN project as p ON p.id = ej.project_id
    LEFT JOIN bird as b ON b.id = ej.bird_id 
    LEFT JOIN species as s ON s.id = b.species_id
    INNER JOIN species_group_membership AS sgm ON sgm.species_id = s.id
    INNER JOIN species_group AS sg ON sg.id = sgm.group_id 
    LEFT JOIN bander as bar ON bar.id = ej.event_reporter_id
    LEFT JOIN bander as bap ON bap.id = ej.event_provider_id
    LEFT JOIN bander as bao ON bao.id = ej.event_owner_id
     LEFT JOIN characteristic_measurement AS cmstatus ON cmstatus.event_id = e.id AND cmstatus.characteristic_id = 43
     LEFT JOIN characteristic_measurement AS cmage ON cmage.event_id = e.id AND cmage.characteristic_id = 39
     LEFT JOIN characteristic_measurement AS cmsex ON cmsex.event_id = e.id AND cmsex.characteristic_id = 40
     LEFT JOIN characteristic_measurement AS cmmoult ON cmmoult.event_id = e.id AND cmmoult.characteristic_id = 38
  `
  return fullExportQuery;
}

const getAdvancedExportQuery = (governingCognitoGroup, downloadPayload) => {
  // -----------------------------------------------------------
  console.info(RESOURCE_NAME + ".getAdvancedExportQuery()");

  let columnSelection = (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) ? 
  // Standard user selection of columns (this should only contain non-sensitive information)
  `
    select ej.id as event_id, to_char(ej.event_timestamp AT TIME ZONE 'NZ', 'YYYY-MM-DD HH24:MI:SS') AS event_timestamp, 
    ej.event_timestamp_accuracy, ej.event_type, ej.event_banding_scheme, 
    ej.event_bird_situation AS wild_captive, ej.event_capture_type, 
    ej.latitude, ej.longitude, ej.location_description, ej.location_comment, ej.locality_general, ej.locality_accuracy,
    b.id as bird_id, s.species_code_nznbbs, s.scientific_name_nznbbs AS scientific_name, s.common_name_nznbbs AS common_name, 
    sg.name AS species_group_name,
    cmstatus.value AS status_code,
    cmage.value AS age,
    cmsex.value AS sex,
    p.name AS projct_name,
    bar.nznbbs_certification_number as reporter_cert_number,
    bap.nznbbs_certification_number AS provider_cert_number,
    bao.nznbbs_certification_number As owner_cert_number,
    mcat.agg_mc AS alphanumerics,
    mclti.agg_mc AS left_tibia,
    mclta.agg_mc AS left_tarsus,
    mcrti.agg_mc AS right_tibia,
    mcrta.agg_mc AS right_tarsus,
    mco.agg_mc AS other_marks,
    cm.agg_cm AS detail_characteristic_measurements, 
    cmmoult.value AS moult_score,
    ej.comments
  `
  :
  // Admin selection of columns (this includes person names!)
  `
    select ej.id as event_id, to_char(ej.event_timestamp AT TIME ZONE 'NZ', 'YYYY-MM-DD HH24:MI:SS') AS event_timestamp, 
    ej.event_timestamp_accuracy, ej.event_type, ej.event_banding_scheme, 
    ej.event_bird_situation AS wild_captive, ej.event_capture_type, 
    ej.latitude, ej.longitude, ej.location_description, ej.location_comment, ej.locality_general, ej.locality_accuracy,
    b.id as bird_id, s.species_code_nznbbs, s.scientific_name_nznbbs AS scientific_name, s.common_name_nznbbs AS common_name,
    sg.name AS species_group_name,
    cmstatus.value AS status_code,
    cmage.value AS age,
    cmsex.value AS sex,
    p.name AS projct_name,
    bar.nznbbs_certification_number as reporter_cert_number, bar.person_name as reporter_name,
    bap.nznbbs_certification_number AS provider_cert_number, bap.person_name AS provider_name,
    ej.event_other_person_name AS other_name, ej.event_other_contact AS other_email,
    bao.nznbbs_certification_number As owner_cert_number, bao.person_name AS owner_name,
    mcat.agg_mc AS alphanumerics,
    mclti.agg_mc AS left_tibia,
    mclta.agg_mc AS left_tarsus,
    mcrti.agg_mc AS right_tibia,
    mcrta.agg_mc AS right_tarsus,
    mco.agg_mc AS other_marks,
    cm.agg_cm AS detail_characteristic_measurements,
    cmmoult.value AS moult_score,
    ej.comments
  `;

  console.log(`Generating query with criteria: ${downloadPayload.query}`);

  // TODO -> Refactor job - convert to view and submit criteria only...
  let fullExportQuery = `
    ${columnSelection}
    FROM 
	(
        WITH event AS
        (SELECT DISTINCT id FROM advanced_search_events ${downloadPayload.query})
      SELECT event.id
      FROM event
    ) as e 
	LEFT JOIN
    (
        WITH event AS
        (SELECT DISTINCT id FROM advanced_search_events ${downloadPayload.query})
      SELECT event.id, string_agg(char.name::text || ':' || cm.value::text, CHR(10)) AS agg_cm
      FROM event
      LEFT JOIN characteristic_measurement as cm on cm.event_id = event.id
      LEFT JOIN characteristic as char on char.id = cm.characteristic_id
      WHERE cm.characteristic_id NOT IN (38, 39, 40, 43)
      GROUP BY event.id
    ) as cm ON cm.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM advanced_search_events ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
        FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE mc.side = 'LEFT' AND mc.position = 'TIBIA'
      GROUP BY event.id
    ) as mclti ON mclti.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM advanced_search_events ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE mc.side = 'LEFT' AND mc.position = 'TARSUS'
      GROUP BY event.id
    ) as mclta ON mclta.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM advanced_search_events ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE mc.side = 'RIGHT' AND mc.position = 'TIBIA'
      GROUP BY event.id
    ) as mcrti ON mcrti.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM advanced_search_events ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE mc.side = 'RIGHT' AND mc.position = 'TARSUS'
      GROUP BY event.id
    ) as mcrta ON mcrta.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM advanced_search_events ${downloadPayload.query})
      SELECT event.id, string_agg(
        'BAND -' ||
        ' IDX: ' || COALESCE(mc.location_idx::text, 'null') ||
        ' TYPE: ' || COALESCE(mc.mark_type::text, 'null') ||
        ' COLOUR: ' || COALESCE(mc.colour::text, 'null') ||
        ' ALPHANUMERIC-TEXT: (' || COALESCE(mc.alphanumeric_text::text, 'null') || ')' ||
        ' TEXT-COLOUR:' || COALESCE(mc.text_colour::text, 'null') ||
        ' MATERIAL:' || COALESCE(mc.mark_material::text, 'null') ||
        ' MC_ID:' || mc.id, CHR(10) ORDER BY mc.location_idx) AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
      WHERE (mc.side != 'RIGHT' AND mc.side != 'LEFT') OR (mc.position != 'TIBIA' AND mc.position != 'TARSUS')
      GROUP BY event.id
    ) as mco ON mco.id = e.id
    LEFT JOIN
    (
      WITH event AS
        (SELECT DISTINCT id FROM advanced_search_events ${downloadPayload.query})
      SELECT event.id, string_agg(
        COALESCE(mc.alphanumeric_text::text, 'null'), ',') AS agg_mc
      FROM event
      INNER JOIN mark_configuration as mc on mc.event_id = event.id
	    WHERE mc.alphanumeric_text IS NOT NULL
      GROUP BY event.id
    ) as mcat ON mcat.id = e.id
    INNER JOIN event as ej ON ej.id = e.id 
    INNER JOIN project as p ON p.id = ej.project_id
    LEFT JOIN bird as b ON b.id = ej.bird_id 
    LEFT JOIN species as s ON s.id = b.species_id
    INNER JOIN species_group_membership AS sgm ON sgm.species_id = s.id
    INNER JOIN species_group AS sg ON sg.id = sgm.group_id 
    LEFT JOIN bander as bar ON bar.id = ej.event_reporter_id
    LEFT JOIN bander as bap ON bap.id = ej.event_provider_id
    LEFT JOIN bander as bao ON bao.id = ej.event_owner_id
     LEFT JOIN characteristic_measurement AS cmstatus ON cmstatus.event_id = e.id AND cmstatus.characteristic_id = 43
     LEFT JOIN characteristic_measurement AS cmage ON cmage.event_id = e.id AND cmage.characteristic_id = 39
     LEFT JOIN characteristic_measurement AS cmsex ON cmsex.event_id = e.id AND cmsex.characteristic_id = 40
     LEFT JOIN characteristic_measurement AS cmmoult ON cmmoult.event_id = e.id AND cmmoult.characteristic_id = 38
  `
  return fullExportQuery;
}

const getObjectMetadata = (objectPath) => {
  // -----------------------------------------------------------------------------   

  let s3 = new AWS.S3({ signatureVersion: 'v4' });

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: objectPath
  }

  return s3.headObject(params).promise();
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

  var userAssetsS3Bucket = process.env.USER_ASSETS_BUCKET;

  console.log(`DEBUG: user asset bucket name: ${userAssetsS3Bucket}`);

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

  // Event Batch
  var rawEventBatch = {};
  var eventBatch = {};

  // Creation Body
  var creationData = {};

  // Lookup Data
  var lookupData = {};

  // Search criteria
  let query = '';

  // Response Object
  var response = {};

  var count = 0;
  var moratoriumCount = 0;

  // Export mode
  let isAdvanced = (event.queryStringParameters && 'exportMode' in event.queryStringParameters
                    && event.queryStringParameters.exportMode
                    && event.queryStringParameters.exportMode === 'advanced') ? true : false;

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
      console.info('Querystring parameters OK. Validating payload...');
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      return BoilerPlate.validateJSON(payload, payloadSchema);
    })
    // Handle errors / Validate Claims
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('Event batch payload OK. Connecting to DB...');
      return validateExportRequest(event);
    })
    .then(res => {
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Connected to DB. Validating upload access...');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return BBHelpers.getUserProjects(db, claims);
    })
    .then(userProjectList => {
      return generateExportQuery(event, claims, governingCognitoGroup, userProjectList, isAdvanced);
    })
    .then(res => {
      query = res;
      return getRecordCount(db, event, query);
    })
    .then(res => {
      count = res[0].ro_search_events_count;
      return checkMoratoriumRecords(db, event, query)
    })
    .then(res => {
      moratoriumCount = res[0].ro_search_events_count;
      return createExportRequest(event, query, count, moratoriumCount > 0, isAdvanced);
    })
    .then(res => {
      console.log(db);
      return pushDownloadToDB(db, res);
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

  var userAssetsS3Bucket = process.env.USER_ASSETS_BUCKET;

  console.log(`DEBUG: user asset bucket name: ${userAssetsS3Bucket}`);

  // JSON Schemas
  var paramSchema = {};
  var payloadSchema = {};

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;  

  // Payload
  var payload = JSON.parse(event.body);

  // Response Object
  var response = {};  

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => { 
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
      return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version); })
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
    // Handle errors / marshall and Put
    .then(errors => {
      console.info('Payload structure OK (business). Connecting to DB...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Get the requested bander download record from the database
      return getBanderDownloadFromDB(db, event);
    })
    .then(res => {
      console.log(res);
      if (res.length > 0) {
        return validateExportRequest(event, res[0]);
      }
      throw new BoilerPlate.ParameterValidationError(`banderDownloadId does not exist!`, { message: `The banderDownloadId ${event.pathParameters.banderDownloadId} does not exist.`, path: `pathParameters.banderDownloadId`, value: event.pathParameters.banderDownloadId, schema: 'API_GATEWAY' });
    })
    .then(res => {
      console.info('Putting event');

      let updatedDownload = {
        ...res
      };
      updatedDownload.download_status = payload.download_status;
      
      console.log(updatedDownload);

      return updateDownloadToDB(db, updatedDownload);
    })
    .then(res => {
      // Commence download if permitted, otherwise update the status accordingly
      if (res.download_status === 'START') {
        return commenceExport(db, res, governingCognitoGroup);
      }
      return res;
    })
    .then(res => {
      response = res;

      // Commencing download, cleanup any uncommenced downloads for this bander
      return cleanInactiveDownloads(db, event);
    })
    .then(res => {

      console.log(response);

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
        "body": JSON.stringify(err),
        "isBase64Encoded": false
      });
    });
};

// GET PROJECTS FOR A GIVEN BANDER
module.exports.getDownloads = (event, context, cb) => {
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

  var userAssetsS3Bucket = process.env.USER_ASSETS_BUCKET;

  console.log(`DEBUG: user asset bucket name: ${userAssetsS3Bucket}`);

  // JSON Schemas
  var paramSchema = {};

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
    // Handle errors / Connect to DB
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('Querystring parameters OK. Validating bander for download...');
      return validateExportRequest(event);
    })
    .then(res => {
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Getting bander downloads...');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return listBanderDownloadsFromDB(db, event);
    })
    .then(res => {
      return convertAvailableDownloadsToPresignedUrls(res);
    })
    .then(res => {

      console.log('Final res: ', res);

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

// GET PROJECTS FOR A GIVEN BANDER
module.exports.generateDownload = (event, context, cb) => {
  // ----------------------------------------------------------------------------
  context.callbackWaitsForEmptyEventLoop = false;

  console.log(JSON.stringify(event));

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

  var userAssetsS3Bucket = process.env.USER_ASSETS_BUCKET;

  console.log(`DEBUG: user asset bucket name: ${userAssetsS3Bucket}`);

  // JSON Schemas
  var paramSchema = {};

  // Bander download payload
  let downloadPayload = JSON.parse(event.Records[0].body).bander_download;
  let governingCognitoGroup = downloadPayload.governingCognitoGroup;
  console.log(governingCognitoGroup);
  console.log(downloadPayload);
  let object_path = `bander_exports/${downloadPayload.bander_id}/${MomentTimezone().tz('NZ').format('DD-MM-YYYY')}_${MomentTimezone().tz('NZ').format('HH-mm-ss')}.csv`;

  let response = {};

  // Do the actual work
  return DBAccess.getDBConnection(db, dbCreationTimestamp)
    .then(dbAccess => {
      console.info('Generating download...');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;

      // Generate export and save to s3 using Aurora Postgres Export to S3 feature
      let fullExportQuery = getExportQuery(governingCognitoGroup, downloadPayload, downloadPayload.is_advanced);
      return db.rw_export_query(fullExportQuery, userAssetsS3Bucket, object_path);
    })
    .then(res => {
      return getObjectMetadata(object_path);
    })
    .then(res => {
      console.info('Putting download');

      let response = {
        ...downloadPayload
      };

      delete response.governingCognitoGroup;

      response.download_status = 'AVAILABLE_FOR_DOWNLOAD'
      response.object_path = object_path;
      response.object_version = res.VersionId;
      response.file_size_in_bytes = res.ContentLength;      
      console.log(response);

      return updateDownloadToDB(db, response);
    })
    .then(res => {
      return convertAvailableDownloadsToPresignedUrls([res]);
    })
    .then(res => {

      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(res[0]),
        "isBase64Encoded": false
      });
    })
    .catch(err => {
      console.info('Updating download to indicate failure...');

      let response = {
        ...downloadPayload
      };
      response.download_status = 'DOWNLOAD_FAILED'

      updateDownloadToDB(db, response);

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

