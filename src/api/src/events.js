'use strict';

// Modules
const Promise = require('bluebird');
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

const PAGINATION_MULTIPLIER = 100; // Could be reduced due to continuous pagination design change

const RESOURCE_NAME = "event";
const ALLOWABLE_SORT_ORDERS = BBHelpers.ALLOWABLE_EVENT_SORT_ORDERS;
const ALLOWABLE_FILTERS = BBHelpers.ALLOWABLE_EVENT_FILTERS;
const ALLOWABLE_SEARCHES = BBHelpers.ALLOWABLE_EVENT_SEARCHES;

// Imports - TODO REPLACE WITH LAMBDA LAYER
const validateUploadAccess = require('event-validation-lib').validateUploadAccess;
const validateUpdateAccess = require('event-validation-lib').validateUpdateAccess;
const validateGetAccess = require('event-validation-lib').validateGetAccess;
const validateDeleteAccess = require('event-validation-lib').validateDeleteAccess;
const validateEventExists = require('event-validation-lib').validateEventExists;
const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;
const getPostLookupData = require('event-validation-lib').getPostLookupData;
const getPutLookupData = require('event-validation-lib').getPutLookupData;
const supplementPostEventWithLookupData = require('event-validation-lib').supplementPostEventWithLookupData;
const supplementPutEventWithLookupData = require('event-validation-lib').supplementPutEventWithLookupData;
const validatePostBusinessRules = require('event-validation-lib').validatePostBusinessRules;
const validatePutBusinessRules = require('event-validation-lib').validatePutBusinessRules;

const generateUserProjectCriteria = (projectList, prefix = '') => {
  // ----------------------------------------------------------------------------   
  return projectList.map(projectId => {
    return {
      [`${prefix}project_id =`]: projectId
    }
  });
}


// The getDB method has been extracted from the searchDB method because of complications with pagination
// Pagination will require the separation of paginated entities (conveniently all 1:1 relations)
const getDB = (db, event) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getDB()");

  // Default pagination to the most recent records and submits empty criteria object
  let criteria = {};
  let idOperation = 'event.id =';
  criteria[idOperation] = event.pathParameters.eventId;

  // -----------------------------------
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


const getLatestEventsById = (db, eventIds) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getEventsById()");

  console.log(eventIds);

  // Default pagination to the most recent records and submits empty criteria object
  let criteria = {
    or: eventIds.map(eventId => { return { 'event.id =': eventId } })
  };

  if(criteria.or.length > 0) {
    
    // -----------------------------------
    console.log(criteria);

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
            .find(criteria, 
              { 
                order: [{ field: 'event.row_creation_idx', direction: 'asc' }],
              });
  }
  else {
    throw new BoilerPlate.ServiceError('No valid event_ids provided, cannot carry out search');
  }
}


const generateQueryStringFilterCriteria = (initialCriteria, multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringFilterCriteria()");

  let criteria = { ...initialCriteria };

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
        let innerCriteria = {
          'or': []
        };

        multiValueQueryStringParameters[filterQueryStringParameterName].map(filterQueryStringParameterValue => {
          // ---------------------------
          // If region name is submitted, convert to the code - special handling required
          if (filterQueryStringParameterName === 'regionName') {
            filterQueryStringParameterValue = BBSSHelpers.GetCodeFromRegion(filterQueryStringParameterValue);
            innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: `BBR${filterQueryStringParameterValue}`});
            innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: `GBR${filterQueryStringParameterValue}`});
          }
          else if (filterQueryStringParameterName === 'nznbbsCode') {
            // NZNBBS Code is an amalgamation of schema fields (i.e. it is not stored at-rest) - special handling required
            let nznbbsCodeCriteria = BBHelpers.convertNznbbsCodeToFilterCriteria(filterQueryStringParameterValue);
            if (nznbbsCodeCriteria) innerCriteria.or.push(nznbbsCodeCriteria);
          }
          else if (filterQueryStringParameterName === 'eventDate') {
            // Event timestamp filters need the New Zealand timezone applied to them
            innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: MomentTimezone.tz(filterQueryStringParameterValue, 'NZ').format() });
          }
          else if (filterQueryStringParameterName === 'eventDateGte') {
            // Event timestamp range filters need the New Zealand timezone applied to them as well as the beginning/end of the day
            innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: MomentTimezone.tz(`${filterQueryStringParameterValue} 00:00:00`, 'NZ').format() });
          }
          else if (filterQueryStringParameterName === 'eventDateLte') {
            // Event timestamp range filters need the New Zealand timezone applied to them as well as the beginning/end of the day
            innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: MomentTimezone.tz(`${filterQueryStringParameterValue} 23:59:59`, 'NZ').format() });
          }
          else {
            innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: filterQueryStringParameterValue});
          }
          return;
        });

        if (innerCriteria.or.length > 0) criteria.and.push(innerCriteria);
      }
    });
  }

  return criteria;
}


const processWildcardSearch = (paramName, paramValue) => {
  // ----------------------------------------------------------------------------
 //paramName should be 'prefixNumber' or 'shortNumber' see ln 82 bb_helpers.js ALLOWABLE_EVENT_SEARCHES

  paramValue = paramValue.toLowerCase();
  if ( paramValue.includes('*')) {
    // ------------------------------------------------
    paramValue = paramValue.split('*').join('%');
    return { [ALLOWABLE_SEARCHES[paramName].wildcard_filter_name]: paramValue };
  }
  else {
    // ------------------------------------------------
    return { [ALLOWABLE_SEARCHES[paramName].filter_name]: paramValue };
  }
}


const generateQueryStringSearchCriteria = (initialCriteria, multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringSearchCriteria()");

  let criteria = { ...initialCriteria };

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
          criteria.and.push(processWildcardSearch(searchQueryStringParameterName, searchQueryStringParameterValue));
        });
      }
    });
  }
  
  return criteria;
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
      { [`${prefix}default_moratorium_expiry <`]: MomentTimezone().tz('NZ').format() },
      { [`${prefix}default_moratorium_expiry IS`]: null },
    ]
  });

  // 4) ANY BIRDS WHOSE TIMELINE OF RECORDS THIS BANDER HAS CONTRIBUTED TO
  criteria.or.push({ [`${prefix}previous_bander =`]: claims.sub });
  
  return criteria;
}


const generatePaginationFilterCriteria = (type = 'lookahead', paginationToken, sortOrder) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generatePaginationFilterCriteria()");

  let criteria = null;
  
  switch (type) {
    case 'lookbehind': {
      if (paginationToken) {
        criteria = { and: [{'pagination_idx IS NOT': null}, { 'pagination_idx >' : 0 }] };
        switch(sortOrder) {
          case 'asc':
            // Lookbehind
            criteria.and.push({ 'pagination_idx <' : paginationToken });
            break;
          case 'desc':
            // Lookbehind
            criteria.and.push({ 'pagination_idx >' : paginationToken });
            break;
          default:
            break;
        }
      }
      return criteria;
    }
    case 'lookahead':
    default:
      criteria = { and: [{'pagination_idx IS NOT': null}, { 'pagination_idx >' : 0 }] };
      if (paginationToken) {
        switch(sortOrder) {
          case 'asc':
            // Lookahead
            criteria.and.push({ 'pagination_idx >' : paginationToken });
            break;
          case 'desc':
            // Lookahead
            criteria.and.push({ 'pagination_idx <' : paginationToken });
            break;
          default:
            break;
        }
      }
      return criteria;
  }
}


const searchDB = (db, event = null, claims, governingCognitoGroup, userProjectList) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".searchDB()");

  let pathParameters = event.pathParameters;
  let queryStringParameters = event.queryStringParameters;
  let multiValueQueryStringParameters = event.multiValueQueryStringParameters;

  let promises = [];

  // Initial pagination limit being set at 100
  let limit = (queryStringParameters && queryStringParameters.limit && parseInt(queryStringParameters.limit) <= 100) ? parseInt(queryStringParameters.limit) : 100;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
                             parseInt(queryStringParameters.paginationToken) : null;

  // ------------------------------------
  // QUERY STRING SORT COLUMN AND ORDER
  // ------------------------------------
  // Set standard materialized view name and sort order
  let materializedViewName = ALLOWABLE_SORT_ORDERS.eventTimestamp.materialized_view_name;
  let sortOrder = 'desc';

  let count = null;
  let maximumPaginationIdxCount = 0;
  let prev = null;


  // Check if a sort order has been requested
  if (queryStringParameters && 'sortBy' in queryStringParameters && Object.keys(ALLOWABLE_SORT_ORDERS).includes(queryStringParameters.sortBy)) {
    console.log(queryStringParameters.sortBy);
    materializedViewName = ALLOWABLE_SORT_ORDERS[queryStringParameters.sortBy].materialized_view_name;
  }

  // Check if a sort order has been requested
  if (queryStringParameters && 'order' in queryStringParameters && ['ASC', 'DESC'].includes(queryStringParameters.order)) {
    sortOrder = queryStringParameters.order.toLowerCase();
  }

  // -----------------------------------------------------
  // INITIALISE LOOKAHEAD AND LOOKBEHIND QUERY CRITERIA
  // -----------------------------------------------------
  let lookAheadSearchQueryCriteria = generatePaginationFilterCriteria('lookahead', paginationToken, sortOrder);
  let lookBehindSearchQueryCriteria = generatePaginationFilterCriteria('lookbehind', paginationToken, sortOrder);

  // -----------------------
  // USER ACCESS CONTROL
  // -----------------------
  // If a user is anything besides admin we need to manage their access to the banding records
  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    // ------------------------------------------------
    let userAccessControlCriteria = generateUserAccessControlFilterCriteria(userProjectList, claims);
    lookAheadSearchQueryCriteria.and.push(userAccessControlCriteria);
    if (lookBehindSearchQueryCriteria) lookBehindSearchQueryCriteria.and.push(userAccessControlCriteria);
  }

  // If a bird record is requested, add additional filter criteria
  if (typeof queryStringParameters !== 'undefined' && queryStringParameters && 'birdRecord' in queryStringParameters && queryStringParameters.birdRecord === 'true') {
    lookAheadSearchQueryCriteria.and.push({ 'bird_id IS NOT': null });
    if (lookBehindSearchQueryCriteria) lookBehindSearchQueryCriteria.and.push({ 'bird_id IS NOT': null });
  }

  // -----------------------
  // QUERY STRING SEARCHING
  // -----------------------
  lookAheadSearchQueryCriteria = generateQueryStringSearchCriteria(lookAheadSearchQueryCriteria, multiValueQueryStringParameters);
  if (lookBehindSearchQueryCriteria) lookBehindSearchQueryCriteria = generateQueryStringSearchCriteria(lookBehindSearchQueryCriteria, multiValueQueryStringParameters);
  
  // -----------------------
  // QUERY STRING FILTERING
  // -----------------------
  lookAheadSearchQueryCriteria = generateQueryStringFilterCriteria(lookAheadSearchQueryCriteria, multiValueQueryStringParameters);
  if (lookBehindSearchQueryCriteria) lookBehindSearchQueryCriteria = generateQueryStringFilterCriteria(lookBehindSearchQueryCriteria, multiValueQueryStringParameters);

  console.info('-------------- Completing combination of search and filtering with look ahead --------------');
  

  console.log(JSON.stringify(lookAheadSearchQueryCriteria));
  console.log(JSON.stringify(lookBehindSearchQueryCriteria));

  console.log(materializedViewName);

  // ---------------------------------------------------------
  // Complete standard search
  promises.push(
    db[materializedViewName]
    .find(lookAheadSearchQueryCriteria, 
      { 
        fields: ['id', 'pagination_idx'],
        order: [{ field: 'pagination_idx', direction: sortOrder }],
        limit: PAGINATION_MULTIPLIER * limit
    })
  );

  if (lookBehindSearchQueryCriteria) {
    promises.push(
      db[materializedViewName]
      .find(lookBehindSearchQueryCriteria, 
        { 
          fields: ['id', 'pagination_idx'],
          order: [{ field: 'pagination_idx', direction: (sortOrder === 'asc') ? 'desc' : 'asc' }],
          limit: PAGINATION_MULTIPLIER * limit
      })
    );
  }

  // Use a MATERIALIZED_VIEW for complex filtering (i.e. pre-join events to filter columns)
  // Also -> choose the corresponding view which relates to the required sort order to preserve pagination
  return Promise.all(promises)
  .then(res => {
    // Default pagination to the most recent records and submits empty criteria object
    let searchAndFilterCriteria = {};
    let idOperation;

    // Pagination on a join is slightly trickier (cannot use limit due to amalgamation of resultset)
    idOperation = `pagination_idx ${(sortOrder === 'asc') ? '>' : '<' }`;
    searchAndFilterCriteria.and = [ { [idOperation]: paginationToken} ];

    // If there is a preliminary look ahead query result, we need to factor this into our query criteria
    if (res[0].length > 0) {
      // ------------------------------------

      let uniqueEventIds = new Set();
      let lookBehindPaginationTokens = new Set();

      let criteria = {
        or: []
      };
      let idOperation = `${materializedViewName}.id =`;
      criteria.or = [];

      let currentPaginationIdx = 0;
      let currentPaginationIdxCount = 0;

      res[0].forEach((resItem, idx) => {
        // Register how many duplicate pagination tokens come back
        if (resItem.pagination_idx !== currentPaginationIdx) {
          // Change in pagination idx means a new event_id
          // Update maximumPaginationIdxCount if required
          maximumPaginationIdxCount = (maximumPaginationIdxCount < currentPaginationIdxCount) ? currentPaginationIdxCount : maximumPaginationIdxCount;
          currentPaginationIdx = resItem.pagination_idx;
          currentPaginationIdxCount = 1;
        }
        else {
          currentPaginationIdxCount += 1;
        }

        // Add criteria to the final join if we are looking ahead of the paginationToken
        uniqueEventIds.add(resItem.id);
      });

      [...uniqueEventIds].map((eventId, idx) => {
        if (idx < limit) criteria.or.push({ [idOperation]: eventId });
      });
      
      
      if (res.length > 1 && res[1].length > 0) {
        res[1].forEach((resItem, idx) => {
          // Register lookBehind pagination_idx for use as the previous reference
          lookBehindPaginationTokens.add(resItem.pagination_idx);
        });
        let lookBehindIndex = (lookBehindPaginationTokens.size > limit) ? (limit - 1) : null;
        prev = (lookBehindIndex) ? [...lookBehindPaginationTokens][lookBehindIndex] : null;
      }

      console.log('Current pagination idx: ', currentPaginationIdx);
      console.log('Max pagination idx count: ', maximumPaginationIdxCount);

      count = uniqueEventIds.size;

      console.log('Calculated prev: ', prev);

      console.log(uniqueEventIds.size);
      console.log('Criteria: ', criteria);

      let joinDefinition = {
        pk: 'id',
        event: {
          type: 'INNER',
          pk: 'id',
          on: { 'id': `${materializedViewName}.id` },
          decomposeTo: 'object',
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
            on: { 'event_id': 'event.id' },
            mark_state: {
              type: 'LEFT OUTER',
              pk: 'id',
              on: { 'event_id': 'event.id', 'mark_id': 'mark_configuration.mark_id' },
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
              on: { 'event_id': 'event.id', 'mark_id': 'mark_configuration.mark_id' }
            }
          }
        },
      };

      return db[materializedViewName]
      .join(joinDefinition)
      .find(criteria, 
        { 
          order: [{ field: 'pagination_idx', direction: sortOrder }],
        })
        .then(res => {
          // Handle structural change to join
          res = res.map(obj => {
            let formattedObj = obj;

            formattedObj.event.mark_state = [];
            formattedObj.event.mark_allocation = [];
            formattedObj.event.mark_configuration.map(markConfig => {
              // ----------------------------------------
              if (markConfig.mark_state.length > 0) {
                formattedObj.event.mark_state = [...formattedObj.event.mark_state, ...markConfig.mark_state];
              }
              if (markConfig.mark_allocation.length > 0) {
                formattedObj.event.mark_allocation = [...formattedObj.event.mark_allocation, ...markConfig.mark_allocation];
              }


              delete markConfig.mark_state;
              delete markConfig.mark_allocation;
            });

            return formattedObj;
          });
          
          return {
            data: res,
            count: count,
            countFromPageToEnd: count < (PAGINATION_MULTIPLIER/maximumPaginationIdxCount),
            sortOrder: sortOrder,
            isLastPage: (count < (PAGINATION_MULTIPLIER/maximumPaginationIdxCount) && count <= res.length) || res.length < limit,
            prev: prev
          }
        })
        .catch(error => {
          throw new Error(error);
        });
      }
      else {

        console.info('Returning from 1:N joins. No results from combination of search and filter criteria!')
        return {
          data: [],
          count: 0,
          countFromPageToEnd: true,
          sortOrder: sortOrder,
          isLastPage: true,
          prev: null
        };
      }
    });
}


const addCalculatedFields = (method = 'search', dbResult) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".addCalculatedFields()");

  console.log(dbResult);

  let res = dbResult.map(dbResultItem => {
    let event = (method === 'search') ? dbResultItem.event : dbResultItem;
    event = { ...event, pagination_idx: (method === 'search') ? dbResultItem.pagination_idx : dbResultItem.row_creation_idx };

    switch(event.event_type) {
      case 'FIRST_MARKING_IN_HAND':
      case 'SIGHTING_BY_PERSON':
      case 'IN_HAND':
      case 'RECORDED_BY_TECHNOLOGY':
      case 'IN_HAND_PRE_CHANGE':
      case 'IN_HAND_POST_CHANGE':
        // ----------------------------
        // If this is a bird event, add some calculated fields including: colour_bands (bool, true if colour bands present) and nznbbs_code
        return {
          ...event,
          calculated_fields: {
            colour_bands: (event.mark_configuration.filter(markConfig => typeof markConfig.colour !== 'undefined' && markConfig.colour).length > 0),
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
        if (response.length > 0) {
          let formattedResponse = response[0];
          console.log(formattedResponse);
          formattedResponse.mark_configuration.sort((a, b) => {
            if (
                  (Number.isInteger(a.location_idx) && Number.isInteger(b.location_idx) 
                      && a.location_idx > b.location_idx)
                  || (!Number.isInteger(a.location_idx) && Number.isInteger(b.location_idx))
                ) {
              return 1;
            }
            else if (
                        (Number.isInteger(a.location_idx) && Number.isInteger(b.location_idx)
                         && a.location_idx < b.location_idx)
                        || (!Number.isInteger(b.location_idx) && Number.isInteger(a.location_idx))
                    ) {
              return -1;
            }
            else {
              return 0;
            }
          });

          return formattedResponse;
        }
        return {};
      }
      case 'search':
      case 'post':
      case 'put':
      default: {
        return response;
      }
    }
}


const formatPostAndPutResponse = (response, refreshFromDb = false) => {
  // --------------------------------------------------------
  console.info('Formatting HTTP POST/PUT response');
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

const getDistinctMarkPrefixesFromEvents = (payload) => {
  // --------------------------------------------------------
  console.info('Getting distinct mark prefixes from event batch');

  let distinctPrefixes = new Set([]);

  payload.map(record => {
    // Process each event's mark configuration to pick out tracked stock
    // Add any new prefixes to a growing set of distinct prefixes involved
    record.mark_configuration
      .filter(markConfig => markConfig.mark_id)
      .map(markConfig => {
        // ---------------------------
        distinctPrefixes.add(markConfig.alphanumeric_text.split('-')[0].toLowerCase());
      });
  });

  return Array.from(distinctPrefixes);
}


const generatePostDbPayload = async (db, event, payload) => {
  // --------------------------------------------------------
  console.info('Generating post DB Payload');

  let responseData = [];

  let rawData = payload.data;

  let projectId = event.pathParameters.projectId;

  // Get the project_coordinator (a.k.a project manager) and assign as event_owner
  let event_owner_id = (await db.ro_get_project_coordinator(projectId))[0].ro_get_project_coordinator;

  rawData.map(record => {
    // Process each individual record - split where marks change (i.e. there is a mark_configuration_capture)
    let recordRelease = { event: null, bird: null, characteristic_measurement: [], mark_configuration: [], mark_state: [] };
    let recordCapture = null;

    // BIRD - generate ID where not already available
    recordRelease.bird = (!record.bird.id) ? {
      id: uuidv4(),
      species_id: record.bird.species_id,
      friendly_name: record.mark_configuration_release.reduce((accumulator, current) => {
        if (accumulator) {
          return accumulator;
        }
        else if (current.alphanumeric_text) {
          return current.alphanumeric_text;
        }
        else {
          return null;
        }
      }, null),
      new: true
    } : {
      ...record.bird,
      new: false
    }

    // EVENT - generate ID and link to bird ID
    // Supplement the event with the event_owner_id (i.e. project coordinator)
    recordRelease.event = {
      ...record.event,
      id: uuidv4(),
      bird_id: recordRelease.bird.id,
      event_owner_id: event_owner_id
    };

    // If a split event, handle the recordCapture as well
    if (record.mark_configuration_capture.length > 0) {
      recordCapture = _.cloneDeep(recordRelease);
      recordCapture.event.id = uuidv4();
      recordRelease.event.event_timestamp = Moment(record.event.event_timestamp).add(1, 'seconds');
      recordCapture.event.event_type = 'IN_HAND_PRE_CHANGE';
      recordRelease.event.event_type = 'IN_HAND_POST_CHANGE';
      recordRelease.bird.new = false; // NEED THIS TO ENSURE WE DON'T TRY TO ADD A NEW RECORD TWICE
      
      // CHARACTERISTIC_MEASUREMENT
      recordCapture.characteristic_measurement = record.characteristic_measurement.map(charMeasure => {
        // Supplement characteristic_measurements with event_ids
        return {
          ...charMeasure,
          event_id: recordCapture.event.id
        }
      });
      // Need to also add out_status_code if not included
      if (recordCapture.characteristic_measurement.filter(charMeasure => charMeasure.characteristic_id === 43).length <= 0) {
        recordCapture.characteristic_measurement.push({
          characteristic_id: 43,
          value: 'UNKNOWN',
          event_id: recordCapture.event.id
        });
      }


      // MARK CONFIGURATION CAPTURE
      recordCapture.mark_configuration = record.mark_configuration_capture.map(markConfig => {
        return {
          ...markConfig,
          event_id: recordCapture.event.id
        }
      });
    }

    // CHARACTERISTIC_MEASUREMENT (only included on release if not recorded for capture already)
    recordRelease.characteristic_measurement = record.characteristic_measurement.map(charMeasure => {
      // Supplement characteristic_measurements with event_ids
      return {
        ...charMeasure,
        event_id: recordRelease.event.id
      }
    });

    if (recordRelease.characteristic_measurement.filter(charMeasure => charMeasure.characteristic_id === 43).length <= 0) {
      recordRelease.characteristic_measurement.push({
        characteristic_id: 43,
        value: 'UNKNOWN',
        event_id: recordRelease.event.id
      });
    }

    // MARK CONFIGURATION RELEASE
    recordRelease.mark_configuration = record.mark_configuration_release.map(markConfig => {
      return {
        ...markConfig,
        event_id: recordRelease.event.id
      }
    });

    // MARK_STATE
    // -> If this is a split record we need to be a bit smarter about the mark_state
    if (recordCapture) {
      // -----------------------------
      let mark_ids_at_capture = [];
      let mark_ids_at_release = [];
      // Any marks explicitly stated are 'ATTACHED'
      recordCapture.mark_state = recordCapture.mark_configuration
                      .filter(markConfig => markConfig.mark_id)
                      .map(markConfig => { 
                        mark_ids_at_capture.push(markConfig.mark_id); 
                        return { 
                          event_id: recordCapture.event.id,
                          mark_id: markConfig.mark_id, 
                          state: 'ATTACHED', 
                          state_idx: 0, 
                          is_current: false 
                        };
                      });
      
      recordRelease.mark_state = recordRelease.mark_configuration
                      .filter(markConfig => markConfig.mark_id)
                      .map(markConfig => { 
                        mark_ids_at_release.push(markConfig.mark_id); 
                        return { 
                          event_id: recordRelease.event.id,
                          mark_id: markConfig.mark_id, 
                          state: 'ATTACHED', 
                          state_idx: 0, 
                          is_current: false 
                        }; 
                      });
    
      // Any marks present at capture but not at release are 'DETACHED'
      mark_ids_at_capture.filter(captureMarkId => !mark_ids_at_release.includes(captureMarkId))
                         .map(markId => { 
                           recordRelease.mark_state.push({ 
                             event_id: recordRelease.event.id,
                             mark_id: markId, 
                             state: 'DETACHED', 
                             state_idx: 0, 
                             is_current: false 
                            }); 
                          });
    }
    else {
      // -----------------------------
      recordRelease.mark_state = recordRelease.mark_configuration
                      .filter(markConfig => markConfig.mark_id)
                      .map(markConfig => { 
                        return { 
                          event_id: recordRelease.event.id,
                          mark_id: markConfig.mark_id, 
                          state: 'ATTACHED', 
                          state_idx: 0, 
                          is_current: false 
                        };
                      });
    }

    responseData.push(recordRelease);
    (recordCapture) && responseData.push(recordCapture);
  });

  console.log(JSON.stringify(responseData));
  return responseData;
}


const generatePutDbPayload = async (db, payload, current, warnings) => {
  // --------------------------------------------------------
  console.info('Generating put DB Payload');

  let responseData = [];

  let rawData = payload.data;

  let projectId = rawData[0].event.project_id;

  // Get the project_coordinator (a.k.a project manager) and assign as event_owner
  let event_owner_id = (await db.ro_get_project_coordinator(projectId))[0].ro_get_project_coordinator;

  rawData.map(record => {
    // Process each individual record - split where marks change (i.e. there is a mark_configuration_capture)
    let recordChange = { event: null, bird: null, characteristic_measurement: [], mark_configuration: [], mark_state: [] };

    console.log(record.bird.id);
    console.log(current[0].bird.id);
    console.log(warnings.filter(error => error.code === 4004).length > 0);    

    // BIRD - generate ID where not already available
    recordChange.bird = (!record.bird.id) ? {
      id: uuidv4(),
      species_id: record.bird.species_id,
      friendly_name: (record.bird.friendly_name) ? record.bird.friendly_name 
                          : record.mark_configuration.reduce((accumulator, current) => {
                              if (current.mark_id) {
                                return current.alphanumeric_text;
                              }
                              else if (accumulator) {
                                return accumulator;
                              }
                              else if (current.alphanumeric_text) {
                                return current.alphanumeric_text;
                              }
                              else {
                                return null;
                              }
                            }, null),
      change: true
    } : {
      id: record.bird.id,
      species_id: record.bird.species_id,
      friendly_name: (record.bird.friendly_name) ? record.bird.friendly_name 
                          : record.mark_configuration.reduce((accumulator, current) => {
                              if (current.mark_id) {
                                return current.alphanumeric_text;
                              }
                              else if (accumulator) {
                                return accumulator;
                              }
                              else if (current.alphanumeric_text) {
                                return current.alphanumeric_text;
                              }
                              else {
                                return null;
                              }
                            }, null),
      change: (( record.bird.id === current[0].bird.id 
                && (record.bird.friendly_name !== current[0].bird.friendly_name 
                || record.bird.species_id !== current[0].bird.species_id)) 
                || warnings.filter(error => error.code === 4004).length > 0)
    }

    // EVENT - generate ID and link to bird ID
    // Supplement the event with the event_owner_id (i.e. project coordinator)
    recordChange.event = {
      ...record.event,
      bird_id: recordChange.bird.id,
      event_owner_id: event_owner_id
    };

    // NO SPLIT RECORDS FOR UPDATES TO NO NEED TO HANDLE CAPTURE/RELEASE HERE

    // CHARACTERISTIC_MEASUREMENT (only included on release if not recorded for capture already)
    recordChange.characteristic_measurement = record.characteristic_measurement.map(charMeasure => {
      // Supplement characteristic_measurements with event_ids
      return {
        ...charMeasure,
        event_id: recordChange.event.id
      }
    });
    // Add out_status_code if not provided/removed
    if (recordChange.characteristic_measurement.filter(charMeasure => charMeasure.characteristic_id === 43).length <= 0) {
      recordChange.characteristic_measurement.push({
        characteristic_id: 43,
        value: 'UNKNOWN',
        event_id: recordChange.event.id
      });
    }

    // MARK CONFIGURATION RELEASE
    recordChange.mark_configuration = record.mark_configuration.map(markConfig => {
      return {
        ...markConfig,
        event_id: recordChange.event.id
      }
    });

    // MARK_STATE
    // -> If this is a split record we need to be a bit smarter about the mark_state
    recordChange.mark_state = record.mark_configuration
                    .filter(markConfig => markConfig.mark_id)
                    .map(markConfig => { 
                      return { 
                        event_id: record.event.id,
                        mark_id: markConfig.mark_id, 
                        state: 'ATTACHED', 
                        state_idx: 0, 
                        is_current: false 
                      };
                    });
    // -> If event is a POST_CHANGE event and 'current'/pre-edit event has mark_state indicating DETACHED states,
    //    also include these in order to preserve the mark_state (we will do a full refresh of mark_state/mark_configuration/characteristic_measurement)
    recordChange.mark_state = [...recordChange.mark_state, ...(current[0].mark_state.filter(markState => markState.state === 'DETACHED' )
      .map(markState => { return {
        event_id: recordChange.event.id,
        mark_id: markState.mark_id,
        state: 'DETACHED', 
        state_idx: 0, 
        is_current: false 
      } }))];

    responseData.push(recordChange);
  });

  console.log(JSON.stringify(responseData));
  return responseData;
}


const postEventToDB = async(db, payload) => {
  // --------------------------------------------------------
  console.info('Posting record to DB');

  console.log(payload);

  return db.withTransaction(async tx => {
    // -----------------------------------

    // ----------------------------------
    // 1) INSERT THE NEW DATA
    // ----------------------------------
    // BIRD
    let birds = await tx.bird.insert(payload.filter(record => record.bird.new).map(record => { 
      if (record.bird.friendly_name) {
        return { id: record.bird.id, species_id: record.bird.species_id, friendly_name: record.bird.friendly_name }; 
      }
      return { id: record.bird.id, species_id: record.bird.species_id }; 
    }));
    console.log(`BIRDS: ${JSON.stringify(birds)}`);

    // EVENT
    let events = await tx.event.insert(payload.map(record => record.event));
    console.log(`EVENTS: ${JSON.stringify(events)}`);

    // CHARACTERISTIC_MEASUREMENTS
    let characteristic_measurements = (payload.reduce((accumulator, current) => [...accumulator, ...current.characteristic_measurement], []).length > 0) 
            && await tx.characteristic_measurement.insert(payload.reduce((accumulator, current) => [...accumulator, ...current.characteristic_measurement], []));
    console.log(`CHAR MEAS: ${JSON.stringify(characteristic_measurements)}`);

    // MARK_CONFIGURATION
    let mark_configurations = await tx.mark_configuration.insert(payload.reduce((accumulator, current) => [...accumulator, ...current.mark_configuration], []))
    console.log(`MARK CONFIGS: ${JSON.stringify(mark_configurations)}`);
    
    // MARK_STATE
    let mark_states = null;
    let mark_states_to_add = payload.reduce((accumulator, current) => [...accumulator, ...current.mark_state], []);
    let mark_states_to_update = mark_states_to_add.map(markState => markState.mark_id);

    // If any stock statuses to update...
    if (mark_states_to_add.length > 0) {
      // 1) Insert new mark_state values
      mark_states = await tx.mark_state.insert(mark_states_to_add);
      console.log(`MARK STATES: ${JSON.stringify(mark_states)}`);

      // 2) Reset all marks to is_current = false
      let reset_current_mark_states = await tx.mark_state.update(
        { 
          'or': mark_states_to_add
                        .map(markState => { return { 'mark_id =': markState.mark_id } })
        },
        {
          'is_current': false
        });
        console.log(`RESET MARK STATES: ${JSON.stringify(reset_current_mark_states)}`);

      // 3) Run full update of state_idx and is_current for all marks involved
      let current_mark_states = await tx.rw_update_latest_mark_state([mark_states_to_update]);
      console.log(`CURRENT MARK STATES: ${JSON.stringify(current_mark_states)}`);
    }

    // If required, update all stock tallies for banders/marks affected
    let prefixes_to_update = getDistinctMarkPrefixesFromEvents(payload);
    console.log(`prefixes to update: ${prefixes_to_update}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixes_to_update.length > 0) {
      // -----------------------------------
      tx.rw_update_bander_stock_by_prefix([ process.env.BANDING_OFFICE_ID, prefixes_to_update ]);
      tx.rw_update_banding_office_stock([ [process.env.BANDING_OFFICE_ID], prefixes_to_update ]);
    }

    return {
      birds: birds,
      events: events,
      characteristic_measurements: characteristic_measurements,
      mark_configurations: mark_configurations,
      mark_states: mark_states
    }
  });
}


const putEventToDB = (db, payload, event) => {
  // ----------------------------------------------------------------------------   
  console.info('Putting record to DB');

  console.log(payload);

  let originalEventId = event.pathParameters.eventId;

  return db.withTransaction(async tx => {
    // -----------------------------------

    // ------------------------------------------------------------------------------------------------------
    // 1) CLEAR THE PREVIOUS CHARACTERISTIC_MEASUREMENTS MARK_CONFIGURATION AND MARK_STATE
    // ------------------------------------------------------------------------------------------------------
    let charMeasureClearout = await tx.characteristic_measurement.destroy({
      'event_id =': originalEventId
    });
    let markConfigClearout = await tx.mark_configuration.destroy({
      'event_id =': originalEventId
    });
    let markStateClearout = await tx.mark_state.destroy({
      'event_id =': originalEventId
    });
    console.log(JSON.stringify(markConfigClearout));

    let markIdRemovals = markStateClearout.map(markState => markState.mark_id);

    // ----------------------------------
    // 2) UPDATE THE BIRD AND EVENT DATA
    // ----------------------------------
    let bird = null;
    if (payload[0].bird.change) {
      // If detail about the bird has changed, we need to update this in the bird table
       bird = await tx.bird.save({
        id: payload[0].bird.id,
        species_id: payload[0].bird.species_id,
        friendly_name: payload[0].bird.friendly_name
      });
      console.log(`BIRD: ${JSON.stringify(bird)}`);
    }

    // EVENT
    let event = await tx.event.save(payload[0].event);
    console.log(`EVENT: ${JSON.stringify(event)}`);
    
    
    // ------------------------------------------------
    // 3) INSERT THE NEW DATA ASSOCIATED TO THE EVENT
    // ------------------------------------------------
    // CHARACTERISTIC_MEASUREMENTS
    let characteristic_measurements = (payload.reduce((accumulator, current) => [...accumulator, ...current.characteristic_measurement], []).length > 0) 
            && await tx.characteristic_measurement.insert(payload.reduce((accumulator, current) => [...accumulator, ...current.characteristic_measurement], []));
    console.log(`CHAR MEAS: ${JSON.stringify(characteristic_measurements)}`);

    // MARK_CONFIGURATION
    let mark_configurations = await tx.mark_configuration.insert(payload.reduce((accumulator, current) => [...accumulator, ...current.mark_configuration], []))
    console.log(`MARK CONFIGS: ${JSON.stringify(mark_configurations)}`);
    
    // MARK_STATE
    let mark_states = null;
    let mark_states_to_add = payload.reduce((accumulator, current) => [...accumulator, ...current.mark_state], []);
    let mark_ids_for_state_update = [...(mark_states_to_add.map(markState => markState.mark_id)), ...markIdRemovals];
    // If any stock statuses to add...
    if (mark_states_to_add.length > 0) {
      // ------------------------------------------
      // 1) Insert new mark_state values
      mark_states = await tx.mark_state.insert(mark_states_to_add);
      console.log(`MARK STATES: ${JSON.stringify(mark_states)}`);

      // 2) Reset all marks to is_current = false
      let reset_current_mark_states = await tx.mark_state.update(
        { 
          'or': mark_states_to_add.map(markState => { return { 'mark_id =': markState.mark_id } })
        },
        {
          'is_current': false
        });
        console.log(`RESET MARK STATES: ${JSON.stringify(reset_current_mark_states)}`);
    }

    // If any stock mark_states to update...
    if (mark_ids_for_state_update.length > 0) {
      // ------------------------------------------
      // 3) Run full update of state_idx and is_current for all marks involved
      console.log(`MARK IDS FOR STATE UPDATE: ${JSON.stringify(mark_ids_for_state_update)}`);
      let current_mark_states = await tx.rw_update_latest_mark_state([mark_ids_for_state_update]);
      console.log(`CURRENT MARK STATES: ${JSON.stringify(current_mark_states)}`);
    }

    // If required, update all stock tallies for banders/marks affected
    let prefixPayload = [...payload, { mark_configuration: markConfigClearout }];
    console.log(`Prefix payload: ${prefixPayload}`);
    let prefixes_to_update = getDistinctMarkPrefixesFromEvents(prefixPayload);
    console.log(`prefixes to update: ${prefixes_to_update}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixes_to_update.length > 0) {
      // -----------------------------------
      tx.rw_update_bander_stock_by_prefix([ process.env.BANDING_OFFICE_ID, prefixes_to_update ]);
      tx.rw_update_banding_office_stock([ [process.env.BANDING_OFFICE_ID], prefixes_to_update ]);
    }

    return {
      birds: bird,
      events: [event],
      characteristic_measurements: characteristic_measurements,
      mark_configurations: mark_configurations,
      mark_states: mark_states
    }
  });
}


const validateDeleteBusinessRules = (customErrorFactory, db, event) => {
  // ----------------------------------------------------------------------------   
  console.info('.validateDeleteBusinessRules()');

  return [];
}


const deleteEventFromDB = (db, event, currentEvent) => {
  // ----------------------------------------------------------------------------   
  console.info('.deleteEventFromDB()');

  let eventId = event.pathParameters.eventId;
  let birdId = currentEvent[0].bird.id;

  console.log(JSON.stringify(currentEvent));

  console.log(birdId);

  return db.withTransaction(async tx => {
    // -----------------------------------

    // Get the distinct prefix numbers for later use
    let distinctPrefixNumbersResultset = await tx.vw_distinct_prefix_numbers_by_event.find({ 'event_id =': eventId});

    // Setup the default record actions for persisting to support updating the modern data platform
    let recordActionParams = [
      {
        db_action: 'DELETE',
        db_table: 'characteristic_measurement',
        db_table_identifier_name: 'event_id',
        db_table_identifier_value: eventId
      },
      {
        db_action: 'DELETE',
        db_table: 'mark_configuration',
        db_table_identifier_name: 'event_id',
        db_table_identifier_value: eventId
      },
      {
        db_action: 'DELETE',
        db_table: 'mark_state',
        db_table_identifier_name: 'event_id',
        db_table_identifier_value: eventId
      },
      {
        db_action: 'DELETE',
        db_table: 'event',
        db_table_identifier_name: 'id',
        db_table_identifier_value: eventId
      }     
    ];

    // ------------------------------------------------------------------------------------------------------
    // 1) CLEAR THE EVENTS CHARACTERISTIC_MEASUREMENTS MARK_CONFIGURATION AND MARK_STATE
    // ------------------------------------------------------------------------------------------------------
    let charMeasureClearout = await tx.characteristic_measurement.destroy({
      'event_id =': eventId
    });
    let markConfigClearout = await tx.mark_configuration.destroy({
      'event_id =': eventId
    });
    let markStateClearout = await tx.mark_state.destroy({
      'event_id =': eventId
    });

    let markIdRemovals = markStateClearout.map(markState => markState.mark_id);

    console.log(`Updating current mark_state for the following marks: ${JSON.stringify(markIdRemovals)}`);    

    // ----------------------------------
    // 2) CLEAR THE EVENT ITSELF
    // ----------------------------------
    let eventClearout = await tx.event.destroy({
      'id =': eventId
    });

    // ------------------------------------------------------------------------------------------------------
    // 3) CHECK WHETHER THE BIRD HAS ANY OTHER ASSOCIATED EVENTS AND IF NOT, DELETE THE BIRD AS WELL
    // ------------------------------------------------------------------------------------------------------
    let birdCount = await tx.ro_bird_event_count(birdId);
    let birdClearout = null;

    if(typeof birdId !== 'undefined' && birdId && birdCount.ro_bird_event_count === 0) {
      birdClearout = await tx.bird.destroy({
        'id =': birdId
      });
      recordActionParams.push({
        db_action: 'DELETE',
        db_table: 'bird',
        db_table_identifier_name: 'id',
        db_table_identifier_value: birdId
      }  )
    }

    // ------------------------------------------------------------------------------------------------------
    // 4) Reset the mark_state for any marks whose timelines are alterred as a result of the deletion
    // ------------------------------------------------------------------------------------------------------
    let current_mark_states = await tx.rw_update_latest_mark_state([markIdRemovals]);
    console.log(`CURRENT MARK STATES: ${JSON.stringify(current_mark_states)}`);

    // ------------------------------------------------------------------------------------------------------
    // 5) Update the mark state rollup (to port to bird event delete as well)
    // ------------------------------------------------------------------------------------------------------
    // If required, update all stock tallies for banders/marks affected
    
    let prefixes_to_update = (typeof distinctPrefixNumbersResultset[0]?.prefix_numbers !== 'undefined') ? distinctPrefixNumbersResultset[0]?.prefix_numbers.split(',') : [];
    console.log(`prefixes to update: ${prefixes_to_update}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixes_to_update.length > 0) {
      // -----------------------------------
      let banderStockUpdate = await tx.rw_update_bander_stock_by_prefix([ process.env.BANDING_OFFICE_ID, prefixes_to_update ]);
      let bandingOfficeStockUpdate = await tx.rw_update_banding_office_stock([ [process.env.BANDING_OFFICE_ID], prefixes_to_update ]);
      console.log(`[INFO] STOCK UPDATES: ${JSON.stringify(banderStockUpdate)} ${JSON.stringify(bandingOfficeStockUpdate)}`);
    }

    let recordActionUpdate = await tx.record_action.insert(recordActionParams);

    return {
      birds: birdClearout,
      events: eventClearout,
      characteristic_measurements: charMeasureClearout,
      mark_configurations: markConfigClearout,
      mark_states: markStateClearout,
      record_actions: recordActionUpdate
    }
  });
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
      console.info('Querystring parameters OK. Connecting to DB...');
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Getting event');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Validate bander is not suspended
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
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
      return addCalculatedFields('get', res); 
    })
    .then(res => { 
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

  var customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
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
      return searchDB(db, event, claims, governingCognitoGroup, userProjectList); })
    .then(res => { 
      count = res.count;
      countFromPageToEnd = res.countFromPageToEnd;
      sortOrder = res.sortOrder;
      isLastPage = res.isLastPage;
      prev = res.prev;
      return addCalculatedFields('search', res.data); 
    })
    .then(res => { return formatResponse('search', res); })
    .then(res => { 
      // -------------------
      let params = {
        data: res, path: event.path,
        queryStringParameters: event.queryStringParameters,
        multiValueQueryStringParameters: event.multiValueQueryStringParameters,
        paginationPointerArray: ['event', 'pagination_idx'],
        maxLimit: 100, order: sortOrder,
        count: count, countType: countType,
        countFromPageToEnd: countFromPageToEnd,
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
      // Migrate to bb helpers
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
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
      response = formatPostAndPutResponse(response);
      response.errors = [...response.errors, ...errors];
      if (errors.filter(error => error.severity === 'CRITICAL').length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload business validation error(s)!`, errors);
      }
      return generatePutDbPayload(db, response, lookupData.currentEvent, response.errors.filter(error => error.severity === 'WARNING'));
    })
    .then(res => {
      response.data = res;
      return putEventToDB(db, res, event);
    })
    .then(res => {
      // Complete a fresh pull of the new events from the database
      console.log(JSON.stringify(res));
      return getLatestEventsById(db, res.events.map(singleEvent => singleEvent.id));
    })
    .then(res => { 
      return addCalculatedFields('get', res); 
    })
    .then(res => { 
      return formatResponse('put', res); 
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
      // Migrate to bb helpers
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
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
      return getPostLookupData(db, validations, response.data);
    })
    .then(res => {
      lookupData = res;
      response.data = supplementPostEventWithLookupData(validations, response.data, lookupData);
      return validatePostBusinessRules(customErrorFactory, claims, governingCognitoGroup, validations, response.data);
    })
    .then(errors => {
      response = formatPostAndPutResponse(response);
      response.errors = [...response.errors, ...errors];
      if (errors.filter(error => error.severity === 'CRITICAL').length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload business validation error(s)!`, errors);
      }
      return generatePostDbPayload(db, event, response);
    })
    .then(res => {
      response.data = res;
      return postEventToDB(db, res);
    })
    .then(res => {
      // Complete a fresh pull of the new events from the database
      console.log(JSON.stringify(res));
      return getLatestEventsById(db, res.events.map(singleEvent => singleEvent.id));
    })
    .then(res => { 
      return addCalculatedFields('get', res); 
    })
    .then(res => { 
      return formatResponse('post', res); 
    }) 
    .then(res => {

      response.data = res;

      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 201,
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

// DELETE
module.exports.delete = (event, context, cb) => {
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
  console.info(event);

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
  var payloadSchema = {};

  // Payload
  let payload = JSON.parse(event.body);

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  let currentEvent = null;

  // Do the actual work
  BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => {
      // Store highest claimed group for reference further on in the function
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
      console.info('QueryString parameters OK. Connecting to DB to validate business rules...');
      // Get Custom Errors schema from Dynamo   ########## MODIFY OR delete ################
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Migrate to bb helpers
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
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
      console.info('Validating Deletion business rules...');
      currentEvent = res;
      return validateDeleteAccess(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ForbiddenError(`${errors.length} Authorisation validation error(s)!`, errors);
      }
      return validateDeleteBusinessRules(customErrorFactory, db, event);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Business validation error(s)!`, errors);
      }
      return deleteEventFromDB(db, event, currentEvent);
    })
    .then(res => {
      console.log(JSON.stringify(res));

      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 204,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(res.events),
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
