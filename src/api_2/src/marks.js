'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
var _ = require('lodash');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const Helpers = require('helpers');
const BBHelpers = require('bb-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;

const USER_MARK_STATE_ACTIONS = require('bb-helpers').USER_MARK_STATE_ACTIONS;

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "marks";
const ALLOWABLE_FILTERS = {
  'markState': {
    'filter_name': 'state ='
  },
  'shortNumberFrom': {
    'filter_name': 'short_number_numeric >='
  },
  'banderId': {
    'filter_name': 'bander_id ='
  },
  'shortNumberTo': {
    'filter_name': 'short_number_numeric <='
  }
};
const ALLOWABLE_SEARCHES = {
  'prefixNumber': {
    'filter_name': 'prefix_number =',
    'wildcard_filter_name': 'prefix_number LIKE'
  },
  'shortNumber': {
    'filter_name': 'short_number =',
    'wildcard_filter_name': 'short_number LIKE'
  }
};


const getDB = async (db, event, claims, governingCognitoGroup) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getDB()");

  // Initial query of distinct event ids related to mark 
  // -> This will be used as a filter criteria to the main query
  let initialCriteria = {
    'mark_id =': event.pathParameters.markId
  };

  let initialResultSet = await db.vw_distinct_mark_events.find(initialCriteria);

  // Main query of the mark in question as well as its associated history
  let parentResultSet = [];

  let parentIdOperation = `id =`;
  let childViewName = 'vw_mark_history';
  let grandchildViewName = 'vw_mark_event_summary';
  let allocationIdOperation = `${childViewName}.ma_mark_id =`;
  let allocationNullOperation = `${childViewName}.ma_mark_id is`;
  let stateIdOperation = `${childViewName}.ms_mark_id =`;
  let stateNullOperation = `${childViewName}.ms_mark_id is`;

  let parentCriteria = {
    [parentIdOperation]: event.pathParameters.markId,
  }

  let childCriteria = {
    and: [
      {
        or: [
          { [allocationIdOperation]: event.pathParameters.markId },
          { [allocationNullOperation]: null }
        ]
      },
      {
        or: [
          { [stateIdOperation]: event.pathParameters.markId },
          { [stateNullOperation]: null }
        ]
      }
    ]
  }

  // If some events are found related to the selected mark, we continue to process a result
  // -> Where no events are found, we immediately close out the search and return a 404
  if (initialResultSet.length > 0) {
    // --------------------------------
    let childEventIdOperation = `${childViewName}.event_id =`;
    let grandchildEventIdOperation = `${grandchildViewName}.event_id =`;
    let eventCriteria = initialResultSet.map(initialResult => {
      // ------------------------------
      return {
        and: [
          { [childEventIdOperation]: initialResult.event_id },
          { [grandchildEventIdOperation]: initialResult.event_id }
        ]
      }
    });
    childCriteria.and.push({
      or: eventCriteria
    });

    // -----------------------
    // USER ACCESS CONTROL
    // -----------------------
    // If a user is anything besides admin we need to manage their access to the banding records
    if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
      let userAccessControlCriteria = generateUserAccessControlFilterCriteria(claims, `check_`);
      parentCriteria = { ...parentCriteria, ...userAccessControlCriteria };
    }

    // -----------------------------------
    // THESE BOTH RETURN A PROMISE
    parentResultSet = await db.vw_mark_latest
                                  .find(parentCriteria);

    let childResultSet = await db.vw_mark_history
                                  .join({
                                    pk: 'event_id',
                                    vw_mark_event_summary: {
                                      type: 'LEFT',
                                      pk: 'event_id',
                                      on: { [`event_id`]: `${childViewName}.event_id` },
                                      decomposeTo: 'object',
                                    }
                                  })
                                  .find(childCriteria);

    parentResultSet = parentResultSet.map(result => {
      return {
        ...result,
        mark_history: [ ...childResultSet]
      }
    });
  }

    return {
      data: parentResultSet
    };
}


const generatePaginationFilterCriteria = (type = 'lookahead', paginationToken, sortOrder = 'asc') => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generatePaginationFilterCriteria()");

  let criteria = {
    and: []
  };

  switch (type) {
    case 'lookbehind':
      if (paginationToken) {
        switch (sortOrder) {
          case 'asc':
            // Lookbehind
            criteria.and.push({ 'pagination_idx <': paginationToken });
            break;
          case 'desc':
            // Lookbehind
            criteria.and.push({ 'pagination_idx >': paginationToken });
            break;
          default:
            break;
        }
      }
      return (criteria.and.length > 0) ? criteria : { and: [{ 'pagination_idx >': 0 }] };
    case 'lookahead':
    default:
      if (paginationToken) {
        switch (sortOrder) {
          case 'asc':
            // Lookahead
            criteria.and.push({ 'pagination_idx >': paginationToken });
            break;
          case 'desc':
            // Lookahead
            criteria.and.push({ 'pagination_idx <': paginationToken });
            break;
          default:
            break;
        }
      }
      return (criteria.and.length > 0) ? criteria : { and: [{ 'pagination_idx >': 0 }] };
  }
}


const generateUserAccessControlFilterCriteria = (claims, prefix = '') => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateUserAccessControlFilterCriteria()");

  let criteria = { [`${prefix}bander_id =`]: claims.sub };

  return criteria;
}


const processWildcardSearch = (paramName, paramValue) => {
  // ----------------------------------------------------------------------------
  //paramName should be 'prefixNumber' or 'shortNumber' see ln 82 bb_helpers.js ALLOWABLE_EVENT_SEARCHES

  paramValue = paramValue.toLowerCase();
  if (paramValue.includes('*')) {
    // ------------------------------------------------
    paramValue = paramValue.split('*').join('%');
    return { [ALLOWABLE_SEARCHES[paramName].wildcard_filter_name]: paramValue };
  }
  else {
    // ------------------------------------------------
    return { [ALLOWABLE_SEARCHES[paramName].filter_name]: paramValue };
  }
}


const generateQueryStringFilterCriteria = (multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringFilterCriteria()");

  let criteria = {
    and: []
  };

  let nonFilterQueryStringParameters = [...['limit', 'paginationToken'], ...Object.keys(ALLOWABLE_SEARCHES)];

  // If there are any query string parameters, we need to process them, otherwise, continue
  if (multiValueQueryStringParameters && Object.keys(multiValueQueryStringParameters).length > 0) {
    // ----------------------------------------------------------------------------   
    let filterQueryStringParameterNames = Object.keys(multiValueQueryStringParameters).filter(queryStringParam => !nonFilterQueryStringParameters.includes(queryStringParam));

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
          if (['shortNumberTo', 'shortNumberFrom'].includes(filterQueryStringParameterName)) {
            innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: parseInt(filterQueryStringParameterValue) });
          }
          else {
            innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: filterQueryStringParameterValue });
            return;
          }
        });

        if (innerCriteria.or.length > 0) criteria.and.push(innerCriteria);
      }
    });
  }

  return criteria;
}


const generateQueryStringSearchCriteria = (multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringSearchCriteria()");

  let criteria = {
    and: []
  };

  let nonSearchQueryStringParameters = [...['limit', 'paginationToken'], ...Object.keys(ALLOWABLE_FILTERS)];

  // If there are any query string parameters, we need to process them, otherwise, continue
  if (multiValueQueryStringParameters && Object.keys(multiValueQueryStringParameters).length > 0) {
    // -----------------------------------------------------------
    let searchQueryStringParameterNames = Object.keys(multiValueQueryStringParameters).filter(queryStringParam => !nonSearchQueryStringParameters.includes(queryStringParam));

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


const searchDB = async (db, event = null, claims, governingCognitoGroup) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".searchDB()");

  let pathParameters = event.pathParameters;
  let queryStringParameters = event.queryStringParameters;
  let multiValueQueryStringParameters = event.multiValueQueryStringParameters;

  let view_name = 'vw_mark_latest';
  let aggregation_view_name = 'mark_stock_aggregation_rollup';

  // Validate that the query string parameters are compatible
  if (queryStringParameters && 'shortNumber' in queryStringParameters && ('shortNumberFrom' in queryStringParameters || 'shortNumberTo' in queryStringParameters)) {
    // ---------------------------------------------------------------------------------------
    throw new BoilerPlate.ParameterValidationError(`Incompatible query string parameters.`, `Both 'shortNumber' and either 'shortNumberFrom' or 'shortNumberTo' have been submitted with this request`);
  }

  // For format querys we just want to return the inventory counts for the bander (or banding office)
  if (queryStringParameters
    && 'format' in queryStringParameters
    && queryStringParameters.format.toLowerCase() === 'summary') {

    let criteria = {
      'bander_id = ': claims.sub
    };

    if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME
      && queryStringParameters && 'banderId' in queryStringParameters) {
      // If the admin group is enabled and a banderId is requested, we want to return that banders stock levels
      // ------------------------------------------------
      criteria["bander_id = "] = queryStringParameters.banderId;
    }
    else if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
      // If the admin group is enabled we want to return the banding office stock levels
      // ------------------------------------------------
      criteria["bander_id = "] = process.env.BANDING_OFFICE_ID;
    }

    let standardAggregation = await db[aggregation_view_name].find(criteria,
      {
        fields: ['prefix_number', 'number_of_bands', 'last_short_number']
      }
    );

    // Theses are enumerated in the database based on the latest version of the Databox
    // -> May require review in due course, additions are possible
    let supported_prefix_numbers = await db.enums.enum_supported_prefix_numbers;

    return {
      data: [...standardAggregation]
        .filter(agg => (agg.last_short_number
          && supported_prefix_numbers.includes(agg.prefix_number)
          && !['pit'].includes(agg.prefix_number)))
        // filter migration artifacts and and non-numeric short numbers out then sort alphanumerically
        .sort((a, b) => {
          if (a.prefix_number < b.prefix_number) { return -1; }
          if (a.prefix_number > b.prefix_number) { return 1; }
          return 0;
        })
    };
  }

  // Get the resource-name, remove the plurality and append 'Id'
  let limit = (queryStringParameters && queryStringParameters.limit) ? parseInt(queryStringParameters.limit) : 100;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
    parseInt(queryStringParameters.paginationToken) : null;
  let prev = null;
  // ------------------------------------
  // QUERY STRING ORDER
  // ------------------------------------
  // Set standard sort order
  let sortOrder = 'asc';

  // -----------------------------------------------------
  // INITIALISE LOOKAHEAD AND LOOKBEHIND QUERY CRITERIA
  // -----------------------------------------------------
  let lookAheadCriteria = generatePaginationFilterCriteria('lookahead', paginationToken, sortOrder);
  let lookBehindCriteria = generatePaginationFilterCriteria('lookbehind', paginationToken, sortOrder);

  // Check if a sort order has been requested
  if (queryStringParameters && 'order' in queryStringParameters && ['ASC', 'DESC'].includes(queryStringParameters.order)) {
    sortOrder = queryStringParameters.order.toLowerCase();
  }

  console.info(`Lookahead pagination criteria: ${JSON.stringify(lookAheadCriteria)}`);
  console.info(`Lookbehind pagination criteria: ${JSON.stringify(lookBehindCriteria)}`);

  // -----------------------
  // USER ACCESS CONTROL
  // -----------------------
  // If a user is anything besides admin we need to manage their access to the banding records
  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    // ------------------------------------------------
    let userAccessControlCriteria = generateUserAccessControlFilterCriteria(claims, ``);
    lookAheadCriteria.and.push(userAccessControlCriteria);
    lookBehindCriteria.and.push(userAccessControlCriteria);
  }

  // -----------------------
  // QUERY STRING SEARCHING
  // -----------------------
  let searchCriteria = generateQueryStringSearchCriteria(multiValueQueryStringParameters);
  if (searchCriteria.and.length > 0) {
    lookAheadCriteria.and = [...lookAheadCriteria.and, ...searchCriteria.and];
    lookBehindCriteria.and = [...lookBehindCriteria.and, ...searchCriteria.and];
  }

  // -----------------------
  // QUERY STRING FILTERING
  // -----------------------
  let filterCriteria = generateQueryStringFilterCriteria(multiValueQueryStringParameters);
  if (filterCriteria.and.length > 0) {
    lookAheadCriteria.and = [...lookAheadCriteria.and, ...filterCriteria.and];
    lookBehindCriteria.and = [...lookBehindCriteria.and, ...filterCriteria.and];
  }

  let lookAheadOptions = {
    limit: (limit + 1),
    order: [{ field: 'pagination_idx', direction: sortOrder }]
  }

  let lookBehindOptions = {
    limit: (limit + 1),
    order: [{ field: 'pagination_idx', direction: (sortOrder === 'asc') ? 'desc' : 'asc' }],
  }

  // Flatten any single entry 'OR' criteria
  lookAheadCriteria.and = lookAheadCriteria.and.map(subCriteria => {
    if ('or' in subCriteria && subCriteria.or.length === 1) {
      return subCriteria.or[0];
    }
    else {
      return subCriteria
    }
  });

  lookBehindCriteria.and = lookBehindCriteria.and.map(subCriteria => {
    if ('or' in subCriteria && subCriteria.or.length === 1) {
      return subCriteria.or[0];
    }
    else {
      return subCriteria
    }
  });

  console.info(`Lookahead criteria: ${JSON.stringify(lookAheadCriteria)}`);
  console.info(`Lookbehind criteria: ${JSON.stringify(lookBehindCriteria)}`);

  let finalLookaheadResultSet = await db[view_name].find(lookAheadCriteria, lookAheadOptions);
  let finalLookbehindResultSet = [];

  if (paginationToken) {
    // -----------------------
    finalLookbehindResultSet = await db[view_name].find(lookBehindCriteria, lookBehindOptions);
  }

  let lookBehindPaginationTokens = [];

  if (finalLookbehindResultSet.length > 0) {
    finalLookbehindResultSet.forEach((resItem, idx) => {
      lookBehindPaginationTokens.push(resItem.pagination_idx);
    });

    lookBehindPaginationTokens.sort();
    if (sortOrder === 'desc') {
      // ----------------------------------
      lookBehindPaginationTokens.reverse();
    }

    let lookBehindIndex = (lookBehindPaginationTokens.length > limit) ? 1 : null;
    prev = (lookBehindIndex) ? [...lookBehindPaginationTokens][lookBehindIndex] : null;
  }

  // Capture cases where the mark_state is changing (or mark_allocatee)
  finalLookaheadResultSet = finalLookaheadResultSet.map(markDetail => {
    // -------------------------------------------------------
    let res = {
      ...markDetail
    };

    if (markDetail.state !== markDetail.check_mark_state) {
      // -------------------------
      res.state = `**${markDetail.check_mark_state}**`;
    }

    if (markDetail.bander_id !== markDetail.check_bander_id) {
      // -------------------------
      res.state = `**TRANSFER**`
    }

    return res;
  });

  if (finalLookaheadResultSet.length > 0) {
    // -------------------------------------------
    return {
      data: (finalLookaheadResultSet.length > limit) ? finalLookaheadResultSet.slice(0, finalLookaheadResultSet.length - 1) : finalLookaheadResultSet,
      count: (finalLookaheadResultSet.length > limit) ? (finalLookaheadResultSet.length - 1) : finalLookaheadResultSet.length,
      sortOrder: sortOrder,
      isLastPage: finalLookaheadResultSet.length <= limit,
      current: paginationToken,
      prev: (paginationToken && prev) ? prev : null
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


const formatResponse = (event, method = 'search', res, pagination_idx) => {
  // ----------------------------------------------------------------------------    

  let response = res.data;

  switch (method) {
    case 'get': {
      response = response.map(mark => {
        mark.mark_history = mark.mark_history.map(historyEvent => {
          let formattedMarkEventSummary = historyEvent.vw_mark_event_summary;
          delete formattedMarkEventSummary.state;

          let formattedHistoryEvent = {
            ...historyEvent,
            ...formattedMarkEventSummary,
            is_multiple_bands: historyEvent.vw_mark_event_summary.mark_count > 1
          }
          delete formattedHistoryEvent.vw_mark_event_summary;
          return formattedHistoryEvent;
        })
        return mark;
      });
      return (response.length > 0) ? response[0] : {};
    }
    case 'put': {
      return response.mark_state;
    }
    case 'post': {
      return response.mark.map(mark => mark.id);
    }
    case 'delete': {
      return {
        marks: res.marks
      };
    }
    case 'search':
    default: {
      // MARK AGGREGATION
      if (event.queryStringParameters && 'format' in event.queryStringParameters && event.queryStringParameters.format.toLowerCase() === 'summary') {
        // -------------------------------------------
        response = response.map(bandSummary => {
          return {
            prefix_number: bandSummary.prefix_number,
            number_of_bands: bandSummary.number_of_bands,
            last_short_number: bandSummary.last_short_number
          }
        });
      }
      // REGULAR MARKS SEARCH
      // Check if pagination_idx is accessible (if not matview may be stale, provide optimistic pagination_idx)
      else if (response.length > 0 && !response[response.length - 1].pagination_idx) {
        response[response.length - 1].pagination_idx = pagination_idx + response.length;
      }
      return response;
    }
  }
}


const validateSupportedPrefix = async (customErrorFactory, db, prefixNumber) => {
  // -----------------------------------------
  let isSupportedPrefixRes = await db.ro_is_supported_prefix(prefixNumber);
  if (!isSupportedPrefixRes[0].ro_is_supported_prefix) {
    return customErrorFactory.getError('UnsupportedPrefixNumber', [prefixNumber, 'prefix_number']);
  }
  else {
    return null;
  }
}


const validateShortNumbers = async (customErrorFactory, firstShortNumber, lastShortNumber) => {
  // -----------------------------------------
  if (firstShortNumber > lastShortNumber) {
    return Promise.resolve(customErrorFactory.getError('InvalidShortNumberRange', [firstShortNumber, lastShortNumber, 'first_short_number,last_short_number']));
  }
}


const validateShortNumberUniqueness = async (customErrorFactory, prefixNumber, shortNumbers, firstShortNumber, lastShortNumber) => {
  // -----------------------------------------
  let duplicates = await db.ro_are_marks_unique(prefixNumber, shortNumbers);
  if (duplicates.length > 0) {
    return customErrorFactory.getError('DuplicateShortNumber', [duplicates.length, prefixNumber, firstShortNumber, lastShortNumber, 'first_short_number,last_short_number'])
  }
  else {
    return null;
  }
}


const validateIsAdmin = async (governingCognitoGroup, customErrorFactory) => {
  // -----------------------------------------
  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    return customErrorFactory.getError('AccessDenied');
  }
  else {
    return null;
  }
}


const validateMarkStatusNew = async (customErrorFactory, db, payload) => {
  // -----------------------------------------
  let stringArray = payload.map(id => id);

  let status = await db.ro_are_marks_new([stringArray])
  if (status.length > 0) {
    let list_ids = status.map(mark => mark.mark_id)
    return customErrorFactory.getError('StatusNotNew', [status.length, list_ids, 'mark_id'])
  } else {
    return null;
  }
}


const validateMarkExists = async (customErrorFactory, db, event, claims) => {
  // -----------------------------------------
  console.info(RESOURCE_NAME + ".validateMarkExists()");

  let markId = event.pathParameters.markId;

  let markExistsResultset = await db.ro_is_mark(markId);

  if (!markExistsResultset[0].ro_is_mark) {
    return customErrorFactory.getError('NotFoundError', ['markId', markId, 'pathParameters.markId']);
  }
  else {
    return null;
  }
}


const validateMarkAllocatee = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------
  console.info(RESOURCE_NAME + ".validateMarkAllocatee()");

  let markId = event.pathParameters.markId;
  let banderId = claims.sub;

  let markAllocateeResultset = await db.ro_is_mark_allocatee(markId, banderId);

  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME && !markAllocateeResultset[0].ro_is_mark_allocatee) {
    return customErrorFactory.getError('ForbiddenError', [`updating the state of mark_id ${markId}`, `banderId`, 'claims.sub']);
  }
  else {
    return null;
  }
}


const validateMarkStateLifecycle = async (customErrorFactory, db, event) => {
  // -----------------------------------------
  console.info(RESOURCE_NAME + ".validateMarkStateLifecycle()");

  let markId = event.pathParameters.markId;
  let proposedState = JSON.parse(event.body).state;

  let latestMarkStateResultset = await db.ro_latest_mark_state(markId);
  if (!USER_MARK_STATE_ACTIONS[latestMarkStateResultset[0].ro_latest_mark_state].includes(proposedState)) {
    return customErrorFactory.getError('MarkStateLifecycleError', [`Before being updated to ${proposedState}, a mark must be in one of the following states: ${USER_MARK_STATE_ACTIONS[latestMarkStateResultset[0].ro_latest_mark_state].join(', ')}`, `This mark's current state is ${latestMarkStateResultset[0].ro_latest_mark_state}`, proposedState, `/state`, 'CRITICAL']);
  }
  else {
    return null;
  }
}


const validateBusinessRules = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------------------
  let markCreationLimit = 2500;
  let payload = JSON.parse(event.body);
  let promises = [];
  let format = event.queryStringParameters.format

  // Switch the validation depending on whether a range or batch has been selected

  if (format === 'range') {
    let prefixNumber = payload.prefix_number.toLowerCase();
    let firstShortNumber = parseInt(payload.first_short_number);
    let lastShortNumber = parseInt(payload.last_short_number);
    let shortNumbers = _.range(firstShortNumber, (lastShortNumber + 1)).map(String).map(shortNumber => shortNumber.padStart(4, '0'));

    // -------------------------------------
    // Validate admin user group
    // -------------------------------------
    if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
      return customErrorFactory.getError('ForbiddenError', ['creating bands', claims.sub, 'claims.sub']);
    }

    // -------------------------------------
    // Performance control, ensure no more than a soft limited number of marks can be created
    // -------------------------------------
    if (lastShortNumber > firstShortNumber && shortNumbers.length > markCreationLimit) {
      return [customErrorFactory.getError('LimitExceedanceError', [`Number of marks exceeds mark-creation api limit of ${markCreationLimit}`, '', shortNumbers.length, 'first_short_number/last_short_number'])];
    }

    // -------------------------------------
    // Validate prefix_number is supported
    // -------------------------------------
    let supportedPrefixError = await validateSupportedPrefix(customErrorFactory, db, prefixNumber);
    if (supportedPrefixError) {
      return [supportedPrefixError];
    }

    // -------------------------------------
    // Validate range is valid
    // -------------------------------------
    let shortNumbersError = await validateShortNumbers(customErrorFactory, firstShortNumber, lastShortNumber);
    if (shortNumbersError) {
      return [shortNumbersError];
    }

    // ------------------------------------------------
    // Generate array of short numbers are all unique
    // ------------------------------------------------
    let uniquenessError = await validateShortNumberUniqueness(customErrorFactory, prefixNumber, shortNumbers, firstShortNumber, lastShortNumber);
    if (uniquenessError) {
      return [uniquenessError];
    }

    return [];
  }

  if (format === 'batch') {
    // -------------------------------------
    // Validate user is Admin
    // -------------------------------------
    promises.push(validateIsAdmin(governingCognitoGroup));


    let errors = await Promise.all(promises);

    return errors.filter(error => error);
  }


  return [];
}


const generateMarkCreationEvent = (event) => {
  // -----------------------------------------------------
  let payload = JSON.parse(event.body);

  // Handle Range Band Creation
  if (event.queryStringParameters.format === 'range') {
    // ------------------------------------
    let promises = [];

    let prefixNumber = payload.prefix_number.toLowerCase();
    let firstShortNumber = parseInt(payload.first_short_number);
    let lastShortNumber = parseInt(payload.last_short_number);

    // Generate short number range
    let shortNumbers = _.range(firstShortNumber, (lastShortNumber + 1)).map(String).map(shortNumber => shortNumber.padStart(4, '0'));
    let mark_state = [];
    let mark_allocation = [];
    let mark = shortNumbers.map(shortNumber => {
      // Add to mark_state
      return {
        "prefix_number": prefixNumber,
        "short_number": shortNumber
      }
    });

    // Generate event body
    let event = {
      "event_banding_scheme": "NZ_NON_GAMEBIRD",
      "event_owner_id": process.env.BANDING_OFFICE_ID,
      "event_provider_id": process.env.BANDING_OFFICE_ID,
      "event_reporter_id": process.env.BANDING_OFFICE_ID,
      "event_state": "VALID",
      "event_timestamp": Moment().format(),
      "event_timestamp_accuracy": "D",
      "event_type": "NEW_MARK",
      "project_id": process.env.STOCK_PROJECT_ID,
      "transfer_recipient_id": process.env.BANDING_OFFICE_ID,
      "mark_count": mark.length
    }

    return {
      event: event,
      mark: mark
    }
  }
}


const postMarkCreationEventToDB = (db, payload) => {
  // -----------------------------------------------------

  return db.withTransaction(async tx => {
    // -----------------------------------
    let mark = await tx.mark.insert(payload.mark);

    let event = await tx.event.insert(payload.event);

    let mark_allocation = await tx.mark_allocation.insert(mark.map(mark => {
      // ----------------------------------------------------------
      return {
        event_id: event.id,
        mark_id: mark.id,
        bander_id: process.env.BANDING_OFFICE_ID,
        allocation_idx: 0,
        is_current: true
      }
    }));

    let mark_state = await tx.mark_state.insert(mark.map(mark => {
      // ----------------------------------------------------------
      return {
        event_id: event.id,
        mark_id: mark.id,
        state: 'NEW',
        state_idx: 0,
        is_current: true
      }
    }));

    let bander_prefix_stock_update = await tx.rw_update_banding_office_stock([[process.env.BANDING_OFFICE_ID], [payload.mark[0].prefix_number]]);

    return {
      data: {
        event: event,
        mark: mark,
        mark_state: mark_state,
        mark_allocation: mark_allocation
      }
    }
  });
}


const validatePutBusinessRules = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------------------
  let payload = JSON.parse(event.body);
  let promises = [];

  // -------------------------------------
  // Validate prefix_number is supported
  // -------------------------------------
  promises.push(validateMarkAllocatee(customErrorFactory, db, event, claims, governingCognitoGroup));

  // -------------------------------------
  // Validate range is valid
  // -------------------------------------
  promises.push(validateMarkStateLifecycle(customErrorFactory, db, event));



  return Promise.all(promises).filter(error => error);
}

const generateMarkStateUpdateEvent = (event, claims) => {
  // -----------------------------------------------------
  let markId = event.pathParameters.markId;
  let payload = JSON.parse(event.body);

  let event_type = null;

  switch (payload.state) {
    case 'LOST':
      event_type = 'LOST';
      break;
    case 'ALLOCATED':
      event_type = 'FOUND';
      break;
    case 'RETURNED_USED':
      event_type = 'TRANSFER';
      break;
    case 'PRACTICE':
      event_type = 'PRACTICE';
      break;
    case 'OTHER':
    default:
      event_type = 'OTHER';
      break;
  }

  // Generate event body
  let eventUpdate = {
    "event_banding_scheme": "NZ_NON_GAMEBIRD",
    "event_owner_id": process.env.BANDING_OFFICE_ID,
    "event_provider_id": claims.sub,
    "event_reporter_id": claims.sub,
    "event_state": "VALID",
    "event_timestamp": Moment().format(),
    "event_timestamp_accuracy": "D",
    "event_type": event_type,
    "project_id": process.env.STOCK_PROJECT_ID
  }

  let mark_allocation = null;
  if (payload.state === 'RETURNED_USED') {
    mark_allocation = {
      mark_id: markId,
      bander_id: process.env.BANDING_OFFICE_ID,
      allocation_idx: 0,
      is_current: false
    }
  }

  let mark_state = {
    mark_id: markId,
    state: payload.state,
    state_idx: 0,
    is_current: false
  }

  // Add to mark_state
  return {
    event: eventUpdate,
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


const putMarkStateUpdateEventToDB = (db, payload) => {
  // -----------------------------------------------------

  return db.withTransaction(async tx => {
    // -----------------------------------
    // 1) Create the new event
    // -----------------------------------
    let eventUpdate = await tx.event.insert(payload.event);

    // ---------------------------------------------------
    // 2) If a TRANSFER event, add a mark allocation entry
    // ---------------------------------------------------
    let markAllocationUpdate = null;
    if (payload.mark_allocation) {
      markAllocationUpdate = await tx.mark_allocation.insert({
        event_id: eventUpdate.id,
        ...payload.mark_allocation
      });
    }

    // ---------------------------------------------------
    // 3) Add the mark state entry
    // ---------------------------------------------------
    let markStateUpdate = await tx.mark_state.insert({
      event_id: eventUpdate.id,
      ...payload.mark_state
    });

    // ---------------------------------------------------
    // 4a) Reset all mark_states to is_current = false
    // ---------------------------------------------------
    let reset_current_mark_states = await tx.mark_state.update(
      { 'mark_id =': payload.mark_state.mark_id },
      { 'is_current': false });

    // ---------------------------------------------------
    // 4b) If a TRANSFER event, reset all mark_allocations to is_current = false
    // ---------------------------------------------------
    let reset_current_mark_allocations = null;
    if (payload.mark_allocation) {
      reset_current_mark_allocations = await tx.mark_allocation.update(
        { 'mark_id =': payload.mark_state.mark_id },
        { 'is_current': false });
    }

    // ---------------------------------------------------
    // 5) Run full update of state_idx and is_current for all marks involved,
    //    and if required, mark_allocation as well
    // ---------------------------------------------------
    let mark_state_and_allocation_refresh = null;
    if (payload.mark_allocation) {
      mark_state_and_allocation_refresh = await tx.rw_update_latest_mark_allocation_and_state([[payload.mark_state.mark_id]]);
    }
    else {
      mark_state_and_allocation_refresh = await tx.rw_update_latest_mark_state([[payload.mark_state.mark_id]]);
    }

    // ---------------------------------------------------
    // 6) Update the stock aggregation depending on the 
    // ---------------------------------------------------
    let marks = await tx.mark.find({
      'id': payload.mark_state.mark_id
    });

    console.log(`Updating prefixes related to these marks: ${JSON.stringify(marks)}}`);

    let prefixesToUpdate = getDistinctMarkPrefixesFromMarks(marks);
    console.log(`prefixes to update: ${prefixesToUpdate}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixesToUpdate.length > 0) {
      // -----------------------------------
      let banderPrefixUpdate = await tx.rw_update_bander_stock_by_prefix([process.env.BANDING_OFFICE_ID, prefixesToUpdate]);
      let bandingOfficePrefixUpdate = await tx.rw_update_banding_office_stock([[process.env.BANDING_OFFICE_ID], prefixesToUpdate]);
    }

    return {
      data: {
        event: eventUpdate,
        mark_state: markStateUpdate,
        mark_allocation: markAllocationUpdate,
        mark_state_and_allocation_refresh: mark_state_and_allocation_refresh
      }
    }
  });
}


const validateDeleteBusinessRules = async (customErrorFactory, db, event) => {
  // -----------------------------------------------------
  let payload = JSON.parse(event.body);
  let promises = [];
  let format = event.queryStringParameters.format
  // Switch the validation depending on whether a range or batch has been selected

  if (format === 'range') {

    let errors = await Promise.all(promises);

    return errors.filter(error => error);
  }
  if (format === 'batch') {

    // -------------------------------------
    // Validate all marks have status New
    // -------------------------------------
    promises.push(validateMarkStatusNew(customErrorFactory, db, payload));

    let errors = await Promise.all(promises);
    return errors.filter(error => error);
  }

  return [];
}


const deleteNewMarkFromDB = (db, event) => {
  let payload = JSON.parse(event.body);

  return db.withTransaction(async tx => {
    // -----------------------------------

    let recordActionParams = [];

    let mark_criteria = {
      or: payload.map(mark_id => {
        recordActionParams.push({
            db_action: 'DELETE',
            db_table: 'mark',
            db_table_identifier_name: 'id',
            db_table_identifier_value: mark_id
          });
        return {
          'id =': mark_id
        }
      })
    }

    let mark_state_and_allocation_criteria = {
      or: payload.map(mark_id => {
        recordActionParams.push({
          db_action: 'DELETE',
          db_table: 'mark_state',
          db_table_identifier_name: 'mark_id',
          db_table_identifier_value: mark_id
        });
        recordActionParams.push({
          db_action: 'DELETE',
          db_table: 'mark_allocation',
          db_table_identifier_name: 'mark_id',
          db_table_identifier_value: mark_id
        });
        return {
          'mark_id =': mark_id
        }
      })
    }

    let mark_state = await tx.mark_state.destroy(mark_state_and_allocation_criteria);

    let mark_allocation = await tx.mark_allocation.destroy(mark_state_and_allocation_criteria);

    let marks = await tx.mark.destroy(mark_criteria);

    console.log(`Updating prefixes related to these marks: ${JSON.stringify(marks)}}`);

    let prefixesToUpdate = getDistinctMarkPrefixesFromMarks(marks);
    console.log(`prefixes to update: ${prefixesToUpdate}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixesToUpdate.length > 0) {
      // -----------------------------------
      let bandingOfficePrefixUpdate = await tx.rw_update_banding_office_stock([[process.env.BANDING_OFFICE_ID], prefixesToUpdate]);
    }

    let recordActionUpdate = await tx.record_action.insert(recordActionParams);

    return {
      mark_state: mark_state,
      mark_allocation: mark_allocation,
      marks: marks,
      record_actions: recordActionUpdate
    };
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

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  // JSON Schemas
  var paramSchema = {};

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
      console.info('Getting mark');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return getDB(db, event, claims, governingCognitoGroup);
    })
    .then(res => {
      if (res.data.length === 0) throw new BoilerPlate.NotFoundError('MarkNotFound', { type: 'NOT_FOUND', message: `Mark cannot be found with this id`, data: { path: `pathParameters.markId` }, value: event.pathParameters.markId, schema: null });
      return formatResponse(event, 'get', res);
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

  console.log(JSON.stringify(event));

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
  let sortOrder = 'asc';

  // JSON Schemas
  var paramSchema = {};

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

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
      return searchDB(db, event, claims, governingCognitoGroup);
    })
    .then(res => {
      if (!(event.queryStringParameters && 'format' in event.queryStringParameters && event.queryStringParameters.format.toLowerCase() === 'summary')) {
        count = res.count;
        sortOrder = res.sortOrder;
        isLastPage = res.isLastPage;
        current = res.current;
        prev = res.prev;
      }
      return formatResponse(event, 'search', res, current);
    })
    .then(res => {
      if (event.queryStringParameters && 'format' in event.queryStringParameters && event.queryStringParameters.format.toLowerCase() === 'summary') {
        return res;
      }
      let params = {
        data: res, path: event.path,
        queryStringParameters: event.queryStringParameters,
        multiValueQueryStringParameters: event.multiValueQueryStringParameters,
        paginationPointerArray: ['pagination_idx'],
        maxLimit: 100, order: sortOrder,
        count: count,
        countFromPageToEnd: isLastPage,
        isLastPage: isLastPage, prevPaginationToken: prev
      }

      console.log(JSON.stringify(params));
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
  console.info(event);

  // Get the schema details for parameter validation
  var parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  let format = event.queryStringParameters.format

  var payloadSchemaParams = {
    table: process.env.PAYLOAD_SCHEMA_TABLE,
    id: (format === 'range') ? process.env.RANGE_PAYLOAD_SCHEMA_ID : process.env.BATCH_PAYLOAD_SCHEMA_ID,
    version: (format === 'range') ? Number(process.env.RANGE_PAYLOAD_SCHEMA_VERSION) : Number(process.env.RANGE_PAYLOAD_SCHEMA_VERSION)
  }

  var customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
  }

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  // JSON Schemas
  var paramSchema = {};
  var payloadSchema = {};

  // Payload
  let payload = JSON.parse(event.body);

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
      console.info('Payload parameters OK. Connecting to DB to validate business rules...');
      // Get Custom Errors schema from Dynamo
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
      console.info('Validating business rules...');
      return validateBusinessRules(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      return generateMarkCreationEvent(event);
    })
    .then(res => {
      return postMarkCreationEventToDB(db, res);
    })
    .then(res => {
      return formatResponse(event, 'post', res);
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
  console.info(JSON.stringify(event));

  // Get the schema details for parameter validation
  var parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  let component = event.queryStringParameters.component

  // Down the road, handle alternative access to this endpoint
  var payloadSchemaParams = {
    table: process.env.PAYLOAD_SCHEMA_TABLE,
    id: process.env.BATCH_MARK_STATE_PAYLOAD_SCHEMA_ID,
    version: Number(process.env.BATCH_MARK_STATE_PAYLOAD_SCHEMA_VERSION)
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
      // Store highest claimed group for reference further on in the function
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
      // Get Custom Errors schema from Dynamo
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
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
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload schema validation error(s)!`, responseErrors);
      }
      console.info('Payload parameters OK. Connecting to DB to validate business rules...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return validateMarkExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Mark not found', error);
      console.info('Validating business rules...');
      return validatePutBusinessRules(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(errors => {
      console.log(errors);
      if (errors.filter(error => error.code === 403).length > 0) {
        throw new BoilerPlate.ForbiddenError(`Authorisation validation error(s)!!`, errors);
      }
      else if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      return generateMarkStateUpdateEvent(event, claims);
    })
    .then(res => {
      console.log(JSON.stringify(res));
      return putMarkStateUpdateEventToDB(db, res);
    })
    .then(res => {
      console.log(JSON.stringify(res));
      return formatResponse(event, 'put', res);
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

  let format = event.queryStringParameters.format

  var payloadSchemaParams = {
    table: process.env.PAYLOAD_SCHEMA_TABLE,
    id: (format === 'range') ? process.env.RANGE_PAYLOAD_SCHEMA_ID : process.env.BATCH_PAYLOAD_SCHEMA_ID,
    version: (format === 'range') ? Number(process.env.RANGE_PAYLOAD_SCHEMA_VERSION) : Number(process.env.RANGE_PAYLOAD_SCHEMA_VERSION)
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


  let governingCognitoGroup = null;

  // Do the actual work
  BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => {
      // Store highest claimed group for reference further on in the function
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
      // Get Custom Errors schema from Dynamo   ########## MODIFY OR delete ################
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
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
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      return BoilerPlate.validateJSON(payload, payloadSchema);
    })
    // Handle payload errors / Validate business rules
    .then(errors => {
      if (errors.length > 0) {
        // Format the errors and throw at this point to signify we aren't ready for business validation
        let responseErrors = formatSchemaErrors(customErrorFactory, errors, payload);
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload schema validation error(s)!`, responseErrors);
      }
      console.info('Payload parameters OK. Connecting to DB to validate business rules...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      console.info('Validating Deletion business rules...');
      return validateDeleteBusinessRules(customErrorFactory, db, event);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      return deleteNewMarkFromDB(db, event);
    })
    .then(res => {
      return formatResponse(event, 'delete', res);
    })
    .then(res => {
      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 204,
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