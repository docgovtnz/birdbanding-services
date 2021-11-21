'use strict';

// Modules
const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const MomentTimezone = require('moment-timezone');
const _ = require('lodash');
const uuidv4 = require('uuid/v4');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBHelpers = require('bb-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;
const validateEventExists = require('event-validation-lib').validateEventExists;

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "mark-event";


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


const getEventTypeFromState = (markState) => {
  // -----------------------------------------
  if (typeof markState === 'undefined' || !markState) {
    // ---------------------
    throw new Error('getEventTypeFromState inputs null or undefined');
  }

  switch(markState) {
    case 'NEW':
      return 'NEW_MARK';
    case 'ALLOCATED':
    case 'RETURNED_USED':
    case 'RETURNED':
      return 'TRANSFER'
    case 'LOST':
      return 'LOST';
    case 'PRACTICE':
      return 'PRACTICE';
    case 'OTHER':
    default:
      return 'OTHER';
  }
}


const getBanderIdForUpdate = (banderId, markState) => {
  // -----------------------------------------
  if (markState === 'RETURNED' || markState === 'RETURNED_USED') { return process.env.BANDING_OFFICE_ID; }
  else if (banderId) { return banderId; }
  else { return null };
}


const validateIsAdmin = async (governingCognitoGroup, customErrorFactory) => {
  // -----------------------------------------
  console.info(RESOURCE_NAME + ".validateIsAdmin()");

  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    return customErrorFactory.getError('ForbiddenError', [`executing this action`, claims.sub, 'claims.sub']);
  }
  else {
    return [];
  }
}


const getPostLookupData = async (customErrorFactory, event, claims, governingCognitoGroup, payload) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".getPostLookupData()");
  
  let criteria = {
    and: [
      { or: payload.marks.map(mark_id => { return { 'id =': mark_id } }) }
    ]
  };

  let resultSet = await db.mark.find(criteria);

  return resultSet;
}


const getPutLookupData = async (customErrorFactory, event, claims, governingCognitoGroup, payload) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".getPutLookupData()");

  console.log(event.pathParameters.eventId);
  console.log(payload.mark_id);

  let markEvent = [];

  if (event.resource.includes('mark-events-batch')) {
    // ---------------------------
    markEvent = await db.event
    .join({
      vw_distinct_prefix_numbers_by_event: {
        type: 'INNER',
        pk: 'event_id',
        on: { 'event_id': 'event.id' },
        decomposeTo: 'object'
      },
      vw_mark_event_summary: {
        type: 'INNER',
        pk: 'event_id',
        on: { 'event_id': 'event.id' },
        decomposeTo: 'object'
      }
    })
    .find({
      'id =': event.pathParameters.eventId
    });
  }
  else {
    // 1. Get detail for this mark event
    markEvent = await db.vw_mark_history
    .join({
      pk: 'event_id',
      vw_mark_event_summary: {
        type: 'INNER',
        pk: 'event_id',
        on: { 'event_id': 'vw_mark_history.event_id' },
        decomposeTo: 'object'
      },
      event: {
        type: 'INNER',
        pk: 'id',
        on: { 'id': 'vw_mark_history.event_id' },
        decomposeTo: 'object'
      },
      mark_allocation: {
        type: 'LEFT OUTER',
        pk: 'id',
        on: { 
          'event_id': 'vw_mark_history.event_id',
          'mark_id': 'vw_mark_history.ms_mark_id'
        },
        decomposeTo: 'object'
      },
      mark_state: {
        type: 'LEFT OUTER',
        pk: 'id',
        on: { 
          'event_id': 'vw_mark_history.event_id',
          'mark_id': 'vw_mark_history.ms_mark_id'
        },
        decomposeTo: 'object'
      },
      mark: {
        type: 'INNER',
        pk: 'id',
        on: {
          'id': 'vw_mark_history.ms_mark_id'
        },
        decomposeTo: 'object'
      }
    })
    .find({
      'vw_mark_history.event_id =': event.pathParameters.eventId,
      'vw_mark_history.ms_mark_id =': payload.mark_id,
      'or': [
        { 'vw_mark_history.ma_mark_id =': payload.mark_id },
        { 'vw_mark_history.ma_mark_id is': null }
      ]
    });
  }

  console.log(markEvent);

  if (markEvent.length === 0) {
    throw new BoilerPlate.NotFoundError('Event not found', customErrorFactory.getError('NotFoundError', ['eventId', event.pathParameters.eventId, 'pathParameters.eventId']));
  }

  return markEvent[0];
}


const getDeleteLookupData = async (customErrorFactory, event, claims, governingCognitoGroup) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".getDeleteLookupData()");

  console.log(event.pathParameters.eventId);

  let markEvent = await db.event
    .join({
      vw_distinct_prefix_numbers_by_event: {
        type: 'INNER',
        pk: 'event_id',
        on: { 'event_id': 'event.id' },
        decomposeTo: 'object'
      },
      vw_mark_event_summary: {
        type: 'INNER',
        pk: 'event_id',
        on: { 'event_id': 'event.id' },
        decomposeTo: 'object'
      }
    })
    .find({
      'id =': event.pathParameters.eventId
    });

  console.log(markEvent);

  return markEvent[0];
}


const validatePostBusinessRules = async (customErrorFactory, db, event, lookupData, claims, governingCognitoGroup) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".validatePostBusinessRules()");
  
  let bulkMarkEventCreationLimit = 500;
  let payload = JSON.parse(event.body);

  // ------------------------------------
  let errors = [];

  let marks = payload.marks;

  // -------------------------------------
  // Performance control, ensure no more than 500 marks associated with the proposed event
  // -------------------------------------
  if (marks.length > bulkMarkEventCreationLimit) {
    errors.push(customErrorFactory.getError('LimitExceedanceError', [`Number of marks exceeds bulk-mark-event-create api limit of ${bulkMarkEventCreationLimit}`, '', marks.length, 'marks.length']));
  }

  // -------------------------------------
  // Validate marks exists
  // -------------------------------------
  let marksNotFound = payload.marks.filter(payloadMarkId => {
    if (typeof lookupData.find(lookupMark => lookupMark.id === payloadMarkId) === 'undefined') {
      // If not found, do not filter the payloadMarkId
      return true;
    }
    // Otherwise filter out the payloadMarkId as it represents a real mark in the system
    return false;
  });

  console.log(marksNotFound.length);
  
  if (marksNotFound.length > 0) {
    // --------------------------
    errors.push(customErrorFactory.getError('NotFoundError', [`Marks:`, `${marksNotFound.join(', ')}`, '/marks']));
  }

  return errors;
}


const validatePutBusinessRules = async (customErrorFactory, event, lookupData, claims, governingCognitoGroup, payload) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".validatePutBusinessRules()");

  let errors = [];

  // -----------------------------------------
  // VALIDATE EVENT IS NOT A 'BIRD' EVENT
  // -----------------------------------------
  if (['ATTACHED', 'DETACHED'].includes(lookupData.state)) {
    errors.push(customErrorFactory.getError('MarkStateLifecycleError', [`Cannot update stock event with state: ${lookupData.state}`, `This relates to a bird event, please edit at: ${process.env.APPLICATION_DOMAIN}/data-uploads/edit-record/${event.pathParameters.eventId}`, `${lookupData.state}`, 'event.pathParameters.eventId', 'CRITICAL']));
  }

  // -----------------------------------------
  // IF A NEW_MARK event (state = NEW) or TRANSFER event (state = ALLOCATED) is being proposed,
  //    then a bander_id is mandatory
  // -----------------------------------------
  if (['NEW', 'ALLOCATED'].includes(lookupData.state) && !payload.bander_id) {
    errors.push(customErrorFactory.getError('InvalidBanderUpdate', []));
  }

  return errors;
}


const validatePutBatchBusinessRules = async (customErrorFactory, event, lookupData, claims, governingCognitoGroup, payload) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".validatePutBatchBusinessRules()");

  let errors = [];

  // -----------------------------------------
  // VALIDATE EVENT IS NOT A 'BIRD' EVENT
  // -----------------------------------------
  if (parseInt(lookupData.vw_mark_event_summary.mark_count) > 10000) {
    errors.push(customErrorFactory.getError('LimitExceedanceError', [`Too many marks associated with this event for update`, `Please contact support for support for updates to events which affect more than 10,000 marks`, lookupData.vw_mark_event_summary.mark_count, 'event.pathParameters.eventId']));
  }

  return errors;
}


const validateDeleteBusinessRules = (customErrorFactory, db, lookupData) => {
  // ----------------------------------------------------------------------------   
  console.info('.validateDeleteBusinessRules()');

  let errors = [];

  let allowablePreDeleteStates = [
    "ALLOCATED",
    "RETURNED",
    "PRACTICE",
    "LOST",
    "RETURNED_USED",
    "OTHER"
  ];

  if (!allowablePreDeleteStates.includes(lookupData.vw_mark_event_summary.state)) {
    // -----------------------
    errors.push(customErrorFactory.getError('PropertyEnumError', [`${lookupData.vw_mark_event_summary.state}`, `${lookupData.vw_mark_event_summary.state}`, `event.pathParameters.eventId`,`${allowablePreDeleteStates.join(', ')}`]));
  }

  return errors;
}


const generateBulkMarkEvent = (event, claims, payload) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".generateBulkMarkEvent()");

  let allocationToBeUpdatedToBandingOffice = ['RETURNED_USED', 'RETURNED'].includes(payload.mark_state);
  let bandAllocationRequested = ('bander_id' in payload && payload.bander_id);

  let newEventId = uuidv4();

  let newEvent = {
    // First, handle the main references
    id: newEventId,
    project_id: process.env.STOCK_PROJECT_ID,
    event_type: getEventTypeFromState(payload.mark_state),
    event_state: "VALID",
    event_banding_scheme: "NZ_NON_GAMEBIRD",
    event_timestamp: payload.event_timestamp,
    event_timestamp_accuracy: 'D',
    event_reporter_id: claims.sub,
    event_provider_id: claims.sub,
    event_owner_id: process.env.BANDING_OFFICE_ID,
    comments: 'Created as bulk stock event'
  };

  let newMarkStates = payload.marks.map(mark => {
    return {
      mark_id: mark,
      event_id: newEventId,
      state: payload.mark_state,
      is_current: false,
      state_idx: 0
    }
  });

  let newMarkAllocations = [];
  if (allocationToBeUpdatedToBandingOffice) {
    newMarkAllocations = payload.marks.map(mark => {
      return {
        mark_id: mark,
        event_id: newEventId,
        bander_id: process.env.BANDING_OFFICE_ID,
        is_current: false,
        allocation_idx: 0
      }
    });
  }
  else if (bandAllocationRequested) {
    newMarkAllocations = payload.marks.map(mark => {
      return {
        mark_id: mark,
        event_id: newEventId,
        bander_id: payload.bander_id,
        is_current: false,
        allocation_idx: 0
      }
    });
  }

  return {
    event: newEvent,
    mark_state: newMarkStates,
    mark_allocation: (allocationToBeUpdatedToBandingOffice || bandAllocationRequested) ? newMarkAllocations : undefined
  };
}


const generatePutDbPayload = async (db, payload, lookupData, warnings) => {
  // --------------------------------------------------------
  console.info(RESOURCE_NAME + ".generatePutDbPayload()");

  // THIS UPDATE ALSO ASSUMES THERE IS A DELTA
  // This is important because a split may occur even if there are no changes as part of the PUT request

  // ----
  // Prerequisite: Work out if this event needs to be split from a broader mark event
  //   which affects multiple marks (this is for UX reasons)
  let eventToBeSplit = parseInt(lookupData.vw_mark_event_summary.mark_count) > 1;
  // Prerequisite: Work out if state needs to be updated (bander change OR event split)
  let stateToBeUpdated = ('mark_state' in lookupData 
  && lookupData.mark_state.state !== payload.state)
  || ('mark_state' in lookupData && eventToBeSplit);  
  // Prerequisite: Work out if allocation needs to be updated 
  //  (bander change OR event split OR state change to RETURNED/RETURNED_USED)
  let allocationToBeUpdated = ('mark_allocation' in lookupData 
                                && lookupData.mark_allocation.bander_id !== payload.bander_id)
                              || ('mark_allocation' in lookupData && eventToBeSplit)
                              || (stateToBeUpdated && ['RETURNED_USED', 'RETURNED'].includes(payload.state));

  let newAllocationToBeCreated = (!('mark_allocation' in lookupData)
                                && payload.bander_id);

  console.log(`lookup: ${JSON.stringify(lookupData)}`);
  console.log(`Allocation to be updated: ${allocationToBeUpdated}`);
  console.log(`New allocation to be created: ${newAllocationToBeCreated}`);

  // ----
  // 1/ Start with the entities as-is
  let eventUpdate = {
    ...lookupData.event,
  }
  let allocationUpdate = (allocationToBeUpdated) ? {
    ...lookupData.mark_allocation
  } : null;
  let stateUpdate = (stateToBeUpdated) ? {
    ...lookupData.mark_state
  } : null;

  // 2/ Update the event ID where relevant if a split is required
  if (eventToBeSplit) {
    // -------------------
    console.info('[INFO] new ID being added to event to split from broader event');
    eventUpdate.id = uuidv4();
  }

  // 3/ Complete the peripheral event/allocation/state updates as required by the request
  eventUpdate.event_type = getEventTypeFromState(payload.state);
  eventUpdate.event_timestamp = payload.event_timestamp;
  delete eventUpdate.row_creation_idx;
  delete eventUpdate.row_update_timestamp_;
  delete eventUpdate.row_update_user_;

  if (allocationUpdate) {
    // ------------------
    allocationUpdate = {
      ...allocationUpdate,
      bander_id: getBanderIdForUpdate(payload.bander_id, payload.state),
      event_id: eventUpdate.id, // either the original ID or the new ID
      mark_id: payload.mark_id,
      allocation_idx: 0,
      is_current: false
    }
    delete allocationUpdate.row_update_timestamp_;
    delete allocationUpdate.row_update_user_;
  }
  else if (newAllocationToBeCreated) {
    // ------------------
    allocationUpdate = {
      bander_id: getBanderIdForUpdate(payload.bander_id, payload.state),
      event_id: eventUpdate.id,
      mark_id: payload.mark_id,
      allocation_idx: 0,
      is_current: false
    }
  }

  if (stateUpdate) {
    // ------------------
    stateUpdate = {
      ...stateUpdate,
      state: payload.state,
      event_id: eventUpdate.id, // either the original ID or the new ID
      mark_id: payload.mark_id,
      state_idx: 0,
      is_current: false
    }
    delete stateUpdate.row_update_timestamp_;
    delete stateUpdate.row_update_user_;
  }

  return {
    eventToBeSplit,
    event: eventUpdate,
    allocationToBeUpdated,
    newAllocationToBeCreated,
    mark_allocation: allocationUpdate,
    stateToBeUpdated,
    mark_state: stateUpdate
  };
}


const generateBatchPutDbPayload = async (db, payload, lookupData, warnings) => {
  // --------------------------------------------------------
  console.info(RESOURCE_NAME + ".generateBatchPutDbPayload()");

  // ----
  // 1/ Update the event timestamp
  let eventUpdate = {
    ...lookupData,
    event_timestamp: payload.event_timestamp
  }

  let prefixNumbersToUpdate = eventUpdate.vw_distinct_prefix_numbers_by_event.prefix_numbers;
  delete eventUpdate.vw_distinct_prefix_numbers_by_event;

  return {
    eventToBeSplit: false,
    event: eventUpdate,
    prefixNumbersToUpdate,
    allocationToBeUpdated: false,
    mark_allocation: null,
    stateToBeUpdated: false,
    mark_state: null
  };
}


const postBulkMarkEventToDB = async (db, res, lookupData) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + ".postBulkMarkEventToDB()");

  return db.withTransaction(async tx => {
    // -----------------------------------

    // ------------------------------------------------------------------------------------------------------
    // 1) INSERT THE NEW EVENT
    // ------------------------------------------------------------------------------------------------------
    let eventCreate = await tx.event.insert(res.event);

    // ------------------------------------------------------------------------------------------------------
    // 2) INSERT ANY UPDATED MARK_STATE/ALLOCATION ENTRIES BASED ON THE REQUEST
    // ------------------------------------------------------------------------------------------------------
    let markStateCreate = await tx.mark_state.insert(res.mark_state);

    let markAllocationCreate = (typeof res.mark_allocation !== 'undefined') ? await tx.mark_allocation.insert(res.mark_allocation) : null;
    
    // ------------------------------------------------
    // 3) UPDATE THE MARK_STATE/ALLOCATION TIMELINES
    // ------------------------------------------------
    if (typeof res.mark_allocation !== 'undefined') {
      await tx.mark_allocation.update(
        { 
          'or': res.mark_allocation.map(allocation => { return { 'mark_id =': allocation.mark_id }})
        },
        {
          'is_current': false
        });
    }
    await tx.mark_state.update(
      { 
        'or': res.mark_state.map(state => { return { 'mark_id =': state.mark_id }})
      },
      {
        'is_current': false
      });

    let resetCurrentMarkStateAndAllocations = await tx.rw_update_latest_mark_allocation_and_state([res.mark_state.map(state => state.mark_id )]);
    console.info(`[INFO] RESET STATE AND ALLOCATION: ${JSON.stringify(resetCurrentMarkStateAndAllocations)}`);

    // If required, update all stock tallies for banders/marks affected
    console.log(JSON.stringify(lookupData));
    let prefixes_to_update = getDistinctMarkPrefixesFromMarks(lookupData);
    console.log(`prefixes to update: ${prefixes_to_update}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixes_to_update.length > 0) {
      // -----------------------------------
      let banderStockUpdate = await tx.rw_update_bander_stock_by_prefix([ process.env.BANDING_OFFICE_ID, prefixes_to_update ]);
      let bandingOfficeStockUpdate = await tx.rw_update_banding_office_stock([ [process.env.BANDING_OFFICE_ID], prefixes_to_update ]);
      console.log(`[INFO] STOCK UPDATES: ${JSON.stringify(banderStockUpdate)} ${JSON.stringify(bandingOfficeStockUpdate)}`);
    }

    return {
      event: eventCreate,
      mark_state: markStateCreate,
      mark_allocation: markAllocationCreate
    }
  });
}


const putEventToDB = (db, payload, lookupData, event) => {
  // ----------------------------------------------------------------------------   
  console.info('.putEventToDB');

  console.log(JSON.stringify(payload));

  let originalEventId = event.pathParameters.eventId;

  return db.withTransaction(async tx => {
    // -----------------------------------

    // ------------------------------------------------------------------------------------------------------
    // 1) IF THE EVENT IS BEING SPLIT, INSERT THE NEW EVENT, OTHERWISE UPDATE THE EXISTING EVENT AS REQUIRED
    // ------------------------------------------------------------------------------------------------------
    let eventUpdate = null;
    if (payload.eventToBeSplit) {
      eventUpdate = await tx.event.insert(payload.event);
    }
    else {
      eventUpdate = await tx.event.update({ 'id =': payload.event.id }, payload.event);
    }
    console.log(JSON.stringify(eventUpdate));
    // ------------------------------------------------------------------------------------------------------
    // 2) DELETE ANY MARK_STATE/ALLOCATION ENTRIES WHICH ARE BEING UPDATED
    // ------------------------------------------------------------------------------------------------------
    if ('mark_allocation' in lookupData && payload.allocationToBeUpdated) {
      let allocationRemoval = await tx.mark_allocation.destroy({
        'id =': lookupData.mark_allocation.id
      });
      console.info(`[INFO] DELETED: ${JSON.stringify(allocationRemoval)}`);
    }
    if ('mark_state' in lookupData && payload.stateToBeUpdated) {
      let stateRemoval = await tx.mark_state.destroy({
        'id =': lookupData.mark_state.id
      })
      console.info(`[INFO] DELETED: ${JSON.stringify(stateRemoval)}`);
    }

    // ------------------------------------------------------------------------------------------------------
    // 2) INSERT ANY UPDATED MARK_STATE/ALLOCATION ENTRIES BASED ON THE REQUEST
    // ------------------------------------------------------------------------------------------------------
    if ((payload.allocationToBeUpdated || payload.newAllocationToBeCreated) && payload.mark_allocation.bander_id) {
      let allocationUpdate = await tx.mark_allocation.insert(payload.mark_allocation);
      console.info(`[INFO] SAVED: ${JSON.stringify(allocationUpdate)}`);
    }
    if (payload.stateToBeUpdated) {
      let stateUpdate = await tx.mark_state.insert(payload.mark_state);
      console.info(`[INFO] SAVED: ${JSON.stringify(stateUpdate)}`);
    }
    
    // ------------------------------------------------
    // 3) UPDATE THE MARK_STATE/ALLOCATION TIMELINES
    // ------------------------------------------------
    // Note: do this regardless of content of update -> ensures fresh current state of marks reflected by database
    if (event.resource.includes('mark-events-batch')) {
      // ---------------------------
      let resetCurrentMarkStateAndAllocations = await tx.rw_update_latest_mark_allocation_and_state_by_event_id([payload.event.id]);
      console.info(`[INFO] RESET STATE AND ALLOCATION: ${JSON.stringify(resetCurrentMarkStateAndAllocations)}`);
    }
    else {
      if (payload.allocationToBeUpdated) {
        await tx.mark_allocation.update(
          { 
            'or': [{ 'mark_id =': payload.mark_allocation.mark_id }]
          },
          {
            'is_current': false
          });
      }
      if (payload.stateToBeUpdated) {
        await tx.mark_state.update(
          { 
            'or': [{ 'mark_id =': payload.mark_state.mark_id }]
          },
          {
            'is_current': false
          });
      }
      let resetCurrentMarkStateAndAllocations = await tx.rw_update_latest_mark_allocation_and_state([[lookupData.mark.id]]);
      console.info(`[INFO] RESET STATE AND ALLOCATION: ${JSON.stringify(resetCurrentMarkStateAndAllocations)}`);
    }

    // If required, update all stock tallies for banders/marks affected
    console.log(JSON.stringify(lookupData));
    console.log(lookupData.prefixNumbersToUpdate);
    let prefixes_to_update = (event.resource.includes('mark-events-batch')) ? lookupData.vw_distinct_prefix_numbers_by_event.prefix_numbers.split(',') : [lookupData.mark.prefix_number];
    console.log(`prefixes to update: ${prefixes_to_update}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixes_to_update.length > 0) {
      // -----------------------------------
      let banderStockUpdate = await tx.rw_update_bander_stock_by_prefix([ process.env.BANDING_OFFICE_ID, prefixes_to_update ]);
      let bandingOfficeStockUpdate = await tx.rw_update_banding_office_stock([ [process.env.BANDING_OFFICE_ID], prefixes_to_update ]);
      console.log(`[INFO] STOCK UPDATES: ${JSON.stringify(banderStockUpdate)} ${JSON.stringify(bandingOfficeStockUpdate)}`);
    }

    return payload;
  })
}

const getUpdateFromDB = async (db, payload, lookupData, event) => {
  // ----------------------------------------------------------------------------   
  console.info('.getUpdateFromDB');

  let returnSet = null;

  if (event.resource.includes('mark-events-batch')) {
    // ---------------------------
    let eventIdOperation = `event_id =`;

    returnSet = await db.vw_mark_event_summary.find({
      [eventIdOperation]: lookupData.id
    });
  }
  else {
    // Return the new mark detail
    let vwName = 'vw_mark_history';
    let parentEventIdOperation = `${vwName}.event_id =`;
    let childEventIdOperation = `vw_mark_event_summary.event_id =`
    let allocationIdOperation = `${vwName}.ma_mark_id =`;
    let allocationNullOperation = `${vwName}.ma_mark_id is`;
    let stateIdOperation = `${vwName}.ms_mark_id =`;
    let stateNullOperation = `${vwName}.ms_mark_id is`;  

    returnSet = await db.vw_mark_history
    .join({
      pk: 'event_id',
      vw_mark_event_summary: {
        type: 'LEFT',
        pk: 'event_id',
        on: { [`event_id`]: `${vwName}.event_id` },
        decomposeTo: 'object',
      }
    })
    .find({
      and: [
        {
          or: [
            { [allocationIdOperation]: lookupData.mark.id },
            { [allocationNullOperation]: null }
          ]
        },
        {
          or: [
            { [stateIdOperation]: lookupData.mark.id },
            { [stateNullOperation]: null }
          ]
        },
        {
          [parentEventIdOperation]: payload.event.id,
          [childEventIdOperation]: payload.event.id,
        }
      ]
    });
  }

  console.log(returnSet);
  return returnSet[0];
}

const deleteMarkEventFromDB = (db, event, lookupData) => {
  // ----------------------------------------------------------------------------   
  console.info('.deleteMarkEventFromDB()');

  let eventId = event.pathParameters.eventId;

  return db.withTransaction(async tx => {
    // -----------------------------------

    // ------------------------------------------------------------------------------------------------------
    // 1) CLEAR THE EVENT'S MARK_STATE AND MARK_ALLOCATIONS
    // ------------------------------------------------------------------------------------------------------
    let markStateClearout = await tx.mark_state.destroy({
      'event_id =': eventId
    });
    console.log(JSON.stringify(markStateClearout));
    let markAllocationClearout = await tx.mark_allocation.destroy({
      'event_id =': eventId
    });


    // ----------------------------------
    // 2) CLEAR THE EVENT ITSELF
    // ----------------------------------
    let eventClearout = await tx.event.destroy({
      'id =': eventId
    });

    // ------------------------------------------------------------------------------------------------------
    // 3) Reset the mark_state and allocations for any marks whose timelines are alterred as a result of the deletion
    // ------------------------------------------------------------------------------------------------------
    let resetCurrentMarkStateAndAllocations = await tx.rw_update_latest_mark_allocation_and_state([markStateClearout.map(mark_state => mark_state.mark_id )]);
    console.info(`[INFO] RESET STATE AND ALLOCATION: ${JSON.stringify(resetCurrentMarkStateAndAllocations)}`);

    // ------------------------------------------------------------------------------------------------------
    // 4) Update the mark state rollup (to port to bird event delete as well)
    // ------------------------------------------------------------------------------------------------------
    // If required, update all stock tallies for banders/marks affected
    console.log(lookupData.vw_distinct_prefix_numbers_by_event.prefix_numbers.split(','));
    let prefixes_to_update = lookupData.vw_distinct_prefix_numbers_by_event.prefix_numbers.split(',');
    console.log(`prefixes to update: ${prefixes_to_update}`);
    console.log(`banding office id: ${process.env.BANDING_OFFICE_ID}`);
    if (prefixes_to_update.length > 0) {
      // -----------------------------------
      let banderStockUpdate = await tx.rw_update_bander_stock_by_prefix([ process.env.BANDING_OFFICE_ID, prefixes_to_update ]);
      let bandingOfficeStockUpdate = await tx.rw_update_banding_office_stock([ [process.env.BANDING_OFFICE_ID], prefixes_to_update ]);
      console.log(`[INFO] STOCK UPDATES: ${JSON.stringify(banderStockUpdate)} ${JSON.stringify(bandingOfficeStockUpdate)}`);
    }

    return {
      events: eventClearout,
      mark_states: markStateClearout,
      mark_allocations: markAllocationClearout
    }
  });
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
      return validateIsAdmin(governingCognitoGroup, customErrorFactory);
    })
    // Handle errors / Read workbook
    .then(errors => {
      if (errors.length > 0) {
        response.errors = errors;
        throw new BoilerPlate.ForbiddenError(`${errors.length} Authorisation validation error(s)!`, errors);
      }
      return validateEventExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Event not found', error);
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
      return getPutLookupData(customErrorFactory, event, claims, governingCognitoGroup, payload);
    })
    .then(res => {
      lookupData = res;
      return validatePutBusinessRules(customErrorFactory, event, lookupData, claims, governingCognitoGroup, payload);
    })
    .then(errors => {
      response.errors = [...response.errors, ...errors];
      if (errors.filter(error => error.severity === 'CRITICAL').length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload business validation error(s)!`, errors);
      }
      return generatePutDbPayload(db, payload, lookupData, response.errors.filter(error => error.severity === 'WARNING'));
    })
    .then(res => {
      return putEventToDB(db, res, lookupData, event);
    })
    .then(res => {
      return getUpdateFromDB(db, res, lookupData, event)
    })
    .then(res => {
      response.data = res;
      console.log(JSON.stringify(res));

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

// PUT BATCH
module.exports.putBatch = (event, context, cb) => {
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
      return validateIsAdmin(governingCognitoGroup, customErrorFactory);
    })
    // Handle errors / Read workbook
    .then(errors => {
      if (errors.length > 0) {
        response.errors = errors;
        throw new BoilerPlate.ForbiddenError(`${errors.length} Authorisation validation error(s)!`, errors);
      }
      return validateEventExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Event not found', error);
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
      return getPutLookupData(customErrorFactory, event, claims, governingCognitoGroup, payload);
    })
    .then(res => {
      lookupData = res;
      return validatePutBatchBusinessRules(customErrorFactory, event, lookupData, claims, governingCognitoGroup, payload);
    })
    .then(errors => {
      response.errors = [...response.errors, ...errors];
      if (errors.filter(error => error.severity === 'CRITICAL').length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload business validation error(s)!`, errors);
      }
      return generateBatchPutDbPayload(db, payload, lookupData);
    })
    .then(res => {
      return putEventToDB(db, res, lookupData, event);
    })
    .then(res => {
      return getUpdateFromDB(db, res, lookupData, event)
    })
    .then(res => {
      response.data = res;
      console.log(JSON.stringify(res));

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
module.exports.postBatch = (event, context, cb) => {
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

  // Lookup Data
  var lookupData = null;
  
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
      return validateIsAdmin(governingCognitoGroup, customErrorFactory);
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
      return getPostLookupData(customErrorFactory, event, claims, governingCognitoGroup, payload);
    })
    .then(res => {
      lookupData = res;
      console.info('Validating business rules...');
      return validatePostBusinessRules(customErrorFactory, db, event, lookupData, claims, governingCognitoGroup);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      return generateBulkMarkEvent(event, claims, payload);
    })
    .then(res => {
      return postBulkMarkEventToDB(db, res, lookupData);
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

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;

  let lookupData = null;

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
      return validateIsAdmin(governingCognitoGroup, customErrorFactory);
    })
    // Handle errors / Read workbook
    .then(errors => {
      if (errors.length > 0) {
        response.errors = errors;
        throw new BoilerPlate.ForbiddenError(`${errors.length} Authorisation validation error(s)!`, errors);
      }
      return validateEventExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      return getDeleteLookupData(customErrorFactory, event, claims, governingCognitoGroup);
    })
    .then(res => {
      lookupData = res;
      return validateDeleteBusinessRules(customErrorFactory, db, lookupData);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Business validation error(s)!`, errors);
      }
      return deleteMarkEventFromDB(db, event, lookupData);
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
