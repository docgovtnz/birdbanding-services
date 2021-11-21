
const Moment = require('moment');
const BBSSHelpers = require('bb-spreadsheet-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');

const Helpers = require('helpers');

// Sometimes for debugging purposes -> using this file (and importing directly into the serverless function)
// Worked best. Once debugged the methods could be migrated simply to the business validation lambda layer

// The idea is that this file gets migrated in to the business validation and errors layer

// Keeping this available for now so that we can debug business validation further in the future

// TODO REFACTOR ALL OF THESE METHODS INTO A LAMBDA LAYER! ONLY HERE TO ALLOW EASE OF DEBUGGING AND REDUCTION OF DEPLOYMENT LIFECYCLE DURATION

const getLookupData = (db, pathParams, rawEventBatch) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info("getLookupData()");

    console.info('Raw event batch for lookup: ', JSON.stringify(rawEventBatch));

    var results = {};

    // Construct prerequisite query criteria for the lookups
    let criteriaMarksToLookup = [];
    let joinCriteriaMarksToLookup = [];
    let criteriaSpeciesToLookup = [];
    let criteriaBandersToLookup = [];
    let duplicateCriteriaToLookup = [];

    rawEventBatch.events.forEach(event => {
      // --------------------------
      //  MARK / BIRD
      // --------------------------
      //  - Lookup to validate specific marks and bird associations
      //  - Complex validations beyond what may be possible client side
      //  - Required for both spreadsheet upload (ss) and json
      if (BBSSHelpers.IsBirdEvent(event.event.event_type)) {
        // -------------------------------------------------
        // IF A BIRD EVENT -> Process the mark_configuration
        event.mark_configuration.forEach(mark => {
          // If the alphanumeric text for the mark can be split on a '-' and results in 2 items
          // We want to search the database to see if this mark exists in inventory
          if ('alphanumeric_text' in mark && mark.alphanumeric_text && mark.alphanumeric_text.split('-').length === 2) {
            criteriaMarksToLookup.push({ prefix_number: mark.alphanumeric_text.split('-')[0], short_number: mark.alphanumeric_text.split('-')[1] });
            joinCriteriaMarksToLookup.push({ 'mark.prefix_number =': mark.alphanumeric_text.split('-')[0], 'mark.short_number =': mark.alphanumeric_text.split('-')[1] });
          }
        });
      }
      else if (BBSSHelpers.IsTransferEvent(event.event.event_type)) {
        // -------------------------------------------------
        // Included in lookup for a transfer event
        // NOTE: only available via JSON event endpoint
        event.mark_allocation.forEach(markAllocation => {
          joinCriteriaMarksToLookup.push({ 'mark.id =': markAllocation.mark_id });
        });
      }

      // --------------------------
      //  BIRD -> SPECIES
      // --------------------------
      //  - Only require a species search where there is no frontend validations 
      //   (i.e. spreadsheet only)
      if (BBSSHelpers.IsBirdEvent(event.event.event_type) && 'bird' in event && typeof event.bird.raw_species_code_nznbbs !== 'undefined' && event.bird.raw_species_code_nznbbs) {
        // IF A BIRD EVENT -> Process the mark_configuration
        criteriaSpeciesToLookup.push({ species_code_nznbbs: event.bird.raw_species_code_nznbbs });
      }

      // --------------------------
      //  BANDERS
      // --------------------------
      //  - Need to alter the criteria depending on whether this is coming direct from JSON or from a spredsheet
      //    In the case of JSON, we will only be dealing with certified banders,
      //    In the case of Spreadsheet, we will given a certification number to lookup on
      // -> EVENT PROVIDER
        switch(event.metadata.type) {
          case 'json': {
            criteriaBandersToLookup.push({ 'bander.id =': event.event.event_provider_id })
            if (BBSSHelpers.IsTransferEvent(event.event.event_type)) {
              // -------------------------------------------------
              // Included in lookup for a transfer event
              // NOTE: only available via JSON event endpoint
              event.mark_allocation.forEach(markAllocation => {
                criteriaBandersToLookup.push({ 'bander.id =': markAllocation.bander_id })
              });
            }
            break;
          }
          case 'ss': {
            if (event.event.raw_event_provider_nznbbs_certification_number) {
              criteriaBandersToLookup.push({ 'bander.nznbbs_certification_number =': event.event.raw_event_provider_nznbbs_certification_number })
              break;
            }
          }
        }
      // -> EVENT REPORTER
        switch(event.metadata.type) {
          case 'json': {
            criteriaBandersToLookup.push({ 'bander.id = ': event.event.event_reporter_id })
            break;
          }
          case 'ss': {
            if (event.event.raw_event_reporter_nznbbs_certification_number) {
              criteriaBandersToLookup.push({ 'bander.nznbbs_certification_number = ': event.event.raw_event_reporter_nznbbs_certification_number })
              break;
            }
          }
        }

      // --------------------------
      //  DUPLICATE
      // --------------------------
      let duplicateCheck = {
        'event.event_type = ': event.event.event_type,
        'event.event_timestamp = ': (Moment(event.event.event_timestamp, [Moment.ISO_8601]).isValid()) ? event.event.event_timestamp : null,
        'event.event_provider_id = ': (typeof event.event.event_provider_id !== 'undefined' && event.event.event_provider_id) ? event.event.event_provider_id : null,
        'event.event_reporter_id = ': (typeof event.event.event_reporter_id !== 'undefined' && event.event.event_reporter_id) ? event.event.event_reporter_id : null,
        'bird.id = ': (typeof event.bird.id !== 'undefined' && event.bird.id) ? event.bird.id : null,
        'event.latitude = ': (event.event.latitude && typeof event.event.latitude === 'number') ? event.event.latitude : null,
        'event.longitude = ': (event.event.longitude && typeof event.event.longitude === 'number') ? event.event.longitude : null,
        'event.user_northing = ': (event.event.user_northing && typeof event.event.user_northing === 'number') ? event.event.user_northing : null,
        'event.user_easting = ': (event.event.user_easting && typeof event.event.user_easting === 'number') ? event.event.user_easting : null
      }

      Object.keys(duplicateCheck).forEach(key => {
        if (!duplicateCheck[key]) {
          delete duplicateCheck[key];
        }
      });

      duplicateCriteriaToLookup.push(duplicateCheck);
    });



    // **********************************
    //  GET THE LOOKUP DATA FROM THE DB
    // **********************************
    // Characteristic Measurement
    // -------------------------------
    return db.characteristic.find({ 'id >': 0 })
      .then(res => {
        results.characteristics = res;
        // Project
        // -------------------------------
        return db.project
          .join({
            project_bander_membership: {
              type: 'INNER',
              pk: 'id',
              on: { 'project_id': 'project.id' },
              bander: {
                type: 'INNER',
                pk: 'id',
                on: { 'id': 'project_bander_membership.bander_id' },
              },
            }
          })
          .find({ 'id =': pathParams.projectId });
      })
      .then(res => {
        results.project = res;
        // Mark
        // -------------------------------
        let markCriteria = (joinCriteriaMarksToLookup.length > 0) ? { 'or': joinCriteriaMarksToLookup } : {};
        console.info('Mark lookup criteria: ', JSON.stringify(markCriteria));
        return Helpers.isEmptyObject(markCriteria) ? 
                    [] : 
                    db.mark
                      .join({
                        mark_state: {
                          type: 'LEFT OUTER',
                          pk: 'id',
                          on: { 'mark_id': 'mark.id' },
                          event: {
                            type: 'LEFT OUTER',
                            pk: 'id',
                            on: { 'id': 'mark_state.event_id' },
                            decomposeTo: 'object',
                            mark_allocation: {
                              type: 'LEFT OUTER',
                              pk: 'id',
                              on: { 'event_id': 'event.id' },
                              decomposeTo: 'object'
                            },
                          },
                        },
                        pk: 'id'
                      })
                      .find(markCriteria);
      })
      .then(res => {
        results.marks = res;
        // Bird
        // -------------------------------
        let birdCriteria = (joinCriteriaMarksToLookup.length > 0) ? { 'or': joinCriteriaMarksToLookup } : {};
        console.info('Bird lookup criteria: ', JSON.stringify(birdCriteria));
        return Helpers.isEmptyObject(birdCriteria) ? 
                        [] :
                        db.bird
                          .join({
                            event: {
                              type: 'INNER',
                              pk: 'id',
                              on: { 'bird_id': 'bird.id' },
                              mark_configuration: {
                                type: 'INNER',
                                pk: 'id',
                                on: { 'event_id': 'event.id' },
                                mark: {
                                  type: 'INNER',
                                  pk: 'id',
                                  on: { 'id': 'mark_configuration.mark_id' }
                                }
                              },
                              characteristic_measurement: {
                                type: 'INNER',
                                pk: 'id',
                                on: { 'event_id': 'event.id' },
                                characteristic: {
                                  type: 'INNER',
                                  pk: 'id',
                                  on: { 'id': 'characteristic_measurement.characteristic_id' },
                                }
                              },
                            },
                            species: {
                              type: 'INNER',
                              pk: 'id',
                              on: { 'id': 'bird.species_id' },
                              decomposeTo: 'object',
                              species_group_membership: {
                                type: 'INNER',
                                pk: 'id',
                                on: { 'species_id': 'species.id' },
                                decomposeTo: 'object',
                                species_group: {
                                  type: 'INNER',
                                  pk: 'id',
                                  on: { 'id': 'species_group_membership.group_id' },
                                  decomposeTo: 'object',
                                }
                              }
                            },
                            pk: 'id'
                          })
                          .find(birdCriteria);
      })
      .then(res => {
        results.birds = res;
        let speciesCriteria = (criteriaSpeciesToLookup.length > 0) ? { 'or': criteriaSpeciesToLookup } : {};
        console.info('Species lookup criteria: ', JSON.stringify(speciesCriteria));
        return Helpers.isEmptyObject(speciesCriteria) ? 
                        [] :
                        db.species.find(speciesCriteria);
      })
      .then(res => {
        results.species = res;
        let bandersCriteria = (criteriaBandersToLookup.length > 0) ? { 'or': criteriaBandersToLookup } : {};
        console.info('Banders lookup criteria: ', JSON.stringify(bandersCriteria));
        return Helpers.isEmptyObject(bandersCriteria) ? 
                    [] :
                    db.bander
                      .join({
                        bander_certifications: {
                          type: 'LEFT OUTER',
                          pk: 'id',
                          on: { 'bander_id': 'bander.id' },
                          species_group: {
                            type: 'LEFT OUTER',
                            pk: 'id',
                            on: { 'id': 'bander_certifications.species_group_id' },
                          }
                        },
                        pk: 'id'
                      })
                      .find(bandersCriteria);
      })
      .then(res => {
        results.banders = res;
        let duplicateCriteria = (duplicateCriteriaToLookup.length > 0) ? { 'or': duplicateCriteriaToLookup } : {};
        console.info('Duplicate lookup criteria: ', JSON.stringify(duplicateCriteria));
        return Helpers.isEmptyObject(duplicateCriteria) ? 
                    [] :
                    db.event
                      .join({
                        bird: {
                          type: 'INNER',
                          pk: 'id',
                          on: { 'id': 'event.bird_id' }
                        },
                        pk: 'id'
                      })
                      .find(duplicateCriteria);
      })
      .then(res => {
        results.duplicateEventCandidates = res;
        // No other lookup data required at this stage 
        console.info('Lookup results: ', JSON.stringify(results));
        return resolve(results);
      })
      .catch(err => {
        return reject(err);
      });
  });
};

// Function to post-process an event object after populating from spreadsheet
const postProcessEventBatch = (eventBatch, lookupData) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info("Post-Processing event batch");

    var promises = [];

    eventBatch.events.forEach(event => {
      promises.push(postProcessIndividualEvent(event, lookupData));
    });

    Promise.all(promises)
      .then(res => {
        let postProcessedEventBatch = {
          events: Helpers.flattenArray(res)
        };
        return resolve(postProcessedEventBatch);
      })
      .catch(err => {
        return reject(err);
      });
  })
};

// Function to post-process an individual event
const postProcessIndividualEvent = (preProcessedEvent, lookupData) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    // This is our top-level object for insertion into the Datastore
    var eventData = {
      metadata: {
        ...preProcessedEvent.metadata
      }
    };

    postProcessBirdFromEvent(preProcessedEvent, lookupData)
      .then(bird => {
        console.info(`Post processed bird: ${JSON.stringify(bird)}`)
        eventData.bird = bird;
        preProcessedEvent.bird = bird;
        return postProcessEventFromEvent(preProcessedEvent, lookupData)
      })
      .then(event => {
        console.info(`Post processed event: ${JSON.stringify(event)}`)
        eventData.event = event;
        return postProcessCharacteristicMeasurementsFromEvent(preProcessedEvent, lookupData);
      })
      .then(characteristicMeasurements => {
        console.info(`Post processed characteristic measurements: ${JSON.stringify(characteristicMeasurements)}`)
        eventData.characteristic_measurements = characteristicMeasurements;
        return postProcessMarkConfigurationFromEvent(preProcessedEvent, lookupData);
      })
      .then(markConfiguration => {
        console.info(`Post processed mark configuration: ${JSON.stringify(markConfiguration)}`)
        eventData.mark_configuration = markConfiguration;
        return postProcessMarkStatesFromEvent(preProcessedEvent, lookupData);
      })
      .then(markStates => {
        console.info(`Post processed mark states: ${JSON.stringify(markStates)}`)
        eventData.mark_state = markStates;
        return postProcessMarkAllocationFromEvent(preProcessedEvent, lookupData);
      })
      .then(markAllocation => {
        console.info(`Post processed mark allocation: ${JSON.stringify(markAllocation)}`)
        eventData.mark_allocation = markAllocation;
        console.info(`Post processed individual event: ${JSON.stringify(eventData)}`)
        return resolve(eventData);
      })
      .catch(err => {
        return reject(err);
      });

  });
}

// Function to populate Bird from the preprocessed spreadsheet row event
const postProcessBirdFromEvent = (event, lookupData) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info("Post-Processing event bird");

    try {
        // ------------------------------------
        // BIRD EVENTS
        if (BBSSHelpers.IsBirdEvent(event.event.event_type)) {
        // Properties
        var bird = Object.assign({}, event.bird);

        // FOR SPREADSHEET - Adding bird species code according to what has been entered into the spreadsheet
        if (event.metadata.type === 'ss' && lookupData.species.length > 0) {
          let species = lookupData.species.find(obj => { return obj.species_code_nznbbs === event.bird.raw_species_code_nznbbs; });
          if (typeof species === 'undefined' || !species) {
            console.error(`Unknown species code ${event.bird.raw_species_code_nznbbs}!`);
          }
          if (typeof species !== 'undefined') {
            bird.species_id = species.id;
            bird.valid_band_prefixes = species.valid_band_prefixes;
          }
        }

        // FOR BOTH SPREADSHEET AND JSON - Adding bird.id if this bird already exists
        if (lookupData.birds.length > 0) {
          // Map over mark_configuration and extract alphanumeric text which could be a band identifier
          let cardinal_mark_configuration = BBSSHelpers.JoinIdsToMarks(event.mark_configuration, lookupData).filter(markConfig => {
            return (typeof markConfig.mark_id !== 'undefined' && markConfig.mark_id);
          });

          let lookupMarks = lookupData.marks.filter(lookupMark => {
            return (cardinal_mark_configuration.some(markConfig => {
              return markConfig.mark_id === lookupMark.id;
            }));
          })

          let lookupBirds = lookupMarks.reduce((arrayBuilder, lookupMark) => {
            // -------------------------------------------------------------------
            // For each mark that gets joined we want to confirm that all birdIds match
            // Otherwise we are joining birdIds incorrectly
            lookupMark.mark_state.forEach(lookupMarkState => {
              if (lookupMarkState.state === 'ATTACHED' && typeof lookupMarkState.event.bird_id !== 'undefined' && lookupMarkState.event.bird_id) {
                // If we find any mark_state history where a cardinal band is attached to a bird and we have the bird_id at hand,
                //  add this to the lookup birds array
                arrayBuilder.push(lookupMarkState.event.bird_id);
              }
            });
            return arrayBuilder;
          }, []);

          // Filter out any duplicates
          lookupBirds = [... new Set(lookupBirds)];

          if (lookupBirds.length > 1) {
            // ---------------------
            // More than one bird_id found... this is an error so do not assign bird_id
            // Instead store array of bird_ids for later return in an error checking method
            console.info('Mulitple birds found, raw bird lookup for error handling');
            bird.raw_lookup_birds = lookupBirds;
          }
          else if (lookupBirds.length === 1) {
            // ---------------------
            // Ideal case -> we have found one bird_id to match this record
            // Assign the bird_id to link up bird chronology
            console.info('Single bird found, adding bird id');
            bird.id = lookupBirds[0];
          }
          else {
            // In this case, we expected to find a bird_id but haven't
            // This means a new bird needs to be created for this record
            console.info('Bird not found, new bird will be created if this record is successfully validated...');
          }
        }
        return resolve(bird);
      }
      // ------------------------------------
      // STOCK EVENTS
      else {
        // Need to resolve in case of a stock record but return a blank object
        // Aiming for an immutable data structure for all events
        return resolve({});
      }
    }
    catch (err) {
      return reject(err);
    }
  });
}

// Function to populate Event from the preprocessed spreadsheet row event
const postProcessEventFromEvent = (event, lookupData) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info("Post-Processing event event");

    try {
      // Properties
      var postProcessedEvent = Object.assign({}, event.event);

      // If a bird_id has been assigned to the bird subobject, assign this to the event also
      if (typeof event.bird.id !== 'undefined' && event.bird.id) {
        postProcessedEvent.bird_id = event.bird.id;
      }

      // FOR SPREADSHEET - If the event_reporter is not provided but the event provider is, assign the provider as the reporter
      if (event.metadata.type === 'ss' && (typeof event.event.raw_event_reporter_nznbbs_certification_number === 'undefined' || !event.event.raw_event_reporter_nznbbs_certification_number) &&
        typeof event.event.raw_event_provider_nznbbs_certification_number !== 'undefined' &&
        event.event.raw_event_provider_nznbbs_certification_number) {
        event.event.raw_event_reporter_nznbbs_certification_number = event.event.raw_event_provider_nznbbs_certification_number;
      }

      // FOR SPREADSHEET - Adding event_reporter and event_provider details based on lookupData
      if (event.metadata.type === 'ss' && lookupData.banders.length > 0) {
        // ------------------------------------------------------
        let providerId = lookupData.banders.find(obj => { return obj.nznbbs_certification_number === event.event.raw_event_provider_nznbbs_certification_number; });
        // ----------------------------------
        if (typeof providerId === 'undefined' || !providerId) {
          console.error(`Unknown event provider certification number ${event.event.raw_event_provider_nznbbs_certification_number}!`);
        }
        else {
          postProcessedEvent.event_provider_id = providerId.id;
        }

        let reporterId = lookupData.banders.find(obj => { return obj.nznbbs_certification_number === event.event.raw_event_reporter_nznbbs_certification_number; });
        if (typeof reporterId === 'undefined' || !reporterId) {
          console.error(`Unknown event reporter certification number ${event.event.raw_event_reporter_nznbbs_certification_number}!`);
        }
        else {
          postProcessedEvent.event_reporter_id = reporterId.id;
        }
      }

      // FOR SPREADSHEET - Add event_owner_id
      // This is the either the owner of the DOC marks being used (if two, this is the first owner that is found)
      // If no DOC stock is involved, the event_owner_id = event_provider_id (who should be an L3)
      // ----------------------------
      // If it's a bird event, derive the event_owner_id from the mark_config
      if (event.metadata.type === 'ss' && BBSSHelpers.IsBirdEvent(event.event.event_type)) {
        // --------------------------------------------------
        let markConfigurationEventOwnerDetails = event.mark_configuration.reduce((event_owner_details, individualMark) => {
          // ----------------------------------------------------
          // Go through the mark_configuration and assess whether the mark is potentially DOC stock
          let markToAdd = Object.assign({}, individualMark);
          let markSplit = individualMark.alphanumeric_text ? individualMark.alphanumeric_text.split('-') : [];

          // If the mark can be split on a '-' attempt to lookup prefix/number
          if (individualMark.mark_material === 'METAL' && markSplit.length === 2) {
            let rawPrefixNumber = markSplit[0];
            let rawShortNumber = markSplit[1];
            // For these cases - get the mark.id from the lookup data 
            var mark = lookupData.marks.find(obj => (obj.prefix_number === rawPrefixNumber && obj.short_number === rawShortNumber));
            var mark_state = (typeof mark !== 'undefined') ? mark.mark_state.filter(markStateObj => (markStateObj.state === 'ALLOCATED'))
              .reduce((mostRecentMarkStateObj, markStateObj) => ((!mostRecentMarkStateObj || Moment(markStateObj).isAfter(Moment(mostRecentMarkStateObj))) ? markStateObj : mostRecentMarkStateObj), null)
              : null;
            // ----------------------------------
            if (typeof mark_state === 'undefined' || !mark_state) {
              console.info(`Mark allocation not found ${rawPrefixNumber}-${rawShortNumber}!`);
            }
            else if (typeof mark_state.event.mark_allocation !== 'undefined' && mark_state.event.mark_allocation && (!event_owner_details || Moment(mark.event.event_timestamp, [Moment.ISO_8601]).isAfter(Moment(event_owner_details.event_timestamp)))) {
              return ({ event_owner_id: mark_state.event.mark_allocation.bander_id, event_timestamp: mark_state.event.event_timestamp });
            }
          }
          return event_owner_details;
        }, null)
        postProcessedEvent.event_owner_id = (markConfigurationEventOwnerDetails && 'event_owner_id' in markConfigurationEventOwnerDetails && markConfigurationEventOwnerDetails['event_owner_id']) ? markConfigurationEventOwnerDetails['event_owner_id'] : null; // Get the event_owner_id key from the result
      }
      // FOR SPREADSHEET - Otherwise this is a stock event - need to derive owner from primary marks
      else if (event.metadata.type === 'ss') {
        // ----------------------------------------
        let markSplit = event.metadata.primaryMark ? event.metadata.primaryMark.split('-') : [];

        // If the mark can be split on a '-' attempt to lookup prefix/number
        if (markSplit.length === 2) {
          let rawPrefixNumber = markSplit[0];
          let rawShortNumber = markSplit[1];
          // For these cases - get the mark.id from the lookup data 
          var mark = lookupData.marks.find(obj => (obj.prefix_number === rawPrefixNumber && obj.short_number === rawShortNumber));
          var mark_state = (typeof mark !== 'undefined') ? mark.mark_state.filter(markStateObj => (markStateObj.state === 'ALLOCATED'))
            .reduce((mostRecentMarkStateObj, markStateObj) => ((!mostRecentMarkStateObj || Moment(markStateObj).isAfter(Moment(mostRecentMarkStateObj))) ? markStateObj : mostRecentMarkStateObj), null)
            : null;
          // ----------------------------------
          if (typeof mark_state === 'undefined' || !mark_state) {
            console.info(`Mark allocation not found ${rawPrefixNumber}-${rawShortNumber}!`);
          }
          else if (typeof mark_state.event.mark_allocation !== 'undefined' && mark_state.event.mark_allocation) {
            postProcessedEvent.event_owner_id = mark_state.event.mark_allocation.bander_id;
          }
        }
      }

      // If event_owner_id is still null after this process, assign to be the event_provider_id
      if (!postProcessedEvent.event_owner_id) {
        postProcessedEvent.event_owner_id = postProcessedEvent.event_provider_id;
      }

      // Provide DEFAULT WILD/CAPTIVE if this has not been provided
      postProcessedEvent.event_bird_situation = (BBSSHelpers.IsBirdEvent(postProcessedEvent.event_type) && !postProcessedEvent.event_bird_situation) ? 'WILD' : postProcessedEvent.event_bird_situation;

      return resolve(postProcessedEvent);
    }
    catch (err) {
      return reject(err);
    }

  });
}

// Function to populate Characteristic Measurements from the preprocessed spreadsheet row event
const postProcessCharacteristicMeasurementsFromEvent = (event, lookupData) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info("Post-Processing characteristics measurements");
    try {
        // ------------------------------------
        // BIRD EVENTS
        if (BBSSHelpers.IsBirdEvent(event.event.event_type)) {
          // ------------------------------------------------------
          var characteristic_measurements = event.characteristic_measurements.reduce((arrayBuilder, raw_characteristic) => {
            // For a spreadsheet upload it is more involved to postprocess characteristic measurements
            if (event.metadata.type === 'ss') {
              // FOR SPREADSHEET - post process event characteristics based on the lookup data
              //  -> Add characteristic.id and post process characteristic.value
              var characteristic = lookupData.characteristics.find(obj => BBSSHelpers.CleanseDetailString(obj.ss_source_name_) === raw_characteristic.raw_characteristic_name);
              // ----------------------------------
              if (typeof characteristic === 'undefined' || !characteristic) {
                console.error(`Unknown characteristic ${raw_characteristic.raw_characteristic_name}!`);
                arrayBuilder.push({ raw_characteristic_name: raw_characteristic.raw_characteristic_name, raw_value: raw_characteristic.raw_value });
                return arrayBuilder;
              }
              var value = null;
              switch (characteristic.datatype) {
                case 'NUMERIC': value = raw_characteristic.raw_value ? Number(raw_characteristic.raw_value) : null; break;
                case 'TEXT': value = raw_characteristic.raw_value ? String(raw_characteristic.raw_value) : null; break;
                case 'DATETIME': value = raw_characteristic.raw_value ? BBSSHelpers.TransformDateTime(raw_characteristic.raw_value) : null; break;
                default: value = raw_characteristic.raw_value;
              }
              // If this case is triggered, we recognise a characteristic but cannot derive its value
              if (typeof raw_characteristic.raw_value === 'undefined') {
                console.error(`Value undefined for ${raw_characteristic.raw_characteristic_name}, assigning null value`);
                arrayBuilder.push({ characteristic_id: characteristic.id, value: null });
              }
              else if (typeof value === 'undefined' || !value) {
                console.error(`Value could not be processed for ${raw_characteristic.raw_characteristic_name}, assigning raw value for validation!`);
                arrayBuilder.push({ characteristic_id: characteristic.id, value: raw_characteristic.raw_value });
              }
              else {
                arrayBuilder.push({ characteristic_id: characteristic.id, value });
              }
            }
            // For a JSON event batch - the process is much similar as we assume a significant amount of limitation/guidance is provided by the frontend
            else if (event.metadata.type === 'json') {
              arrayBuilder.push({ characteristic_id: raw_characteristic.characteristic_id, value: raw_characteristic.value });
            }
            return arrayBuilder;
          }, []);
          // TODO - If we need to massage the units, do so here.
          return resolve(characteristic_measurements);
        }
        // ------------------------------------
        // STOCK EVENTS
        else {
          // Need to resolve in case of a stock record but return a blank object
          // Aiming for an immutable data structure for all events
          return resolve([]);
        }
    }
    catch (err) {
      return reject(err);
    }

  });
}

// Function to populate Mark Configuration from the preprocessed spreadsheet row event
const postProcessMarkConfigurationFromEvent = (event, lookupData) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info("Post-Processing mark configuration");

    try {
      // ------------------------------------
      // BIRD EVENTS
      if (BBSSHelpers.IsBirdEvent(event.event.event_type)) {
        // -------------------------------------------------------
        // Properties
        var mark_configuration = BBSSHelpers.JoinIdsToMarks(event.mark_configuration, lookupData);
        return resolve(mark_configuration);
      }
      // ------------------------------------
      // STOCK EVENTS
      else {
        return resolve([]);
      }
    }
    catch (err) {
      return reject(err);
    }
  });
}

// Function to populate the Mark State from the preprocessed spreadsheet row event
const postProcessMarkStatesFromEvent = (event, lookupData) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info("Post-Processing mark states");

    try {
      // ------------------------------------
      // BIRD EVENTS
      if (BBSSHelpers.IsBirdEvent(event.event.event_type)) {
        // ---------------------------------------
        // This line adds the mark_id's for each of the tracked bands from the mark_configuration for the event_type
        var eventMarkConfig = BBSSHelpers.JoinIdsToMarks(event.mark_configuration, lookupData);

        // This line adds the mark_id's for pre-event mark_configuration 
        // (i.e. which bands were on this bird that we track, prior to the change defined by this event)
        // This allows us to do a DIFF between before/after to establish any detached bands
        var preEventMarkConfig = BBSSHelpers.JoinIdsToMarks(event.mark_state, lookupData);

        // ---------------------------------------
        // For any inventory we track that is included within the mark_configuration,
        //  update the mark state according to the event type
        let mark_state = eventMarkConfig
          .filter(mark => ('mark_id' in mark && mark.mark_id))
          .map(mark => ({ 'mark_id': mark.mark_id, 'state': BBSSHelpers.TransformMarkState(event.event.event_type) }))

        // ---------------------------------------
        // For a bird event:
        // -> For any inventory we track, which is present within the raw pre-event mark_configuration 
        // (stored inside pre-processed mark_state),
        //  update the mark state according to the event type
        let removedMarks = preEventMarkConfig
          .filter(preEventMark => ('mark_id' in preEventMark))
          .filter(preEventTrackedMark => (typeof eventMarkConfig.find(eventMark => ('mark_id' in eventMark && preEventTrackedMark.mark_id === eventMark.mark_id)) === 'undefined'))
          .map(mark => ({ 'mark_id': mark.mark_id, 'state': 'DETACHED' }));

        mark_state = [...mark_state, ...removedMarks];

        return resolve(mark_state);
      }
      // ------------------------------------
      // STOCK EVENTS
      // Spreadsheet requires additional post-processing
      else if (event.metadata.type === 'ss') {
        // This line adds the mark_id's for each of the tracked bands from the mark_state for the event_type
        var eventMarkConfig = BBSSHelpers.JoinIdsToMarks(event.mark_state, lookupData);

        // ---------------------------------------
        // For any inventory we track that is included within the mark_state,
        //  update the mark state according to the event type
        let mark_state = eventMarkConfig
          .filter(mark => ('mark_id' in mark && mark.mark_id))
          .map(mark => ({ 'mark_id': mark.mark_id, 'state': BBSSHelpers.TransformMarkState(event.event.event_type) }))

        return resolve(mark_state);
      }
      // JSON does not require additional post-processing
      else {
        // ---------------------------------------
        // No change to the mark_state required because
        resolve(event.mark_state);
      }
    }
    catch (err) {
      return reject(err);
    }
  })
};

// Function to populate Mark Configuration from the preprocessed spreadsheet row event
const postProcessMarkAllocationFromEvent = (event, lookupData) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info("Post-Processing mark configuration");

    try {
      // ------------------------------------
      // BIRD EVENTS
      if (BBSSHelpers.IsBirdEvent(event.event.event_type)) {
        // -------------------------------------------------------
        return resolve([]);
      }
      // ------------------------------------
      // STOCK EVENTS
      else {
        return resolve(event.mark_allocation);
      }
    }
    catch (err) {
      return reject(err);
    }
  });
}

// Function to validate an event batch after post-processing
const validateEventBatch = (CustomErrors, eventBatch, lookupData, mappingSchema = null) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    let businessErrors = {};
    try {
      console.info("Validating event batch...");
      return BBBusinessValidationAndErrors.DuplicateValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors)
        .then(errors => {
          businessErrors = { ...errors };
          return BBBusinessValidationAndErrors.BanderCertificationValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors);
        })
        .then(errors => {
          businessErrors = { ...errors };
          return BBBusinessValidationAndErrors.MarkStateTimelineValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors);
        })
        .then(errors => {
          businessErrors = { ...errors };
          return BBBusinessValidationAndErrors.StockEventValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors);
        })
        .then(errors => {
          businessErrors = { ...errors };
          return BBBusinessValidationAndErrors.BirdEventValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors);
        })
        .then(errors => {
          businessErrors = { ...errors };
          return BBBusinessValidationAndErrors.BirdSubBirdValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors);
        })
        .then(errors => {
          businessErrors = { ...errors };
          return BBBusinessValidationAndErrors.BirdProjectValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors);
        })
        .then(errors => {
          businessErrors = { ...errors };
          return BBBusinessValidationAndErrors.BirdMarkValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors);
        })
        .then(errors => {
          businessErrors = { ...errors };
          return BBBusinessValidationAndErrors.BirdCharacteristicValidation(CustomErrors, eventBatch, lookupData, mappingSchema, businessErrors);
        })
        .then(errors => {
          businessErrors = { ...errors };
          return resolve(businessErrors);
        });
    }
    catch (err) {
      return reject(err);
    }
  })
};

// Function to format the API response and upload the batch the database if validated successfully
const formatResponseAndUploadToDB = (db, eventBatch) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info('Reviewing validation status of eventBatch prior to upload');

    let businessErrorKeys = Object.keys(eventBatch.business_errors);

    // Sort business errors into critical errors and warnings 
    //  this is so that we can make sensible decisions
    let warnings = [];
    let criticalErrors = [];
    businessErrorKeys.forEach(businessErrorRow => {
      Object.keys(eventBatch.business_errors[businessErrorRow]).forEach(businessErrorCol => {
        eventBatch.business_errors[businessErrorRow][businessErrorCol].forEach(error => {
          switch (error.criticality) {
            case 'WARN':
              warnings.push(error);
              break;
            case 'CRITICAL':
              criticalErrors.push(error);
              break;
            default:
              console.error('Error criticality not recognised');
              break;
          }
        })
      })
    });

    if ((typeof eventBatch.schema_errors !== 'undefined' && Object.keys(eventBatch.schema_errors).length > 0)
      || criticalErrors.length > 0) {
      console.info('Errors found - skipping upload for now');
      return resolve(eventBatch);
    }

    console.info('No errors, uploading and returning event batch...');

    // Prepare a transaction for each event to be uploaded to the database
    // Upload all first markings first (in case we can add bird_ids that are not generated until these are added...)
    return eventBatch.events.reduce((previousPromise, event, index) => {
      // Run the promises sequentially and pass each result to the subsequent promise
      // (This allows us to update bird_ids for records uplaoded earlier in the process)

      // Remove all null props from the event subobjects
      event = Helpers.cleanNullPropsRecursive(event);

      return previousPromise.then(res => {
        return BBSSHelpers.UploadIndividualEvent(db, event, index, res);
      })
    }, Promise.resolve({ events: [], eventBatchBirdIdMarkRelations: {} }))
      .then(res => {
        console.info('Removing un-required keys from event batch');
        eventBatch.events = res.events;
        eventBatch.events.forEach(event => {
          // Remove each event metadata from response
          delete event.metadata;
          // If a bird event. remove the raw keys used in processing the bird subobject
          if (BBSSHelpers.IsBirdEvent(event.event.event_type)) {
            event.bird = Object.keys(event.bird)
              .filter(key => (!key.includes('raw_') && !key.includes('row_')))
              .reduce((updatedBird, filteredKey) => {
                return {
                  ...updatedBird,
                  [filteredKey]: event.bird[filteredKey]
                }
              }, {});

            event.characteristic_measurements = event.characteristic_measurements.map(characteristicMeasurement => Object.keys(characteristicMeasurement)
              .filter(key => (!key.includes('raw_') && !key.includes('row_')))
              .reduce((updatedCharacteristicMeasurement, filteredKey) => {
                  return {
                    ...updatedCharacteristicMeasurement,
                    [filteredKey]: characteristicMeasurement[filteredKey]
                  }
                }, {}));

            event.mark_configuration = event.mark_configuration.map(markConfig => Object.keys(markConfig)
              .filter(key => (!key.includes('raw_') && !key.includes('row_')))
              .reduce((updatedMarkConfig, filteredKey) => {
                  return {
                    ...updatedMarkConfig,
                    [filteredKey]: markConfig[filteredKey]
                  }
                }, {}));
          }
          // Remove raw keys from event and mark state subobjects
          event.event = Object.keys(event.event)
            .filter(key => (!key.includes('raw_') && !key.includes('row_')))
            .reduce((updatedEvent, filteredKey) => {
                return {
                  ...updatedEvent,
                  [filteredKey]: event.event[filteredKey]
                }
              }, {});

          event.mark_state = event.mark_state.map(markState => Object.keys(markState)
            .filter(key => (!key.includes('raw_') && !key.includes('row_')))
            .reduce((updatedMarkState, filteredKey) => {
                return {
                  ...updatedMarkState,
                  [filteredKey]: markState[filteredKey]
                }
              }, {}));
        })
        return resolve(eventBatch);
      });
  });
}

// =============
// EXPORTS
// =============

module.exports = {
  // FUNCTIONS
  getLookupData,
  postProcessEventBatch,
  postProcessIndividualEvent,
  postProcessBirdFromEvent,
  postProcessEventFromEvent,
  postProcessCharacteristicMeasurementsFromEvent,
  postProcessMarkConfigurationFromEvent,
  postProcessMarkStatesFromEvent,

  validateEventBatch,
  formatResponseAndUploadToDB
};