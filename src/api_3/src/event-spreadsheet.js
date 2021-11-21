'use strict';

const Promise = require('bluebird');
const AWS = require('aws-sdk');
const Util = require('util');
const Moment = require('moment');
const uuidv4 = require('uuid/v4');
const XLSX = require('xlsx');
var _ = require('lodash');

const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const Helpers = require('helpers');
const BBHelpers = require('bb-helpers');
const BBSSHelpers = require('bb-spreadsheet-helpers.js');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;

const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;
const getSpreadsheetLookupData = require('event-validation-lib').getSpreadsheetLookupData;
const validateSpreadsheetRowBusinessRules = require('event-validation-lib').validateSpreadsheetRowBusinessRules;

var AWSXRay = require('aws-xray-sdk');
const { result } = require('lodash');
const { stringify } = require('querystring');
const { supplementSpreadsheetEventsWithLookupData, validateIntraSpreadsheetBusinessRules } = require('event-validation-lib');
var AWSCaptureAWS = AWSXRay.captureAWS(require('aws-sdk'));
AWSCaptureAWS.config.setPromisesDependency(require('bluebird'));


// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const sheetBasePath = 'event-spreadsheet/'
const RESOURCE_NAME = "EVENT_SPREADSHEET";
const s3 = new AWS.S3({
  signatureVersion: 'v4'
});


const generateUserProjectCriteria = (projectList, prefix = '') => {
  // ----------------------------------------------------------------------------   
  return projectList.map(projectId => {
    return {
      [`${prefix}project_id =`]: projectId
    }
  });
}


// Validate spreadsheet exists
const validateUploadExists = async (customErrorFactory, db, event, claims) => {
  // ----------------------------------------------------------------------------   
  console.info(`${RESOURCE_NAME}.validateUploadExists()`);

  let uploadId = event.pathParameters.spreadsheetId;

  let uploadExistsResultset = await db.ro_is_bander_upload(uploadId);

  console.log(uploadExistsResultset);
  if (!uploadExistsResultset[0].ro_is_bander_upload) {
    return customErrorFactory.getError('NotFoundError', ['spreadsheetId', uploadId, 'pathParameters.spreadsheetId']);
  } 
  else {
    console.log('No errors');
    return null;
  }
}


// Validate spreadsheet access
const validateUploadAccess = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // ----------------------------------------------------------------------------   
  console.info(`${RESOURCE_NAME}.validateUploadAccess()`);

  // The Authorisation logic is fairly complex, we call a number of functions to capture each component
  //   of the process
  let uploadId = event.pathParameters.spreadsheetId;

  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
    return null;
  }

  let canViewUpload = await db.ro_can_view_upload(uploadId, claims.sub);

  console.log(canViewUpload);

  if (!canViewUpload[0].ro_can_view_upload) {
    return customErrorFactory.getError('ForbiddenError', [`accessing upload: /event-spreadsheets/${uploadId}`, claims.sub, 'claims.sub']);
  } 
  else {
    console.log('No errors');
    return null;
  }
}


const getDB = (db, event) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getDB()");

  let criteria = {};
  let idOperation = 'id =';
  criteria[idOperation] = event.pathParameters.spreadsheetId;

  // -----------------------------------
  console.log('Criteria: ', criteria);

  // THIS RETURNS A PROMISE
  return db.vw_bander_upload_detail.find(criteria);
}


// Update with search DB method including user access filtration
const searchDB = (db, event, claims, governingCognitoGroup, userProjectList) => {
  // ----------------------------------------------------------------------------   
  console.info(`${RESOURCE_NAME}.searchDB()`);

  let pathParameters = event.pathParameters;
  let queryStringParameters = event.queryStringParameters;
  let multiValueQueryStringParameters = event.multiValueQueryStringParameters;

  // Get the resource-name, remove the plurality and append 'Id'
  // let limit = ( queryStringParameters && queryStringParameters.limit ) ? parseInt(queryStringParameters.limit) : 500;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
                             parseInt(queryStringParameters.paginationToken) : null;
  let prev = null;
  // ------------------------------------
  // QUERY STRING ORDER
  // ------------------------------------
  // Set standard sort order
  let sortOrder = 'desc';

  // Check if a sort order has been requested
  if (queryStringParameters && 'order' in queryStringParameters && ['ASC', 'DESC'].includes(queryStringParameters.order)) {
    sortOrder = queryStringParameters.order.toLowerCase();
  }

  let criteria = {
    'upload_status !=': 'CRITICAL_FILE_FORMAT'
  }

  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    criteria.or = [{
      'bander_id =': claims.sub
    }];

    let userProjectCrtieria = generateUserProjectCriteria(userProjectList);
    if (userProjectCrtieria.length > 0) {
      console.log('User project list criteria: ', { 'or': userProjectCrtieria });
      criteria.or.push({ 'or': userProjectCrtieria });
    }
  }

  // Add a criteria which only shows spreadsheets from the last 3 months
  criteria['created_datetime >='] = Moment().subtract(3, 'months').format()

  let options = {
    // limit: limit, // LIMIT REMOVED TO PREVENT CONFUSION, WORK WITH PAGINATION IF REQUIRED DOWN THE LINE
    offset: (paginationToken) ? paginationToken : 0,
    order: [{ field: 'created_datetime', direction: sortOrder }]
  }

  console.log(JSON.stringify(criteria));

  return db.vw_bander_uploads
    .find(criteria, options);
}

// Add 
const addSearchCalculatedFields = async (db, rawResultSet, claims, governingCognitoGroup) => {
  // ----------------------------------------------------------------------------   
  let processedResultSet = await Promise.all(
    rawResultSet.map(async ssItem => {
      let presignedUrl = await getPresignedURL(ssItem.storage_host, ssItem.object_path, null);
      return {
        ...ssItem,
        url: presignedUrl
      }
    })
  );

  let countLastPeriod = (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) ? (await db.ro_admin_get_monthly_uploads())[0].ro_admin_get_monthly_uploads : (await db.ro_get_monthly_uploads(claims.sub))[0].ro_get_monthly_uploads;

  return {
    data: processedResultSet,
    count_last_period: countLastPeriod
  };
}


const completePostAction = async (db, event) => {
  // -------------------------------
  console.info(RESOURCE_NAME + '.completePostAction()');

  let queryStringParameters = event.queryStringParameters;

  // (A) If a presigned URL is all that is being requested, generate the presigned URL and return
  if (queryStringParameters 
    && 'presignedUrl' in queryStringParameters
    && queryStringParameters.presignedUrl.toLowerCase() === 'true') {
    // ---------------------------------------------------------------
    return putSignedUrl(event);
  }

  // Otherwise (B) complete a standard spreadsheet POST action
  return postUploadToDB(db, event);
}


const getPresignedURL = (bucket, key, version) => {
  // -----------------------------------------------
  console.info(RESOURCE_NAME + '.getPresignedURL()');

  // Generate a getObject presigned URL for S3
  let params = {
    Bucket: bucket,
    Key: key,
    VersionId: version,
    Expires: 86400
  }
  
  // Synchronous call to get signed url
  return s3.getSignedUrl('getObject', params);
}


const putSignedUrl = async (event) => {
  // ---------------------------------
  console.info(RESOURCE_NAME + '.putSignedUrl()');

  let payload = JSON.parse(event.body);

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: `${sheetBasePath}${payload.object_path}`,
    Expires: 86400
  };

  console.info('[INFO] presigned url request params: ', JSON.stringify(params));

  let url = await s3.getSignedUrl(`putObject`, params);

  return {
    ...payload,
    presignedUrl: url
  };
}


const postUploadToDB = async (db, event) => {
  // ---------------------------------
  console.info(RESOURCE_NAME + '.postUploadToDB()');

  let result = null;

  let payload = JSON.parse(event.body);

  payload.storage_host = process.env.USER_ASSETS_BUCKET;
  payload.upload_status = 'PENDING_RESULT';
  payload.object_path = `${sheetBasePath}${payload.object_path}`
  payload.no_of_rows = null; // Set to null for RAW upload as we haven't done any validation on the spreadsheet
  let banderUploadResultSet = await db.bander_uploads.insert(payload);

  console.log(banderUploadResultSet);

  let banderUpload = await db.vw_bander_upload_detail.find({
    'id =': banderUploadResultSet.id
  });

  console.log(banderUpload);

  result = banderUpload[0];

  result.url = await getPresignedURL(result.storage_host, result.object_path, null);
  result.created_datetime = result.row_creation_timestamp_;

  return result;
}


const validatePostBusinessRules = async (customErrorFactory, db, event, claims, governingCognitoGroup, userProjectList) => {
  // -----------------------------------------------------
  console.info(RESOURCE_NAME + '.validatePostBusinessRules()');

  let promises = [];
  let queryStringParameters = event.queryStringParameters;

  // -------------------------------------------------------
  // Validate object path does not already exist in the datastore
  // -------------------------------------------------------
  let objectExistenceValidationResult = await validateObjectDoesNotExistInDb(customErrorFactory, db, event);
  if (objectExistenceValidationResult) {
    return [objectExistenceValidationResult];
  }

  // -------------------------------------------------------
  // Validate bander able to upload to project
  // -------------------------------------------------------
  let banderProjectAccessValidationResult = await validateBanderProjectAccess(customErrorFactory, db, event, governingCognitoGroup, claims, userProjectList);
  if (banderProjectAccessValidationResult) {
    return [banderProjectAccessValidationResult];
  }

  // -------------------------------------------------------
  // Basic validation of file extension
  // -------------------------------------------------------
  let databoxFileExtensionValidationResult = await validateDataboxFileExtension(customErrorFactory, db, event);
  if (databoxFileExtensionValidationResult) {
    return [databoxFileExtensionValidationResult];
  }
  
  if (!queryStringParameters 
    || !'presignedUrl' in queryStringParameters) {
    // -------------------------------------------------------
    // Validate object path exists in S3
    // -------------------------------------------------------
    let validationObjectExistsInS3Result = await validateObjectExistsInS3(customErrorFactory, db, event);
    if (validationObjectExistsInS3Result) {
      return [validationObjectExistsInS3Result];
    }
  }

  return [];
}


const validatePutBusinessRules = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------------------
  console.info(RESOURCE_NAME + '.validatePutBusinessRules()');

  let promises = [];
  let queryStringParameters = event.queryStringParameters;

  if (!queryStringParameters 
    || !'presignedUrl' in queryStringParameters) {
    // -------------------------------------------------------
    // Validate object path exists in S3
    // -------------------------------------------------------
    promises.push(validateObjectExistsInS3(customErrorFactory, db, event));
  }

  let errors = await Promise.all(promises);

  return errors.filter(error => error);
}


const validateObjectExistsInS3 = async (customErrorFactory, db, event) => {
  // --------------------------------------------------------
  console.info(RESOURCE_NAME + '.validateObjectExistsInS3()');

  let payload = JSON.parse(event.body);

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: `${sheetBasePath}${payload.object_path}`
  };

  console.log(JSON.stringify(params));

  try {
    let isObjectPath = await s3.headObject(params).promise();
    console.log(isObjectPath);
    return null;
  }
  catch (err) {
    return customErrorFactory.getError('ObjectNotInS3', [payload.object_path, '/object_path']);
  }
}


const validateObjectDoesNotExistInDb = async (customErrorFactory, db, event) => {
  // --------------------------------------------------------
  console.info(RESOURCE_NAME + '.validateObjectDoesNotExistInDb()');

  let payload = JSON.parse(event.body)

  let isObjectResultSet = await db.ro_is_object_path(`${sheetBasePath}${payload.object_path}`);

  if (isObjectResultSet[0].ro_is_object_path) {
    return customErrorFactory.getError('ObjectPathAlreadyExists', [payload.object_path, '/object_path', ' in the datastore']);
  }
  return null
}


const validateBanderProjectAccess = (customErrorFactory, db, event, governingCognitoGroup, claims, userProjectList) => {
  // -----------------------------------------------------
  console.info(RESOURCE_NAME + '.validateBanderProjectAccess()');

  let projectId = event.pathParameters.projectId;

  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
    return null;
  }

  if (userProjectList.filter(projectListId => projectListId === projectId).length === 0) {
    return customErrorFactory.getError('ProjectAuthorisationError', [claims.sub, 'claims.sub']);
  }
  
  return null;
}


const validateDataboxFileExtension = (customErrorFactory, db, event) => {
  // -----------------------------------------------------
  console.info(RESOURCE_NAME + '.validateDataboxFileExtension()');

  let objectPath = JSON.parse(event.body).object_path;
  let objectPathSplit = objectPath.split('.');

  console.log(objectPathSplit);

  if (objectPathSplit && objectPathSplit[( objectPathSplit.length - 1 )] !== 'xlsx') {
    return customErrorFactory.getError('FileExtensionError', [objectPathSplit[( objectPathSplit.length - 1 )], 'extension', '.xlsx']);
  }

}


const queueSQSMessage = (body, delay = null) => {
  // ----------------------------------------------------------------------------    
  console.info(`${RESOURCE_NAME}.queueSQSMessage()`);

  var sqs = new AWS.SQS();

  let params = {
    MessageBody: JSON.stringify(body),
    QueueUrl: process.env.UPLOAD_QUEUE_URL,
  };

  if (delay) {
    params.DelaySeconds = delay;
  }

  return sqs.sendMessage(params).promise();
}


const getBodyFromS3 = async (object_path) => {
  console.info(`${RESOURCE_NAME}.getBodyFromS3()}`);

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: object_path
  }

  let getObj = await s3.getObject(params).promise();

  return getObj.Body;
}


const deleteFromS3 = async (object_path) => {
  console.info(`${RESOURCE_NAME}.deleteFromS3()}`);

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: object_path
  }

  let deleteObj = await s3.deleteObject(params).promise();

  return deleteObj;
}

// Utility function to get the header row from a worksheet
const get_header_row = (header_row_number, sheet) => {
  // ------------------------------------------
  let headers = [];
  let range = XLSX.utils.decode_range(sheet['!ref']);

  /* walk every column in the range */
  for(let col = range.s.c; col <= range.e.c; ++col) {
      let cell = sheet[XLSX.utils.encode_cell({c:col, r:header_row_number})] /* find the cell in the header row */

      let hdr = "UNKNOWN " + col; // <-- replace with your desired default 
      if(cell && cell.t) hdr = XLSX.utils.format_cell(cell);

      headers.push(hdr);
  }

  return headers;
}


// This function reads a workbook object from the passed data
const readWorkbook = (ssData, ssDataType, mapping) => {
  // ----------------------------------------------------------------------------
  return new Promise((resolve, reject) => {

    console.info("readWorkbook()");

    try {
      var json = {};
      var workbook = (XLSX.read(ssData, { type: ssDataType, cellFormula: false, cellHTML: false, cellText: true, raw: false, sheetRows: mapping.max_row_count }));
      json.version = workbook.Sheets[mapping.sheet_name][mapping.version_cell_ref].v;
      json.headers = get_header_row(mapping.header_row, workbook.Sheets[mapping.sheet_name]);
      json.rows = XLSX.utils.sheet_to_json(workbook.Sheets[mapping.sheet_name], { range: mapping.starting_row, raw: false, header: mapping.ordered_property_array, blankrows: true, defval: null });
      return resolve(json);
    }
    catch (err) {
      console.error(`Error reading spreadsheet: ${err}`);
      // Workbook cannot be read, return null to represent error reading workbook
      return resolve(null);
    }
  });
};


// Populate from a spreadsheet
const createRawEventBatchFromSpreadsheet = (sheetJson, projectId, mapping) => {
  // ----------------------------------------------------------------------------
  return new Promise((resolve, reject) => {

    console.info("createEventBatchFromSpreadsheet()");

    var eventBatch = {
      "events": []
    };

    var promises = [];

    let tempSkippedRows = [];
    let sumSkippedRows = [];

    console.info("Creating event batch from spreadsheet containing " + sheetJson.rows.length + " row(s)");

    // Iterate over all the rows in the passed worksheet object creating events as appropriate.
    // This is the row INDEX not the excel row NUMBER. Actual data starts on row 2 which is index 0.
    for (var rowIndex = 0; rowIndex < sheetJson.rows.length; rowIndex++) {

      // Grab a working copy
      let row = sheetJson.rows[rowIndex];

      // Skip Empty Rows
      // If any of the cell values are non-empty, the row is considered valid.
      if (!Object.values(row).some(x => (x !== null && x !== undefined && x !== '' && String(x).trim() !== ''))) {
        //console.warn("Spreadsheet row " + (rowIndex+3) + " appears to be empty. Skipping...");
        tempSkippedRows.push(rowIndex + mapping.starting_row + 1)
        continue;
      }

      // Skip Rows with instructional text
      // If the NZNBBS_Code column includes instructional text, i.e. invalid data, skip it
      if ((row.prefix_number) && String(row.prefix_number).toLowerCase().includes('input below this point will not be processed')) {
        console.warn("Spreadsheet row " + (rowIndex + mapping.starting_row + 1) + " Includes instructional text. Skipping...");
        continue;
      }

      // Skip Rows with stock codes
      // If the NZNBBS_Code column includes instructional text, i.e. invalid data, skip it
      if (row.mark_state_code && row.mark_state_code !== '') {
        console.warn("Spreadsheet row " + (rowIndex + mapping.starting_row + 1) + " Includes a mark state code. Skipping...");
        tempSkippedRows.push(rowIndex + mapping.starting_row + 1)
        continue;
      }

      // Row is legit. Give it a row index.
      row.rowIndex = rowIndex;
      // Handle update of skipped rows so that we can generate a warning message as required
      sumSkippedRows = [...sumSkippedRows, ...tempSkippedRows];
      tempSkippedRows = [];

      // First Pre-Process Mark Configurations. The aim of this is twofold:
      // 1: If data is present in both the incoming Mark Configuration and the
      // outgoing mark configuration, split into two rows.
      // 2: Regardless of whether we split the rows, normalise the "in_mark_config" or
      // "out_mark_config" properties to be just "mark_config".
      let rows = BBSSHelpers.PreProcessMarkConfigurations(row);

      // After normalising and/or splitting, the rows array contains between 1 and 2 rows.
      // We iterate over it, generating a new event for each and populating as appropriate.
      rows.forEach(row => {
        promises.push(createEventFromSpreadsheetRow(row, (rowIndex + mapping.starting_row), projectId));
      });
    }

    Promise.all(promises)
      .then(res => {
        eventBatch.events = Helpers.flattenArray(res);
        eventBatch.skipped = sumSkippedRows;
        return resolve(eventBatch);
      })
      .catch(err => {
        return reject(err);
      });
  });
};


// Populate from a spreadsheet
const createEventFromSpreadsheetRow = (row, ssRowIndex, projectId) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    console.info(JSON.stringify(row));

    // This is out top-level object for insertion into the Datastore
    var eventData = {
      metadata: {
        type: 'ss',
        eventIndex: ssRowIndex - 1,
        primaryMark: `${String(row.prefix_number).toLowerCase()}-${String(row.short_number).toLowerCase()}`
      }
    };

    // console.info("Populating Event from spreadsheet row " + (Number(row.rowIndex) + 2) + " (index: " + Number(row.rowIndex) + ")");

    // Populate the rest from the spreadsheet, then post-process
    // to create an 'final' version of the event in standard format.
    console.info(`Processing row bird: ${ssRowIndex + 1}`);
    populateBirdFromRow(row, projectId)
      .then(bird => {
        // If event relates to a bird i.e. not just a pure mark_state change,
        // Add a minimal bird object to the event 
        // (this will be post-processed to determine if a new bird needs to be created)
        eventData.bird = bird;
        console.info(`Processing row event: ${ssRowIndex + 1}`);
        return populateEventFromRow(row, projectId)
      })
      .then(event => {
        eventData.event = event;
        console.info(`Processing row characteristics: ${ssRowIndex + 1}`);
        return populateRawCharacteristicMeasurementsFromRow(row);
      })
      .then(res => {
        // As noted above, if event is related to a bird, add characteristic_measurements,
        // we will post-process this based on lookup data later
        eventData.characteristic_measurement = (!row.mark_state_code) ? res : [];
        console.info(`Processing row marks: ${ssRowIndex + 1}`);
        return populateMarkConfigurationFromRow(row);
      })
      .then(res => {
        // As noted above, if event is related to a bird, add mark_configuration
        eventData.mark_configuration = (!row.mark_state_code) ? res : [];
        return resolve(eventData);
      })
      .catch(err => {
        return reject(err);
      });
  })
};


// Function to create the top-level bird data that maps to the DB table BIRD
const populateBirdFromRow = (row, projectId) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    try {
      // Top Level Bird properties
      var bird = {
        raw_species_code_nznbbs: (row.species_code && row.species_code !== '') ? Number(row.species_code) : null,
        raw_species_scientific_name: (row.info_species_scientific_name && row.info_species_scientific_name !== '') ? row.info_species_scientific_name : null,
        raw_species_common_name: (row.info_species_common_name && row.info_species_common_name !== '') ? row.info_species_common_name : null
      };

      console.log(`Bird from row: ${JSON.stringify(bird)}`);
      return resolve(bird);
    }
    catch (err) {
      return reject(err);
    }
  })
};


// Function to create the top-level event data that maps to the DB table EVENT
const populateEventFromRow = (row, projectId) => {``
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    let useCoordinateSystem = 'WGS84';
    if (
          (!row.latitude || (!isNaN(Number(row.latitude) && Number(row.latitude) === 0)))
            && (!row.longitude || (!isNaN(Number(row.longitude) && Number(row.longitude) === 0)))
          && (row.user_northing && !isNaN(Number(row.user_northing)) && Number(row.user_northing) !== 0) 
            && (row.user_easting && !isNaN(Number(row.user_easting)) && Number(row.user_easting) !== 0)) {
      // ------------------------------------------------
      useCoordinateSystem = 'USER';
    }
    

    try {
      // Top Level Event properties
      var event = {
        // First, handle the main references
        project_id: projectId,
        event_type: ((row.mark_state_code !== '' && row.mark_state_code !== null) || (row.event_type !== '' && row.event_type !== null)) ? BBSSHelpers.TransformEventType(row.mark_state_code ? row.mark_state_code : row.event_type) : null,
        event_state: "AWAITING_REVIEW",
        event_banding_scheme: row.event_banding_scheme ? BBSSHelpers.TransformBandingSchemeCode(row.event_banding_scheme) : "NZ_NON_GAMEBIRD",
        event_timestamp: row.event_timestamp ? BBSSHelpers.TransformDateTime(row.event_timestamp, row.event_timestamp_accuracy) : null,
        event_timestamp_accuracy: row.event_timestamp_accuracy ? row.event_timestamp_accuracy.trim().toUpperCase() : 'D',
        event_capture_type: row.event_capture_type ? BBSSHelpers.TransformCaptureType(row.event_capture_type) : null,
        event_bird_situation: row.event_bird_situation ? BBSSHelpers.TransformEventSituation(row.event_bird_situation) : 'WILD',
        raw_event_reporter_nznbbs_certification_number: row.event_reporter_number ? String(row.event_reporter_number) : null,
        raw_event_reporter_name: row.info_event_reporter_name ? row.info_event_reporter_name : null,
        raw_event_provider_nznbbs_certification_number: row.event_provider_number ? String(row.event_provider_number) : null,
        raw_event_provider_name: row.info_event_provider_name ? row.info_event_provider_name : null,
        event_other_person_name: row.info_other_name,
        event_other_contact: row.info_other_contact,
        latitude: (row.latitude && !isNaN(Number(row.latitude)) && useCoordinateSystem !== 'USER') ? Number(row.latitude) : (row.latitude && !(!isNaN(Number(row.latitude)) && Number(row.latitude) === 0) ? row.latitude : null),
        longitude: (row.longitude && !isNaN(Number(row.longitude)) && useCoordinateSystem !== 'USER') ? Number(row.longitude) : (row.longitude && !(!isNaN(Number(row.longitude)) && Number(row.longitude) === 0) ? row.longitude : null),
        location_description: row.location_description ? String(row.location_description) : null,
        location_comment: row.location_comment ? BBSSHelpers.TransformLocationComment(String(row.location_comment)) : null,
        locality_general: row.locality_general ? String(row.locality_general) : null,
        locality_accuracy: (row.locality_accuracy && !isNaN(Number(row.locality_accuracy))) ? Number(row.locality_accuracy) : (row.locality_accuracy ? row.locality_accuracy : null),
        user_northing: (row.user_northing && !isNaN(Number(row.user_northing)) && (Number(row.user_northing) !== 0 || (!isNaN(Number(row.user_easting)) && Number(row.user_easting) !== 0))) ? Number(row.user_northing) : (row.user_northing && !(!isNaN(Number(row.user_northing)) && Number(row.user_northing) === 0) ? row.user_northing : null),
        user_easting: (row.user_easting && !isNaN(Number(row.user_easting)) && ((!isNaN(Number(row.user_northing)) && Number(row.user_northing) !== 0) || Number(row.user_easting) !== 0)) ? Number(row.user_easting) : (row.user_easting && !(!isNaN(Number(row.user_easting)) && Number(row.user_easting) === 0) ? row.user_easting : null),
        user_coordinate_system: row.user_coordinate_system ? String(row.user_coordinate_system) : 'WGS84',
        comments: row.info_notes ? row.info_notes : null
      };

      // Update event_timestamp if event is a post change event (add 1 second to preserve chronology with prechange event)
      if (event.event_type === 'IN_HAND_POST_CHANGE') {
        event.event_timestamp = Moment(event.event_timestamp).add(1, 'seconds').toISOString();
      }

      // Update reporter number to the provider number if not the reporter has not been provided (and the provider number has)
      event.raw_event_reporter_nznbbs_certification_number = (!event.raw_event_reporter_nznbbs_certification_number && row.event_provider_number) ? String(row.event_provider_number).padStart(4, '0') : event.raw_event_reporter_nznbbs_certification_number;

      console.log(JSON.stringify(event));
      return resolve(event);
    }
    catch (err) {
      return reject(err);
    }
  })
};


// Function to populate Characteristic Measurements that map to the DB table CHARACTERISTIC_MEASUREMENTS
const populateRawCharacteristicMeasurementsFromRow = (row) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    try {
      var errors = [];
      var measurements = [];

      // Iterate over Detail columns in the spreadsheet and populate each one.
      for (var i = 1; i <= 10; i++) {

        // Get the characteristic name in the appropriate format for a lookup
        var charName = row[`characteristic_detail_${i}`] ? BBSSHelpers.CleanseDetailString(row[`characteristic_detail_${i}`]) : null;
        if (!charName)
          continue;

        // Cast the value to the correct type.
        var rawValue = row[`characteristic_value_${i}`];
        var rawUnits = row[`characteristic_units_${i}`];

        // Add the measurement to the list
        measurements.push({
          raw_characteristic_name: charName,
          raw_value: rawValue,
          raw_units: rawUnits,
          raw_col_ref: i
        });
      }

      const ALLOWABLE_AGES = [
        "a", "j", "p", "u",
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
        "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
        "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
        "51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
        "61", "62", "63", "64", "65", "66", "67", "68", "69", "70",
        "0+", "1+", "2+", "3+", "4+", "5+", "6+", "7+", "8+", "9+", "10+",
        "11+", "12+", "13+", "14+", "15+", "16+", "17+", "18+", "19+", "20+",
        "21+", "22+", "23+", "24+", "25+", "26+", "27+", "28+", "29+", "30+",
        "31+", "32+", "33+", "34+", "35+", "36+", "37+", "38+", "39+", "40+",
        "41+", "42+", "43+", "44+", "45+", "46+", "47+", "48+", "49+", "50+",
        "51+", "52+", "53+", "54+", "55+", "56+", "57+", "58+", "59+", "60+",
        "61+", "62+", "63+", "64+", "65+", "66+", "67+", "68+", "69+", "70+",
        "0-", "1-", "2-", "3-", "4-", "5-", "6-", "7-", "8-", "9-", "10-",
        "11-", "12-", "13-", "14-", "15-", "16-", "17-", "18-", "19-", "20-",
        "21-", "22-", "23-", "24-", "25-", "26-", "27-", "28-", "29-", "30-",
        "31-", "32-", "33-", "34-", "35-", "36-", "37-", "38-", "39-", "40-",
        "41-", "42-", "43-", "44-", "45-", "46-", "47-", "48-", "49-", "50-",
        "51-", "52-", "53-", "54-", "55-", "56-", "57-", "58-", "59-", "60-",
        "61-", "62-", "63-", "64-", "65-", "66-", "67-", "68-", "69-", "70-"
      ];

      const ALLOWABLE_SEXES = [
        "F", "M", "MU", "FU", "U"
      ];

      // **** REFACTOR ****
      // We want to add age, sex and condition to the characteristics
      // -> While not semantic with the spreadsheet this is the clearest place for data storage at-rest
      // -> It also allows for simple queriability
      if (typeof row['characteristic_age'] !== undefined && row['characteristic_age']) {
        measurements.push({
          raw_characteristic_name: 'age',
          raw_value: (ALLOWABLE_AGES.includes(String(row['characteristic_age']).toLowerCase())) ? String(row['characteristic_age']).toLowerCase() : 'u',
          raw_units: null,
          raw_col_ref: null
        });
      }

      if (typeof row['characteristic_sex'] !== undefined && row['characteristic_sex']) {
        measurements.push({
          raw_characteristic_name: 'sex',
          raw_value: (ALLOWABLE_SEXES.includes(String(row['characteristic_sex']).toUpperCase())) ? String(row['characteristic_sex']).toUpperCase() : 'U',
          raw_units: null,
          raw_col_ref: null
        });
      }

      if (typeof row['in_characteristic_status_code'] !== undefined && row['in_characteristic_status_code']) {
        measurements.push({
          raw_characteristic_name: 'instatuscode',
          raw_value: BBSSHelpers.TransformStatusCode(row['in_characteristic_status_code']),
          raw_units: null,
          raw_col_ref: null
        });
        // If out status code is unknown - assign in status code where provided...
        if (typeof row['out_characteristic_status_code'] === undefined || !row['out_characteristic_status_code']) {
          measurements.push({
            raw_characteristic_name: 'outstatuscode',
            raw_value: BBSSHelpers.TransformStatusCode(row['in_characteristic_status_code']),
            raw_units: null,
            raw_col_ref: null
          });
        }
      }

      if (typeof row['out_characteristic_status_code'] !== undefined && row['out_characteristic_status_code']) {
        measurements.push({
          raw_characteristic_name: 'outstatuscode',
          raw_value: BBSSHelpers.TransformStatusCode(row['out_characteristic_status_code']),
          raw_units: null,
          raw_col_ref: null
        });
        // If in status code is unknown - assign out status code where provided...
        if (typeof row['out_characteristic_status_code'] === undefined || !row['out_characteristic_status_code']) {
          measurements.push({
            raw_characteristic_name: 'instatuscode',
            raw_value: BBSSHelpers.TransformStatusCode(row['out_characteristic_status_code']),
            raw_units: null,
            raw_col_ref: null
          });
        }
      }

      if (typeof row['in_characteristic_condition_code'] !== undefined && row['in_characteristic_condition_code']) {
        measurements.push({
          raw_characteristic_name: 'inconditioncode',
          raw_value: BBSSHelpers.TransformConditionCode(row['in_characteristic_condition_code']),
          raw_units: null,
          raw_col_ref: null
        });
        // If out status code is unknown - assign in status code where provided...
        if (typeof row['out_characteristic_condition_code'] === undefined || !row['out_characteristic_condition_code']) {
          measurements.push({
            raw_characteristic_name: 'outconditioncode',
            raw_value: BBSSHelpers.TransformConditionCode(row['in_characteristic_condition_code']),
            raw_units: null,
            raw_col_ref: null
          });
        }
      }

      if (typeof row['out_characteristic_condition_code'] !== undefined && row['out_characteristic_condition_code']) {
        measurements.push({
          raw_characteristic_name: 'outconditioncode',
          raw_value: BBSSHelpers.TransformConditionCode(row['out_characteristic_condition_code']),
          raw_units: null,
          raw_col_ref: null
        });
        // If in status code is unknown - assign out status code where provided...
        if (typeof row['out_characteristic_condition_code'] === undefined || !row['out_characteristic_condition_code']) {
          measurements.push({
            raw_characteristic_name: 'inconditioncode',
            raw_value: BBSSHelpers.TransformConditionCode(row['out_characteristic_condition_code']),
            raw_units: null,
            raw_col_ref: null
          });
        }
      }

      let outStatusCodeIncluded = measurements.filter(measurement => measurement.raw_characteristic_name === 'outstatuscode').length > 0;

      // Add unknown for outConditionCode
      if (!outStatusCodeIncluded) {
        // -----------------------------------------
        console.info(`Out status code not included, adding unknown by default`);
        measurements.push({
          raw_characteristic_name: 'outstatuscode',
          raw_value: 'UNKNOWN',
          raw_units: null,
          raw_col_ref: null
        });
      }
      
      // **** END REFACTOR ****

      let statusDetailsArray = [];
      // For characteristic_status_detail_1 & 2
      for ( i = 1; i < 3; i++) {
        // ------------------------------------
        if (typeof row[`characteristic_status_detail_${i}_code`] !== undefined && row[`characteristic_status_detail_${i}_code`]) {
          // Add status detail and replace commas with hyphens (we comma separate the values and therefore internal commas are a no-no for this purpose)
          statusDetailsArray.push(row[`characteristic_status_detail_${i}_code`].split(',').join(' -'));
        }
      }
      if ( statusDetailsArray.length > 0) {
        measurements.push({
          raw_characteristic_name: 'characteristic_status_detail_code',
          raw_value: statusDetailsArray.join(','),
          raw_units: null,
          raw_col_ref: null
        });
      }

      // Moult Scores
      let moultScoreKeys = ['characteristic_moult_p1', 'characteristic_moult_p2',
        'characteristic_moult_p3', 'characteristic_moult_p4',
        'characteristic_moult_p5', 'characteristic_moult_p6',
        'characteristic_moult_p7', 'characteristic_moult_p8',
        'characteristic_moult_p9', 'characteristic_moult_p10'];

      if (moultScoreKeys.some(moultScoreKey => Object.keys(row).includes(moultScoreKey)) && moultScoreKeys.filter(key => row[key]).length > 0) {
        // At least one moult score has been defined, populate the moult-score raw_characteristic
        measurements.push({
          raw_characteristic_name: 'moultscore',
          raw_value: Object.keys(row).filter(key => (key.includes('characteristic_moult_') && typeof row[key] !== 'undefined')).map(characteristicKey => row[characteristicKey]).join(','),
          raw_units: null,
          raw_col_ref: null
        });
      }

      // Handle moult notes
      if (typeof row['info_moult_notes'] !== undefined && row['info_moult_notes']) {
        measurements.push({
          raw_characteristic_name: 'info_moult_notes',
          raw_value: String(row['info_moult_notes']),
          raw_units: null,
          raw_col_ref: null
        });
      }

      // Return the list of populated characteristic measurements
      console.info(JSON.stringify(measurements));
      return resolve(measurements);
    }
    catch (err) {
      return reject(err);
    }
  })
};


// Function to populate Mark Configuration that maps to an entry in the table MARK_CONFIGURATION
const populateMarkConfigurationFromRow = (row) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {

    try {
      // **** REFACTOR ****
      // With next method
      var marks = [];
      var bandArray = [];
      var bandData = null;
      var rawBands = [];
      var mark_config_components = [
        'mark_config_left_tibia', 'mark_config_left_tarsus', 
        'mark_config_right_tibia', 'mark_config_right_tarsus', 
        'mark_config_other_mark_alpha'
      ];

      // -------------
      // Uncertainty
      // -------------
      let markConfigUncertainty = {};
      if ('mark_config_uncertainty' in row && row.mark_config_uncertainty) {
        markConfigUncertainty = {
          side: (row.mark_config_uncertainty.toLowerCase().includes('side')),
          position: (row.mark_config_uncertainty.toLowerCase().includes('position')),
          alphanumeric: (row.mark_config_uncertainty.toLowerCase().includes('alphanumeric')),
          location_idx: (row.mark_config_uncertainty.toLowerCase().includes('location')),
        };
      } else {
        markConfigUncertainty = {
          side: false,
          position: false,
          alphanumeric: false,
          location_idx: false
        }
      }

      mark_config_components.forEach(component => {
        // Has the mark_config component been filtered out during preprocessing...?
        if (component in row) {
          // -----------------------
          bandData = row[component];
          if (bandData && bandData.includes('(enter bands on')) {
            console.info('Default text detected, skipping band component...');
            return;
          }
          else if (component === 'mark_config_other_mark_alpha' && bandData && bandData !== '') {
            let otherMark = {
              "side": null,
              "position": null,
              "location_idx": null,
              "mark_type": row.mark_config_other_mark_type ? BBSSHelpers.TransformOtherMarkType(row.mark_config_other_mark_type) : 'OTHER',
              "mark_form": null,
              "mark_material": null,
              "mark_fixing": null,
              "colour": null,
              "text_colour": null,
              "alphanumeric_text": ('mark_config_other_mark_alpha' in row && row.mark_config_other_mark_alpha) ? row.mark_config_other_mark_alpha : null
            }
            // Add the other mark to the mark_configuration array
            marks.push(otherMark);
          }
          else if (component !== 'out_mark_config_other_mark_type') {
            rawBands = bandData ? String(bandData).split(/[,.;/]+/).map(word => word.trim()) : null;
            bandArray = BBSSHelpers.ProcessRawLegBandArray(rawBands, markConfigUncertainty, row);
            bandArray.forEach((band, idx) => {
              marks.push({
                'side': !markConfigUncertainty.side ? component.match(/(right|left)/g)[0].toUpperCase() : null,
                'position': !markConfigUncertainty.position ? component.match(/(tibia|tarsus)/g)[0].toUpperCase() : null,
                'location_idx': !markConfigUncertainty.location_idx ? idx : null,
                ...band
              });
            });
          }
        }
      });

      // -----------------------------------------------------------------------
      // ENSURE PRIMARY MARK HAS BEEN ADDED TO THE MARK_CONFIGURATION
      // -----------------------------------------------------------------------
      // At this point, we've done all we can to determine the bands on the bird
      // from the available columns. The only thing left to process is the case
      // where no specific band data has been provided, and we've just (possibly)
      // got our tracked metal band. If that's the case, we need to wring out
      // more information related to that - from even more potential columns.
      // NOTE: this only applies if the event_type is not POST CHANGE
      //      reason being: in this case we need to know explicitly if the
      //      primary mark has been removed or not... therefore we can't implicitly add it
      if (((row.mark_state_code !== '' && row.mark_state_code !== null) || (row.event_type !== '' && row.event_type !== null)) && BBSSHelpers.TransformEventType(row.event_type) !== 'IN_HAND_POST_CHANGE' && marks.length <= 0 && row.short_number && row.prefix_number && !['PIT', 'WEB'].includes(row.prefix_number.toUpperCase())) {
        console.info(`Populating tracked band by default`);
        marks.push(BBSSHelpers.PopulateTrackedBand(row));
      }

      // Return the list of populated marks
      return resolve(marks);
    }
    catch (err) {
      return reject(err);
    }
  })
};


const validateFileFormat = (customErrorFactory, objectPath ,sheetJson, mapping) => {
  // ----------------------------------------------------------------------------
  console.info(".validateFileFormat()");

  let fileFormatError = false;

  // 1) Check that the workbook has been read without issue
  // 2) Check that the file extension is '.xlsx'
  // 3) Check that the tagged version of the spreadsheet matches the currently supported version
  // 4) Check that the headers of the spreadsheet match the required version
  // 5) Check that there is at least 1 non-empty row (without instructional text)
  let objectPathSplit = objectPath.split('.');

  console.log(`sheetJson invalid? ${sheetJson}`);
  !(sheetJson) && console.log('Cannot read spreadsheet file as xlsx');

  console.log(`xlsx extension not found? ${(objectPathSplit && objectPathSplit[( objectPathSplit.length - 1 )] !== 'xlsx')}`);
  (objectPathSplit && objectPathSplit[( objectPathSplit.length - 1 )] !== 'xlsx') && console.log('File extension not .xlsx');
  
  console.log(`version not found? ${(sheetJson) && (sheetJson.version && !sheetJson.version.includes(mapping.internal_version_id))}`);
  (sheetJson) && (sheetJson.version && !sheetJson.version.includes(mapping.internal_version_id)) && console.log('Spreadsheet version does not match the supported version based on the tag');
  
  console.log(`Headers don't match? ${(sheetJson) && (!Object.values(mapping.ordered_ss_property_array).every((headerField, index) => { (headerField !== sheetJson.headers[index]) && console.log(`${headerField} does not match required header: ${sheetJson.headers[index]}`); return headerField === sheetJson.headers[index]; } ))}`);
  (sheetJson) && (!Object.values(mapping.ordered_ss_property_array).every((headerField, index) => { (headerField !== sheetJson.headers[index]) && console.log(`${headerField} does not match required header: ${sheetJson.headers[index]}`); return headerField === sheetJson.headers[index]; } )) && console.log('Column headers do not match the supported version')
  
  console.log(`At no non-empty rows? ${(sheetJson) && !sheetJson.rows.some(row => { 
    return (
      Object.values(row).some(x => (x !== null && x !== undefined && x !== '' && String(x).trim() !== ''))
      && !((row.prefix_number) && String(row.prefix_number).toLowerCase().includes('input below this point will not be processed'))
      && !(row.mark_state_code && row.mark_state_code !== '')
      );
    })}`);
  (sheetJson) && !sheetJson.rows.some(row => { 
    return (
      Object.values(row).some(x => (x !== null && x !== undefined && x !== '' && String(x).trim() !== ''))
      && !((row.prefix_number) && String(row.prefix_number).toLowerCase().includes('input below this point will not be processed'))
      && !(row.mark_state_code && row.mark_state_code !== '')
      );
    }) && console.log('There are no non-blank and non-stock code rows');

  fileFormatError = (
    !(sheetJson)
    || (objectPathSplit && objectPathSplit[( objectPathSplit.length - 1 )] !== 'xlsx')
    || (sheetJson.version && !sheetJson.version.includes(mapping.internal_version_id))
    || (!Object.values(mapping.ordered_ss_property_array).every((headerField, index) => headerField === sheetJson.headers[index] ))
    || !sheetJson.rows.some(row => { 
        return (
          Object.values(row).some(x => (x !== null && x !== undefined && x !== '' && String(x).trim() !== ''))
          && !((row.prefix_number) && String(row.prefix_number).toLowerCase().includes('input below this point will not be processed'))
          && !(row.mark_state_code && row.mark_state_code !== '')
        );
      })
  );

  if (fileFormatError) {
    return [customErrorFactory.getError('FileUploadFormatError', [`have the .xlsx file extension, match version tag: ${mapping.internal_version_id}, have at least 1 row of non-stock data in the 'Bands' tab and match the corresponding spreadsheet headers`])];
  }
  
  return [];
}


const reviewUploadStatus = (stage=null, errors, adminApproved=false) => {
  // ----------------------------------------------------------------------------
  console.info(".reviewUploadStatus()");

  let criticals = errors.filter(error => error.severity === 'CRITICAL').length > 0;
  let warnings = errors.filter(error => error.severity === 'WARNING').length > 0;

  if (stage && stage ==='FILE_FORMAT' && criticals) {
    return 'CRITICAL_FILE_FORMAT';
  }
  else if (stage && stage ==='FILE_FORMAT' && !criticals && !adminApproved) {
    return 'PASS_FILE_FORMAT';
  }
  else if (criticals && warnings){
    return 'WARNINGS_AND_CRITICALS';
  }
  else if (criticals) {
    return 'CRITICALS';
  }
  // If the stage is 'SCHEMA' validation, 
  // -> return the PASS_FILE_FORMAT status so as not to prematurely indicate success
  else if (stage && stage ==='SCHEMA' && !adminApproved) {
    return 'PASS_FILE_FORMAT';
  }
  else if (warnings && !adminApproved) {
    return 'WARNINGS';
  }
  else if (adminApproved) {
    return 'ADMIN_APPROVED';
  }
  else {
    return 'PASS';
  }
};


const generateSpreadsheetDbPayload = async (db, eventBatch, banderUpload) => {
  // --------------------------------------------------------
  console.info('.generateSpreadsheetDbPayload()');

  // 1) Set the bird-id and generate bird_id <-> mark_id mapping (and generate where required)
  let birdMarkMapping = {};
  eventBatch.map((record, idx) => {
    // -------------------------------------
    // Search the mapping for if a mark_id<->bird-id mapping exists
    let matchedMark = record.mark_configuration.find(markConfig => markConfig.mark_id in birdMarkMapping);

    if ( !('id' in record.bird && record.bird.id) && typeof matchedMark !== 'undefined' && matchedMark ) {
      // If a bird-id exists in the mapping already -> update the bird-id
      console.info('Bird id not found, using mapping');
      record.bird.id = birdMarkMapping[matchedMark.mark_id];
    }
    else if ( !('id' in record.bird && record.bird.id)
              && record.event.event_type === 'IN_HAND_POST_CHANGE'
              && typeof eventBatch.find((searchRecord, searchIdx) => { return searchIdx < idx 
                                                                              && searchRecord.metadata.eventIndex === record.metadata.eventIndex
                                                                              && searchRecord.metadata.primaryMark === record.metadata.primaryMark
                                                                              && 'id' in searchRecord.bird }) !== 'undefined' ) {
      // If a POST CHANGE event without a pre-assigned bird-id, check if a matching PRE CHANGE does have a bird-id and assign that
      console.info('Bird id not found, but found matching pre-change with bird-id');
      let preChangeRecord = eventBatch.find((searchRecord, searchIdx) => { return searchIdx < idx 
                                              && searchRecord.metadata.eventIndex === record.metadata.eventIndex
                                              && searchRecord.metadata.primaryMark === record.metadata.primaryMark
                                              && 'id' in searchRecord.bird });
      record.bird.id = preChangeRecord.bird.id;
    }
    else if ( !('id' in record.bird && record.bird.id) ) {
      // Otherwise create a new bird-id and add to the mapping 
      // (we only want to do this once so if subsequent records relate to this bird we don't need to also tag as new)
      console.info('Bird id not found, CREATING NEW BIRD');
      record.bird.id = uuidv4();
      record.bird.new = true;
    }
    // After this, always update the bird-id<->mark_id mapping
    record.mark_configuration.filter(markConfig => markConfig.mark_id).map(markConfig => {
      birdMarkMapping[markConfig.mark_id] = record.bird.id;
    });
  });


  // 2) Get the project_coordinator (a.k.a project manager) and assign as event_owner
  let event_owner_id = (await db.ro_get_project_coordinator(eventBatch[0].event.project_id))[0].ro_get_project_coordinator;

  console.log(`event_owner: ${event_owner_id}`);

  // 3) Prepare the remainder of the eventBatch for upload
  let dbPayload = eventBatch.map((record, idx) => {

    // BIRD - generate ID where not already available
    record.bird =  {
      id: record.bird.id,
      species_id: record.bird.species_id,
      friendly_name: record.mark_configuration.reduce((accumulator, current) => {
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
      new: record.bird.new
    };

    // EVENT - generate ID and link to bird ID
    // Supplement the event with the event_owner_id (i.e. project coordinator)
    record.event = {
      ...record.event,
      id: uuidv4(),
      bird_id: record.bird.id,
      event_owner_id: event_owner_id,
      spreadsheet_id: banderUpload[0].id
    };

    delete record.event.raw_event_reporter_nznbbs_certification_number;
    delete record.event.raw_event_reporter_name;
    delete record.event.raw_event_provider_nznbbs_certification_number;
    delete record.event.raw_event_provider_name;

    // CHARACTERISTIC_MEASUREMENT
    record.characteristic_measurement = record.characteristic_measurement.map(charMeasure => {
      // Supplement characteristic_measurements with event_ids
      delete charMeasure.raw_characteristic_name;
      delete charMeasure.raw_value;
      delete charMeasure.raw_units;
      delete charMeasure.validation_characteristic_unit;

      return {
        ...charMeasure,
        event_id: record.event.id
      };
    });

    if (record.characteristic_measurement.filter(charMeasure => charMeasure.characteristic_id === 43).length <= 0) {
      record.characteristic_measurement.push({
        characteristic_id: 43,
        value: 'UNKNOWN',
        event_id: record.event.id
      });
    }

    // MARK CONFIGURATION
    record.mark_configuration = record.mark_configuration.map(markConfig => {
      delete markConfig.validation_prefix_number;
      delete markConfig.validation_short_number;
      delete markConfig.validation_state;
      delete markConfig.validation_bander_id;

      return {
        ...markConfig,
        event_id: record.event.id
      }
    });

    // MARK_STATE
    // -> If this is a split record we need to be a bit smarter about the mark_state
    record.mark_state = record.mark_configuration
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
    // -> If this is a post change event from a spreadsheet - we need to make sure we check for any detached marks
    
    if (record.event.event_type === 'IN_HAND_POST_CHANGE'
          && typeof eventBatch.find((searchRecord, searchIdx) => { 
            return searchIdx < idx
            && searchRecord.metadata.eventIndex === record.metadata.eventIndex
            && searchRecord.metadata.primaryMark === record.metadata.primaryMark }) !== 'undefined' ) {
      // Need to check for any bands included in preChangeRecord and NOT in subject record (these are detached bands)
      console.info('Prechange event found -> searching for any detached marks and adding to mark state');
      let preChangeRecord = eventBatch.find((searchRecord, searchIdx) => { 
        return searchIdx < idx
        && searchRecord.metadata.eventIndex === record.metadata.eventIndex
        && searchRecord.metadata.primaryMark === record.metadata.primaryMark });

      let mark_ids_at_release = record.mark_configuration.filter(markConfig => markConfig.mark_id)
                                                        .map(markConfig => markConfig.mark_id);
      console.log(`Marks at release: ${mark_ids_at_release}`);
      let mark_ids_at_capture = preChangeRecord.mark_configuration.filter(markConfig => markConfig.mark_id)
                                                        .map(markConfig => markConfig.mark_id);
      console.log(`Marks at capture: ${mark_ids_at_capture}`);
      
      mark_ids_at_capture.filter(captureMarkId => !mark_ids_at_release.includes(captureMarkId))
      .map(markId => { 
        record.mark_state.push({ 
          event_id: record.event.id,
          mark_id: markId, 
          state: 'DETACHED', 
          state_idx: 0, 
          is_current: false 
         }); 
       });
    }

    return record;
  });

  // Remove the metadata subobject prior to DB upload
  dbPayload = dbPayload.map(record => {
    delete record.metadata;
    return record;
  });

  console.log(JSON.stringify(dbPayload));
  return dbPayload;
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


const postSpreadsheetToDB = async(db, payload) => {
  // --------------------------------------------------------
  console.info('.postSpreadsheetToDB()');

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


const formatResponse = (event, method = 'get', res) => {
  // ----------------------------------------------------------------------------    
  console.info('stringify' + JSON.stringify(res));
  switch (method) {
    case 'get': {
      let presignedUrl = getPresignedURL(res[0].storage_host, res[0].object_path, null);
      return {
        ...res[0],
        url: presignedUrl
      }
    }
    case 'search': {
      return res.map(spreadsheet => {
        return {
          ...spreadsheet,
        }
      });
    }
    case 'post': {
      if (event.queryStringParameters 
        && 'presignedUrl' in event.queryStringParameters
        && event.queryStringParameters.presignedUrl.toLowerCase() === 'true') {
          return res;
      }
      return {
        ...res,
      }
    }
    case 'put': {
      if (event.queryStringParameters 
        && 'presignedUrl' in event.queryStringParameters
        && event.queryStringParameters.presignedUrl.toLowerCase() === 'true') {
         return res;
      }
      return {
        ...res[0],
      }
    }
    case 'delete': {
      return {};
    }
    default: {
      return res;
    }
  }
}


// POST
module.exports.post = (event, context, cb) => {
  console.info(RESOURCE_NAME + ' post')
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
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
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
      console.info('QueryString parameters OK. Validating payload...');
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      return BoilerPlate.validateJSON(payload, payloadSchema);
    })
    // Handle errors / Validate payload
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body schema validation error(s)!`, errors);
      }
      console.info('Payload parameters OK. Check/renew connection to DB and get custom errors');
      // Get Custom Errors schema from Dynamo
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    }).then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return BBHelpers.getUserProjects(db, claims);
    })
    .then(userProjectList=> {
      console.info('Validating business rules...');
      return validatePostBusinessRules(customErrorFactory, db, event, claims, governingCognitoGroup, userProjectList);
    })
    .then(errors => {
      // ---------------------
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length}  business validation error(s)!`, errors);
      }
      return completePostAction(db, event);
    })
    .then(async res => {
      // ---------------------
      return formatResponse(event, 'post', res);
    })
    .then(res => {
      console.info("Returning with no errors");
      cb(null, {
        "statusCode": (event.queryStringParameters 
                        && 'presignedUrl' in event.queryStringParameters
                        && event.queryStringParameters.presignedUrl.toLowerCase() === 'true') ? 200 : 201,
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

// SEARCH spreadsheets
module.exports.search = async (event, context, cb) => {
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
  const parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  let resultSet;
  let statusCode = 200;

  try {
    // --------------------------------------------
    const claims = event.requestContext.authorizer.claims;
    const auth = await BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST);
    const governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(auth);
    const isAdmin = governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME;

    console.log("Is admin: " + isAdmin);
   
    const schema = await BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
    const errors = BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, schema);
    if (!!errors.length) {
      throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
    }

    let dbObj = await DBAccess.getDBConnection(db, dbCreationTimestamp);
    db = dbObj.db;
    dbCreationTimestamp = dbObj.dbCreationTimestamp;

    let banderProjectList = await BBHelpers.getUserProjects(db, claims);

    let searchResultSet = await searchDB(db, event, claims, governingCognitoGroup, banderProjectList);

    resultSet = await addSearchCalculatedFields(db, searchResultSet, claims, governingCognitoGroup)

  }
  catch (e) {
    console.log("ERROR")
    console.dir(e);
    statusCode = 500;
    resultSet = e;
  }

  return cb(null, {
    "statusCode": statusCode,
    "headers": {
      "Access-Control-Allow-Origin": "*", // Required for CORS support to work
      "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
    },
    "body": JSON.stringify(resultSet),
    "isBase64Encoded": false
  });
}

// GET spreadsheets
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

  console.info(JSON.stringify(event));

  // Get the schema details for parameter validation
  var parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  //custom error schema 
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
      console.info('QueryString parameters OK. Check/renew connection to DB');
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      //   DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      console.info('Validating business rules...');
      return validateUploadExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Spreadsheet not found', error);
      return validateUploadAccess(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      console.log(error);
      if (error) {
        throw new BoilerPlate.ForbiddenError(`Authorisation validation error(s)!!`, error);
      }
      return getDB(db, event); 
    })
    .then(res => {
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

// todo -> ONLY spreadsheet owner or admin can delete
module.exports.delete = async (event, context, cb) => {
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
  const parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  let resultSet;
  let statusCode = 204;


  try {
    const schema = await BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
    const errors = BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, schema);
    if (!!errors.length) {
      throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
    }

    let dbObj = await DBAccess.getDBConnection(db, dbCreationTimestamp);
    db = dbObj.db;
    dbCreationTimestamp = dbObj.dbCreationTimestamp;


    const auth = await BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST);
    const governingGroup = BBHelpers.getGoverningCognitoGroup(auth);
    const isAdmin = governingGroup == BBHelpers.ADMIN_GROUP_NAME;


    let resultSet = await db.query(`
      SELECT 
        bu.id as bu_id, 
        pbm.bander_id as bander_id
      FROM 
        bander_uploads bu
      LEFT OUTER JOIN 
        project_bander_membership pbm ON bu.project_id = pbm.project_id and pbm.bander_id = \${currUser}
      WHERE
            bu.id = \${spreadsheetId}
			`,
      {
        currUser: event.requestContext.authorizer.claims.sub,
        spreadsheetId: event.pathParameters.spreadsheetId
      });

    let recordExists = resultSet.bu_id !== null;
    let projectMember = resultSet.bander_id !== null;

    if (!recordExists) {
      throw new BoilerPlate.NotFoundError("NotFoundError", {});
    }

    if (projectMember || isAdmin) {
      resultSet = await db.bander_uploads.destroy(event.pathParameters.spreadsheetId);
      let recordActionUpdate = await db.record_action.insert({
        db_action: 'DELETE',
        db_table: 'bander_uploads',
        db_table_identifier_name: 'id',
        db_table_identifier_value: event.pathParameters.spreadsheetId
      });
    }
    else {
      throw new BoilerPlate.ForbiddenError("NotAuthorisedError", {})
    }
  }

  catch (e) {
    console.dir(e);
    statusCode = 500;
    resultSet = e;
  }

  return cb(null, {
    "statusCode": statusCode,
    "headers": {
      "Access-Control-Allow-Origin": "*", // Required for CORS support to work
      "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
    },
    "body": JSON.stringify(resultSet),
    "isBase64Encoded": false
  });
}

//minimal patch just to change the ss state 
module.exports.patch = async (event, context, cb) => {
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
  const parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  }

  let resultSet;
  let statusCode = 200;

  try {
      // Invocation claims
    let claims = event.requestContext.authorizer.claims;
    const auth = await BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST);
    let governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(auth);
    const paramSchema = await BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
    const paramErrors = BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, paramSchema);
    if (!!paramErrors.length) {
      throw new BoilerPlate.ParameterValidationError(`${paramErrors.length} Path parameter validation error(s)!`, paramErrors);
    }
    //  const payloadSchema = await BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    //  const payloadErrors = BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, payloadSchema);
    //  if (!!payloadErrors.length) {
    //    throw new BoilerPlate.ParameterValidationError(`${payloadErrors.length} Path parameter validation error(s)!`, payloadErrors);
    //  }

    let dbObj = await DBAccess.getDBConnection(db, dbCreationTimestamp);
    db = dbObj.db;
    dbCreationTimestamp = dbObj.dbCreationTimestamp;

    // Validate bander access to patch the bander_upload
    let uploadAccessCheck = await validateUploadAccess(customErrorFactory, db, event, claims, governingCognitoGroup);

    if (uploadAccessCheck) {
      throw new BoilerPlate.ForbiddenError(`Authorisation validation error`);
    }

    const UPLOAD_STATUS = db.enums.enum_upload_status;

    let payload = JSON.parse(event.body);
    console.log("                            PAYLOAD                      ");
    console.dir(payload);
    console.log("SSid:" + event.pathParameters.spreadsheetId);
    console.log(payload.upload_status);
    if (!payload.upload_status ||
      !(UPLOAD_STATUS.includes(payload.upload_status.toUpperCase()))) {
      throw new BoilerPlate.ParameterValidationError("Invalid spreadsheet state", null, 422);
    }

    let currentUploadResultset = await db.bander_uploads.find({ 'id = ': event.pathParameters.spreadsheetId });

    if (currentUploadResultset.length <= 0) {
      throw new BoilerPlate.NotFoundError('Spreadsheet not found', {});
    }

    let requeueResultset;

    // Current support is for changing upload_status to 'PENDING_RESULT' (for re-validations) 
    //      OR 'ADMIN_APPROVED' (for re-validation and upload to DB)

    // Standard re-validation if not currently pending a result
    if (payload.upload_status === 'PENDING_RESULT' && currentUploadResultset[0].upload_status !== 'PENDING_RESULT') {
      // -------------------------------------------------
      resultSet = await db.bander_uploads.update({ id: event.pathParameters.spreadsheetId } , payload);

      let snsRequest = {
        Records: [{
            s3: { object: { key: encodeURIComponent(currentUploadResultset[0].object_path) } },
            eventTime: Moment().format()
          }]
      };

      console.log(JSON.stringify(snsRequest));
      requeueResultset = await queueSQSMessage(snsRequest, 2)
    }
    // If re-validation alreadyin progress, return response as though already triggered
    else if (payload.upload_status === 'PENDING_RESULT') {
      resultSet = currentUploadResultset[0];
    }
    // If in either the WARNINGS or PASS states, bander_upload can be updated to REQUEST_FOR_APPROVAL
    // OR: If in the REQUEST_FOR_APPROVAL state, bander_upload can be updated to ADMIN_REJECTEDs
    else if ((payload.upload_status === 'REQUEST_FOR_APPROVAL' 
              && ['WARNINGS', 'PASS'].includes(currentUploadResultset[0].upload_status))
              || (payload.upload_status === 'ADMIN_REJECTED'
              && ['REQUEST_FOR_APPROVAL'].includes(currentUploadResultset[0].upload_status))) {
      resultSet = await db.bander_uploads.update({ id: event.pathParameters.spreadsheetId } , payload);
    }
    // If ADMIN_APPROVED, complete re-validation with extra key to trigger DB upload if re-validated successfully
    else if (payload.upload_status === 'ADMIN_APPROVED' && currentUploadResultset[0].upload_status === 'REQUEST_FOR_APPROVAL') {
      // -------------------------------------------------
      resultSet = await db.bander_uploads.update({ id: event.pathParameters.spreadsheetId } , payload);
      
      let snsRequest = {
        Records: [{
            s3: { object: { key: encodeURIComponent(currentUploadResultset[0].object_path) } },
            eventTime: Moment().format(),
            adminApproval: true
          }]
      };

      console.log(JSON.stringify(snsRequest));
      requeueResultset = await queueSQSMessage(snsRequest, 2)
    }
    else if (payload.upload_status === 'ADMIN_APPROVED' && governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
      // Unauthorised
      throw new BoilerPlate.ForbiddenError("Must be admin to approve a spreadsheet for upload");
    }
    else if (payload.upload_status === 'ADMIN_APPROVED') {
      // Parameter validation error
      throw new BoilerPlate.ParameterValidationError("Spreadsheet not in REQUEST_FOR_APPROVAL status");
    }
    else {
      // Parameter validation error
      throw new BoilerPlate.ParameterValidationError(`Cannot complete spreadsheet status update to ${payload.upload_status} from ${currentUploadResultset[0].upload_status}`);
    }

    if (resultSet.length == 0) {
      throw new BoilerPlate.NotFoundError("Spreadsheet not found", {});
    }
    else {
      resultSet = resultSet[0];
    }
  }
  catch (e) {
    console.log('There was an error');
    console.dir(e);
    statusCode = e.statusCode || 500;
    resultSet = e.details || [];
  }

  console.info("Returning with no errors");
  return cb(null, {
    "statusCode": statusCode,
    "headers": {
      "Access-Control-Allow-Origin": "*", // Required for CORS support to work
      "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
    },
    "body": JSON.stringify(resultSet),
    "isBase64Encoded": false
  });
};

module.exports.cleanupSheets = async (event, context, cb) => {

  let dbObj = await DBAccess.getDBConnection(db, dbCreationTimestamp);
  db = dbObj.db;
  dbCreationTimestamp = dbObj.dbCreationTimestamp;
  let s3 = new AWS.S3({ signatureVersion: 'v4' });
  console.log(`${process.env.USER_ASSETS_BUCKET}/${sheetBasePath}`);

  let resultSet = db.bander_uploads.find({}, { fields: ['id', 'object_path', 'storage_host'] });
  let files = s3.listObjectsV2({ Bucket: process.env.USER_ASSETS_BUCKET, MaxKeys: 1000, Prefix: sheetBasePath }).promise();

  let results = await Promise.all([resultSet, files]);

  let fileNames = results[1].Contents;
  console.log(`${fileNames.length} files found in s3 bucket`);

  let dbNames = results[0].map(f => f.object_path);

  console.log(`${dbNames.length} entries found in db`);

  let orphanedFiles = fileNames
    .filter(f => !dbNames.includes(f.Key) && Moment(f.LastModified).add(8, 'hours') < Moment());

  console.log(`${orphanedFiles.length} files to delete`);

  if (orphanedFiles.length) {
    let result = await s3.deleteObjects({
      Bucket: process.env.USER_ASSETS_BUCKET,
      Delete: {
        Objects: orphanedFiles.map(f => {
          return { Key: f.Key }
        })

      }
    }).promise();

    console.info("Deletion results");
    console.dir(result);
  }


};

// Validation (and optional upload) service for birdbanding spreadsheets
module.exports.validation = async (event, context, cb) => {
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
  const parameterSchemaParams = {
    table: process.env.PARAMETER_SCHEMA_TABLE,
    id: process.env.PARAMETER_SCHEMA_ID,
    version: Number(process.env.PARAMETER_SCHEMA_VERSION)
  };
  const mappingSchemaParams = {
    table: process.env.MAPPING_SCHEMA_TABLE,
    id: process.env.MAPPING_SCHEMA_ID,
    version: Number(process.env.MAPPING_SCHEMA_VERSION)
  };
  const preprocessingSchemaParams = {
    table: process.env.PREPROCESSING_SCHEMA_TABLE,
    id: process.env.PREPROCESSING_SCHEMA_ID,
    version: Number(process.env.PREPROCESSING_SCHEMA_VERSION)
  };

  const customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
  };

  // Validations
  let validations = ['event', 'markConfiguration', 'characteristicMeasurement', 'bird', 'bander', 'location'];

  let errors = [];
  let numberOfRows = null;
  let adminApproved = false;
  let banderUploadId = null;
  let banderUploadUpdateResultset;
  let resultSet;
  let uploadStatus = 'PENDING_RESULT';
  let statusCode = 200;

  try {
    // --------------------------------------------
    // const claims = event.requestContext.authorizer.claims;
    // const auth = await BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST);
    // const governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(auth);
    // const isAdmin = governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME;

    const schema = await BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
    const mappingSchema = await BoilerPlate.getSchemaFromDynamo(mappingSchemaParams.table, mappingSchemaParams.id, mappingSchemaParams.version);
    const preprocessingSchema = await BoilerPlate.getSchemaFromDynamo(preprocessingSchemaParams.table, preprocessingSchemaParams.id, preprocessingSchemaParams.version);
    const customErrors = await BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);

    customErrorFactory = new CustomErrorFactory(customErrors.definitions);

    let dbObj = await DBAccess.getDBConnection(db, dbCreationTimestamp);
    db = dbObj.db;
    dbCreationTimestamp = dbObj.dbCreationTimestamp;

    let request = JSON.parse(event.Records[0].body);
    let objectPath = decodeURIComponent(request.Records[0].s3.object.key.replace('+', ' '));
    console.log(`Processing: ${decodeURIComponent(request.Records[0].s3.object.key.replace('+', ' '))}`);
    console.log(`S3 Event time: ${request.Records[0].eventTime}`);
    console.log(`Aged event? ${Moment(request.Records[0].eventTime).isBefore(Moment().subtract(15, 'minutes'))}`);
    let isStaleS3Event = Moment(request.Records[0].eventTime).isBefore(Moment().subtract(15, 'minutes'));

    // Assess whether validation is admin approved, and if so, we will handle the upload to DB after successful validation
    adminApproved = 'adminApproval' in request.Records[0] && request.Records[0].adminApproval;
    console.info(`Validation has ${adminApproved ? ' ' : 'not '}been admin-approved`)

    // 1) Check whether the spreadsheet entity exists in the database, if not, return the message to the queue
    //  Note: we need to replace '+' with space prior to decoding URI because S3 encodes space at '+' and '+' as '%2B'
    let banderUploadResultset = await db.bander_uploads.find({
      'object_path =': objectPath
    });

    console.log(banderUploadResultset);

    if (isStaleS3Event) {
      // If the upload is stale (> 15 minutes old without a database entry), delete the spreadsheet from the S3 bucket
      // ------------------------------------------------------
      console.info('Stale upload, no DB entry, deleting from S3...');
      resultSet = await deleteFromS3(objectPath);
    }
    else if (banderUploadResultset.length <= 0 && !isStaleS3Event) {
      // If no entry for this spreadsheet in the database, and upload not stale, re-queue SQS event (otherwise complete validation actions)
      // ------------------------------------------------------
      console.info('DB entry does not exist yet and event not stale. Re-queueing notification...');
      resultSet = await queueSQSMessage(request, 180);
    }
    else {
      // ------------------------------------------------------
      try {
        // ----------------------------------------------------------------
        console.info('Spreadsheet found in DB, completing validation...');
        console.log(banderUploadResultset);

        banderUploadId = banderUploadResultset[0].id;
        console.log(`Bander upload id: ${banderUploadId}`);

        // Get the spreadsheet from S3
        let spreadsheetBuffer = await getBodyFromS3(objectPath);

        // Read workbook (as b64) into JSON spreadsheet representation
        let sheetJson = await readWorkbook(spreadsheetBuffer, 'buffer', mappingSchema);
          
        // Check format of spreadsheet and handle errors
        errors = [...validateFileFormat(customErrorFactory, objectPath, sheetJson, mappingSchema)];

        // Review upload status at this point for reference at subsequent validation stages
        uploadStatus = reviewUploadStatus('FILE_FORMAT', errors, adminApproved);

        console.info(`Post file format validation status: ${uploadStatus}`);

        banderUploadUpdateResultset = await db.bander_uploads.update(
          { 'id = ': banderUploadId } ,
          {
            errors: JSON.stringify(errors),
            warnings_count: errors.filter(error => error.severity === 'WARNING').length,
            criticals_count: errors.filter(error => error.severity === 'CRITICAL').length,
            upload_status: uploadStatus
          }
        );
        console.info(`Upload status and errors updated to DB: ${JSON.stringify(banderUploadUpdateResultset)}`);

        if (!['PASS_FILE_FORMAT', 'ADMIN_APPROVED'].includes(uploadStatus)) {
          throw new Error('Spreadsheet File Format Error');
        }

        // Create the event batch from the spreadsheet JSON
        let eventBatch = await createRawEventBatchFromSpreadsheet(sheetJson, banderUploadResultset[0].project_id, mappingSchema);

        console.info(`Preprocessed event batch: ${JSON.stringify(eventBatch)}`);
        console.info(`Row count: ${eventBatch.events.length}`);

        let rawSchemaErrors = await BoilerPlate.validateJSON(eventBatch.events, preprocessingSchema);

        errors = [...errors, ...(await formatSchemaErrors(customErrorFactory, rawSchemaErrors, eventBatch, 'SPREADSHEET', mappingSchema))];

        console.info(`Post processed schema errors: ${JSON.stringify(errors)}`);

        // Review upload status at this point for reference at subsequent validation stages
        uploadStatus = reviewUploadStatus('SCHEMA', errors, adminApproved);

        banderUploadUpdateResultset = await db.bander_uploads.update(
          { 'id = ': banderUploadId } ,
          {
            errors: JSON.stringify(errors),
            warnings_count: errors.filter(error => error.severity === 'WARNING').length,
            criticals_count: errors.filter(error => error.severity === 'CRITICAL').length,
            no_of_rows: eventBatch.events[eventBatch.events.length - 1].metadata.eventIndex,
            upload_status: uploadStatus
          }
        );

        if (!['PASS_FILE_FORMAT', 'ADMIN_APPROVED'].includes(uploadStatus)) {
          throw new Error('Schema Validation Errors');
        }

        // Get lookup data as the beginning of the business validation process
        let lookupData = await getSpreadsheetLookupData(db, validations, eventBatch.events);

        eventBatch.events = await supplementSpreadsheetEventsWithLookupData(validations, eventBatch.events, lookupData);

        console.info(`Supplemented event batch: ${JSON.stringify(eventBatch)}`);

        errors = [
          ...errors, 
          ...(await validateSpreadsheetRowBusinessRules(customErrorFactory, null, null, validations, eventBatch.events, mappingSchema)),
          ...(await validateIntraSpreadsheetBusinessRules(customErrorFactory, null, null, validations, eventBatch.events, mappingSchema))
        ];

        // Also add any warnings for skipped rows
        if (eventBatch.skipped.length > 0) {
          eventBatch.skipped.map(skippedRow => {
            // ------------------------------------
            errors.push(customErrorFactory.getError('RowSkippedWarning',
              [
                `Row ${skippedRow}`,
                `1000 rows`,
                `${skippedRow},`
              ]
            ))
          });
        }

        console.info(`Post processed business errors: ${JSON.stringify(errors)}`);

        // Review upload status at this point for reference at subsequent validation stages
        uploadStatus = reviewUploadStatus('BUSINESS', errors, adminApproved);

        banderUploadUpdateResultset = await db.bander_uploads.update(
          { 'id = ': banderUploadId } ,
          {
            errors: JSON.stringify(errors),
            warnings_count: errors.filter(error => error.severity === 'WARNING').length,
            criticals_count: errors.filter(error => error.severity === 'CRITICAL').length,
            upload_status: uploadStatus
          }
        );

        resultSet = banderUploadUpdateResultset;

        if (!['PASS', 'WARNINGS', 'ADMIN_APPROVED'].includes(uploadStatus)) {
          throw new Error('Critical Business Errors');
        }

        if (adminApproved) {
          console.info('Admin approved spreadsheet, preparing for database upload');

          resultSet = await generateSpreadsheetDbPayload(db, eventBatch.events, banderUploadUpdateResultset);

          console.info(`Prepared DB payload: ${JSON.stringify(resultSet)}`);

          resultSet = await postSpreadsheetToDB(db, resultSet);

          console.info(`Upload resultset: ${JSON.stringify(resultSet)}`);

          banderUploadUpdateResultset = await db.bander_uploads.update(
            { 'id = ': banderUploadId } ,
            {
              upload_status: 'PUSHED_TO_DATABASE'
            }
          );
        }
      }
      catch (e) {
        // ---------------------------------------------
        if (!['Spreadsheet File Format Error', 'Schema Validation Errors', 'Critical Business Errors'].includes(e.message)) {
          console.error('Error during spreadsheet processing - update status and escalate error for response');
          banderUploadUpdateResultset = await db.bander_uploads.update(
            { 'id = ': banderUploadId } ,
            {
              upload_status: 'PROCESS_ERROR',
              errors: JSON.stringify([customErrorFactory.getError('ValidationProcessError', [
                `${process.env.APPLICATION_DOMAIN}/data-uploads/spreadsheet-detail/${banderUploadId}`,
                `,`
              ])]),
              warnings_count: 0,
              criticals_count: 1
            }
          );
        }
        throw e;
      }
    }
  }
  catch (e) {
    if (['Spreadsheet File Format Error', 'Schema Validation Errors', 'Critical Business Errors'].includes(e.message)) {
      console.info(e.message);
      statusCode = 400;
      resultSet = { errors: errors, banderUploadUpdateResultset: banderUploadUpdateResultset };
    }
    else {
      console.log("ERROR");
      console.dir(e);
      statusCode = 500;
      resultSet = { errors: [e], banderUploadUpdateResultset: banderUploadUpdateResultset };
    }
  }

  return cb(null, {
    "statusCode": statusCode,
    "headers": {
      "Access-Control-Allow-Origin": "*", // Required for CORS support to work
      "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
    },
    "body": JSON.stringify(resultSet),
    "isBase64Encoded": false
  });
}
