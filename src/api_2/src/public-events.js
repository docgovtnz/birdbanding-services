'use strict';

// Modules
const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const MomentTimezone = require('moment-timezone');
const uuidv4 = require('uuid/v4');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBSSHelpers = require('bb-spreadsheet-helpers');
const BBHelpers = require('bb-helpers');
const Helpers = require('helpers.js')
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;

const RESOURCE_NAME = "public-event";
const PUBLIC_SIGHTING_UI_FIELD_MAPPING = {
  comments: {
    display: 'Comments'
  },
  contact_email: {
    display: 'Email'
  },
  contact_person_name: {
    display: 'Name'
  },
  count_event_media: {
    display: 'Number of Media Requested'
  },
  event_timestamp: {
    display: 'Date and Time Sighted'
  },
  event_type: {
    display: 'Event Type'
  },
  follow_up_requested: {
    display: 'Follow Up Requested'
  },
  id: {
    display: 'Public Event ID'
  },
  latitude: {
    display: 'Latitude'
  },
  location_comment: {
    display: 'Region'
  },
  location_description: {
    display: 'Location Description'
  },
  longitude: {
    display: 'Longitude'
  },
  other_mark_comments: {
    display: 'Other Mark Comments'
  },
  out_condition_code: {
    display: 'Condition of Bird'
  },
  out_status_code: {
    display: 'Status of Bird'
  },
  public_event_status: {
    display: 'Status'
  },
  row_creation_timestamp: {
    display: 'Created Date'
  },
  species_id: {
    display: 'Species ID'
  },
  mark_configuration: {
    display: 'Mark Configuration'
  },
  google_maps_view: {
    display: 'Location Preview',
    prefix: 'https://www.google.co.nz/maps?q='
  }
};

const DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE = 'Unknown/Unspecified';


const putPresignedURL = (payload) => {
  // ----------------------------------------------------------------------------   
  return new Promise((resolve, reject) => { 
    console.info(RESOURCE_NAME + ".getPresignedURL()");

    var s3 = new AWS.S3({ signatureVersion:'v4' });

    let params = {
      Bucket: process.env.USER_ASSETS_BUCKET,
      Key: payload.object_path
    };

    console.info('[INFO] presigned url request params: ', JSON.stringify(params));

    let url = s3.getSignedUrl('putObject', params);

    let response = payload;

    response.presigned_url = url;

    resolve(response);
  });  
}

const generatePublicEventMediaPresignedUrls = (payload) => {
  // ----------------------------------------------------------------------------   
  let promises = [];
  let response = {
    ...payload
  };

  promises = payload.public_event_media.map(mediaItem => {
    // -------------------------------------------

      return putPresignedURL(mediaItem);
  });

  return Promise.all(promises)
    .then(res => {
      console.log(res);

      response.public_event_media = res;
      return response;
    });
}

const preprocessPublicEvent = (payload, public_event_status = 'RAW') => {
  // ----------------------------------------------------------------------------   

  let response = { ...payload };

  // Add the public_event_status
  response.public_event.public_event_status = public_event_status;

  // Format mark_configuration as nested JSONB value
  response.public_event.mark_configuration = JSON.stringify([ ...payload.mark_configuration ]);
  delete response.mark_configuration;

  // Add nominal public_event_media array if not present
  return response;
}

const putPublicEventToDB = (db, payload) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".putPublicEventToDB()");

  // Prepare public_event_media for deep insert
  // Generate public_event_media payload
  let public_event_media = new Array(payload.public_event.count_event_media_requested).fill(0);

  payload.public_event.public_event_media = public_event_media.map(mediaItem => {
    // ----------------------------------------------
    mediaItem = {
      public_event_id: undefined, // must be explicitly undefined for a deep insert, refer massivejs docs
      storage_host: process.env.USER_ASSETS_BUCKET,
      upload_status: `REQUESTED`
    };
    return mediaItem;
  });

  // Cleanup public_event_media if no media being provided
  if (payload.public_event.public_event_media.length === 0) {
    delete payload.public_event.public_event_media;
  }

  // Insert the component records within a transaction so we can rollback on failure
  return db.public_event.save(payload.public_event, {
    deepInsert: true
  });
}

const getPublicEventFromDB = (id) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getPublicEventFromDB()");

  let criteria = {};
  if (id) {
    let idOperation = 'id =';
    criteria[idOperation] = id;

    console.log('getting id: ', id);

    return db.public_event.join(
      {
        pk: 'id',
        public_event_media: {
          type: 'LEFT OUTER',
          pk: 'id',
          on: { 'public_event_id': 'public_event.id' }
        }
    }
    )
    .find(criteria);
  }
  
  return [];
}

const formatResponse = (method = 'search', res, queryStringParameters = null) => {
  // ----------------------------------------------------------------------------    
  let response;

  switch (method) {
    case 'get': {
      return res;
    }
    case 'post': {
      // Flatten the object's mark_configuration and public_event_media
      response = {};
      response.public_event = { ...res };
      response.mark_configuration = ('public_event_media' in res) ? [ ...res.mark_configuration] : [];
      response.public_event_media = ('public_event_media' in res) ? [ ...res.public_event_media] : [];
      delete response.public_event.mark_configuration;
      delete response.public_event.public_event_media;
      return response;
    }
    case 'put': {
      return res;
    }
    case 'search':
    default: {
      return res;
    }
  }
}

const sendEmailUpdateToAdmin = async (db, payload) => {
  // -----------------------------------------------
  console.log('sendEmailUpdateToAdmin()');

  let speciesResultset = await db.species.find({ 'id =': payload.public_event.species_id });

  let envDisplay = '';

  switch(process.env.ENVIRONMENT) {
    case 'dev':
    case 'test':
    case 'uat':
      envDisplay = `${process.env.ENVIRONMENT.toUpperCase()} `;
      break;
    default:
      break;
  }

  var ses = new AWS.SES();

  var params = {
    Destination: {
     ToAddresses: [
        process.env.ADMIN_EMAIL_ADDRESS
     ]
    }, 
    Message: {
     Body: {
      Html: {
       Charset: "UTF-8", 
       Data: `<h2 style="color: #194036;">${envDisplay}NEW public ${ (speciesResultset.length > 0) ? speciesResultset[0].common_name_nznbbs : 'unknown species' } sighting from Falcon - ${MomentTimezone(payload.public_event.row_creation_timestamp_).tz('NZ').format('DD-MM-YYYY HH:mm:ss')} NZT</h2>
              <h2 style="color: #2e6c80;">Summary:</h2>
              <p>${payload.public_event.event_type.charAt(0).toUpperCase() + payload.public_event.event_type.slice(1).toLowerCase().split('_').join(' ')} of ${(speciesResultset.length > 0) ? speciesResultset[0].common_name_nznbbs : 'unknown species' } at ${MomentTimezone(payload.public_event.event_timestamp).tz('NZ').format('DD-MM-YYYY HH:mm:ss')} NZT</p>
              <p>Public Event ID: ${payload.public_event.id}, Status: ${payload.public_event.public_event_status}</p>
              <p>Follow up requested: ${payload.public_event.follow_up_requested}</p>
              <p>Number of photos uploaded: ${payload.public_event_media.length}</p>
              <p>Name: ${payload.public_event.contact_person_name}</p>
              <p>Email: ${payload.public_event.contact_email}</p>
              <h2 style="color: #2e6c80;">Species and Mark Configuration:</h2>
              <h3>Species: ${ (speciesResultset.length > 0) ? speciesResultset[0].common_name_nznbbs : 'unknown species' }${ (speciesResultset.length > 0) ? `, ${speciesResultset[0].scientific_name_nznbbs}` : '' }</h3>
              <p>Status Code: ${ (payload.public_event.out_status_code) ? payload.public_event.out_status_code : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
              <p>Condition Code: ${ (payload.public_event.out_condition_code) ? payload.public_event.out_status_code : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
              <p>Mark Configuration:</p>
              <ul>${(!payload.mark_configuration || payload.mark_configuration.length == 0) ? DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE: payload.mark_configuration.map(markConfig => {
                  return `<li style="padding: 0px; border: 0px; margin-bottom: 10px;">
                            <p style="padding: 0px; border: 0px; margin: 0px;">Mark type: ${ (markConfig.mark_type) ? markConfig.mark_type : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
                            <p style="padding: 0px; border: 0px; margin: 0px;">Side: ${ (markConfig.side) ? markConfig.side : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
                            <p style="padding: 0px; border: 0px; margin: 0px;">Position: ${ (markConfig.position) ? markConfig.position : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
                            <p style="padding: 0px; border: 0px; margin: 0px;">Position-Index: ${ (markConfig.location_idx === 0 || markConfig.location_idx) ? markConfig.location_idx : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
                            <p style="padding: 0px; border: 0px; margin: 0px;">Colour: ${ (markConfig.colour) ? markConfig.colour : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
                            <p style="padding: 0px; border: 0px; margin: 0px;">Alphanumeric-Text: ${ (markConfig.alphanumeric_text) ? markConfig.alphanumeric_text : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
                            <p style="padding: 0px; border: 0px; margin: 0px;">Text-Colour: ${ (markConfig.text_colour) ? markConfig.text_colour : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
                            <p style="padding: 0px; border: 0px; margin: 0px;">Material: ${ (markConfig.mark_material) ? markConfig.mark_material : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
                          </li>`
                }).join('')}
              </ul>
              <p>Additional Mark Information: ${ (payload.public_event.other_mark_comments) ? payload.public_event.other_mark_comments : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</p>
              <h2 style="color: #2e6c80;">Location:</h2>
              <p><a href="${PUBLIC_SIGHTING_UI_FIELD_MAPPING.google_maps_view.prefix + payload.public_event.latitude + '%2C%20' + payload.public_event.longitude }">Location Preview</a></p>
              <p>Latitude: ${payload.public_event.latitude}</p>
              <p>Longitude: ${payload.public_event.longitude}</p>
              <p>Region: ${payload.public_event.location_comment}</p>
              <p>Location Description: ${payload.public_event.location_description}</p>
              <p style="color: #2e6c80;"><span style="color: #000000;">Other comments: ${ (payload.public_event.comments) ? payload.public_event.comments : DEFAULT_UNKNOWN_PUBLIC_EVENT_VALUE }</span><strong>&nbsp;</strong></p>`
      }
     }, 
     Subject: {
      Charset: "UTF-8", 
      Data: `${envDisplay}NEW Public ${ (speciesResultset.length > 0) ? speciesResultset[0].common_name_nznbbs : 'unknown species' } Sighting from Falcon`
     }
    },
    Source: process.env.ADMIN_EMAIL_ADDRESS
   };
   
   return ses.sendEmail(params).promise()
    .then(res => {
      console.info(JSON.stringify(res));
      return payload;
    })
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

  // Custom Errors
  var customErrorsList = {};

  // Payload
  var payload = JSON.parse(event.body);

  // Invocation claims
  var claims = null;

  // Event Batch
  var rawEventBatch = {};
  var eventBatch = {};

  // Lookup Data
  var lookupData = {};

  // Response Object
  var response = {};  

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
      console.info('Event batch payload OK. Completing payload pre-processing...');
      return preprocessPublicEvent(payload);
    })
    .then(res => {
      payload = res;
      console.info('Preprocessing complete. Connecting to DB...');
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Connected to DB. Validating upload access...');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Assume stub represents post db update
      console.log(`Putting payload to DB: ${JSON.stringify(payload)}`);
      return putPublicEventToDB(db, payload)
    })
    .then(res => {
      console.log(res);
      console.log(res.id);
      return getPublicEventFromDB(res.id);
    })
    .then(res => {
      console.log(res);
      console.log(res[0]);
      return generatePublicEventMediaPresignedUrls(res[0]);
    })
    .then(res => {
      return formatResponse('post', res, null)
    })
    .then(res => {
      return sendEmailUpdateToAdmin(db, res);
    })
    .then(res => {
      console.log(res);
      response = res;

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
        "body": JSON.stringify(err),
        "isBase64Encoded": false
      });
    });
};