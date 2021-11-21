
const Promise = require('bluebird');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
var BanderCertifications = require('./bander-certifications-store');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;
AWS.config.setPromisesDependency(require('bluebird'));

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

let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "logging";

// DELETE
module.exports.delete = (event, context, cb) => {
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

  // JSON Schemas
  var paramSchema = {};

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(() => {
      return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
    })
    // Validate Path parameters
    .then(schema => {
      paramSchema = schema;
      console.info('Group membership authorisation OK. Validating path parameters...');
      return BoilerPlate.validateJSON(event.pathParameters || {}, paramSchema);
    })
    .then(errors => {
        if (errors.length) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
      }
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(async dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      const result = await BanderCertifications.delete(event.pathParameters.banderCertificationsId, db);
      console.log("RESULT")
      console.log(result)
      if(!result) {
        throw new BoilerPlate.ServiceError("Certification not found", [], 404)
      }
      else{
        return result;
      };
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
      console.info("DELETE ERROR");
      console.debug(err);
      console.dir(err);
      cb(null, {
        "statusCode": err.statusCode || 500,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(err.message || err),
        "isBase64Encoded": false
      });
    });
};


// POST
module.exports.post = (event, context, cb, banderCertificationsId = 0) => {
  console.debug("________" + banderCertificationsId ? "PUTTING" : "POSTING" + '_______');
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

  var customErrorsSchemaParams = {
    table: process.env.CUSTOM_ERROR_LIST_TABLE,
    id: process.env.CUSTOM_ERROR_LIST_ID,
    version: Number(process.env.CUSTOM_ERROR_LIST_VERSION)
  }  

  console.log('DEBUG: ', JSON.stringify(event));

  // Payload
  var payload = JSON.parse(event.body);

  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(async () => {
      console.info("Before schema call")

      let customErrors = await BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);

      let validationCalls = [async function validatePayload() {
        console.info("validation payload")
        let payloadSchema = await BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
        return await BoilerPlate.validateJSON(payload, payloadSchema);
      }()]

      if (banderCertificationsId) {
        console.info("validation params")
        validationCalls.push(async function validateSchama() {
          let paramSchema = await BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
          return await BoilerPlate.validateJSON(event.pathParameters || {}, paramSchema);
        }());
      }

      return Promise.all(validationCalls)
        .then((result) => {
          console.info(result);
          if (result[0].length) {
            let formattedErrors = formatSchemaErrors(customErrorFactory, result[0]);
            throw new BoilerPlate.ParameterValidationError(`${result[0].length}  Payload validation error(s)!`, formattedErrors);
          }
          if (result && result.length > 1 && result[1].length) {
            throw new BoilerPlate.ParameterValidationError(`${result[1].length}  Parameter validation error(s)!`, result[1]);
          }
        })
    }).then(() => {
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(async dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      console.info(event.pathParameters)
      if (event.pathParameters && event.pathParameters.banderCertificationsId) {
        payload.id = event.pathParameters.banderCertificationsId;
      }

      let bc = new BanderCertifications(payload, db);
      await bc.save();
      if (bc.errors.length) {
        // Wrap to match uniform error handling pattern
        let errors = bc.errors.map(error => {
          switch(error) {
            case "A record for this bander and endorsement/species group already exists":
              return customErrorFactory.getError('UserPropertyExistsError', ['certification', payload.certification, '/certification']);
            case "That species group id or bander id doesn't exist":
              return customErrorFactory.getError('CertSpeciesGroupOrBanderIdNotFound', [`That species group id or bander id doesn't exist`, `${payload.certification},${payload.bander_id}`, '/certification,/bander_id']);
            default:
              return customErrorFactory.getError('UnexpectedSchemaError', ['bander_certifications', '', 'bander_certifications -> on save', '']);
          }
        })
        throw new BoilerPlate.ParameterValidationError("Validation error", errors, 422);
      }
      else if (bc.notFound) {
        throw new BoilerPlate.ServiceError("Not found", ["No bander certification record was found with that id"], 404);
      }
      else {
        return bc.newRecord;
      }
    })
    .then(res => {
      console.log(banderCertificationsId);
      console.debug("______________Response object (no errors)_________________");
      console.dir(res);
      console.debug("__________________________________________________________");
      cb(null, {
        "statusCode": banderCertificationsId ? 200 : 201, // 200 is easier to test, 204 returns no content
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify(res),
        "isBase64Encoded": false
      });
    })
    .catch(err => {
      console.debug(`_________ Error: statusCode: ${err.statusCode}, details: ${err.details} ____________`);
      console.dir(err);
      console.debug("____________________________________________________________________________________");
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

module.exports.put = (event, context, cb) => {
  console.log(JSON.stringify(event));
  module.exports.post(event, context, cb, 
      (typeof event !== 'undefined'
        && 'pathParameters' in event
        && typeof event.pathParameters !== 'undefined' 
        && 'banderCertificationsId' in event.pathParameters
        && typeof event.pathParameters.banderCertificationsId !== 'undefined'
      ) ? event.pathParameters.banderCertificationsId : null);
}

module.exports.search = async (event, context, cb) => {
  console.debug("___________________________________SEARCHING_______________________________________________");
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


  let statusCode, returnMsgs = [], paramSchema, q = event.queryStringParameters;

  try {
    await BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST);

    let dbAccess = await DBAccess.getDBConnection(db, dbCreationTimestamp);
    dbCreationTimestamp = dbAccess.dbCreationTimestamp;

    try {
      paramSchema = await BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
    } catch (e) {
      throw (new BoilerPlate.ServiceError("Schema retrieval error", e))
    }

    let validationErrors = await BoilerPlate.validateJSON(q ? q : {}, paramSchema);
    if (validationErrors.length) {
      throw new BoilerPlate.ParameterValidationError("Validation error", validationErrors);
    }

    let resultSet = await BanderCertifications.search(q, dbAccess.db);

    console.log("Result set___________________")
    console.log(resultSet)

    statusCode = 200;
    returnMsgs = resultSet;
  }

  catch (e) {
    if (e instanceof BoilerPlate.AuthorisationError) {
      statusCode = 400;
      returnMsgs = e.message;
    }
    if (e instanceof BoilerPlate.ParameterValidationError) {
      statusCode = 422;
      returnMsgs = [e];
    }
    else {
      statusCode = 500;
      returnMsgs = "Server error";
    }
    console.error(e);
  }
  cb(null, {
    "statusCode": statusCode || 500,
    "headers": {
      "Access-Control-Allow-Origin": "*", // Required for CORS support to work
      "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
    },
    "body": JSON.stringify(returnMsgs),
    "isBase64Encoded": false
  });
}

module.exports.get = async (event, context, cb) => {
  console.debug("_________________________________GET By id____________________________________________");
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


  let statusCode, returnMsgs = [], paramSchema, q = event.queryStringParameters;

  try {
    await BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST);

    let dbAccess = await DBAccess.getDBConnection(db, dbCreationTimestamp);
    db = dbAccess.db;
    dbCreationTimestamp = dbAccess.dbCreationTimestamp;

    try {
      paramSchema = await BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version);
    } catch (e) {
      throw (new BoilerPlate.ServiceError("Schema retrieval error", e));
    }

    let validationErrors = await BoilerPlate.validateJSON(q ? q : {}, paramSchema);
    if (validationErrors.length > 0) {
      throw new BoilerPlate.ParameterValidationError("Validation error", validationErrors);
    }

    validationErrors = await BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, paramSchema);
    if (validationErrors.length > 0) {
      throw new BoilerPlate.ParameterValidationError("Validation error", validationErrors);
    }

    // Verify that a bander exists (this determines whether 404 or 200 with empty array is returned)
    const banderId = event.pathParameters.banderId;
    let isBanderRes = await db.ro_is_bander(banderId);
    if (!isBanderRes[0].ro_is_bander) {
      throw new BoilerPlate.NotFoundError('BanderCertificationsNotFound', { type: 'NOT_FOUND', message: `Bander not found for id: ${banderId}`, data: { path: `pathParameters.banderId` }, value: banderId , schema: null });
    }

    let resultSet = await BanderCertifications.search({banderId: banderId}, dbAccess.db);
    console.log("Result set___________________")
    console.log(resultSet)
    statusCode = 200;
    returnMsgs = resultSet;
  }
  catch (e) {
    if (e instanceof BoilerPlate.AuthorisationError) {
      statusCode = e.statusCode;
      returnMsgs = e;
    }
    else if (e instanceof BoilerPlate.NotFoundError) {
      statusCode = e.statusCode;
      returnMsgs = e;
    }
    else if (e instanceof BoilerPlate.ServiceError) {
      statusCode = e.statusCode;
      returnMsgs = e;
    }
    else if (e instanceof BoilerPlate.ParameterValidationError) {
      statusCode = e.statusCode;
      returnMsgs = e;
    }
    else {
      statusCode = 500;
      returnMsgs = "Server error";
    }
  }
  cb(null, {
    "statusCode": statusCode || 500,
    "headers": {
      "Access-Control-Allow-Origin": "*", // Required for CORS support to work
      "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
    },
    "body": JSON.stringify(returnMsgs),
    "isBase64Encoded": false
  });
}