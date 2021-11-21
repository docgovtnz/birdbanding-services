'use strict';

const Promise = require('bluebird');
const AWS = require('aws-sdk');
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
var AWSCaptureAWS = AWSXRay.captureAWS(require('aws-sdk'));
AWSCaptureAWS.config.setPromisesDependency(require('bluebird'));


// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "cms-attachment";
const s3 = new AWS.S3({
  signatureVersion: 'v4'
});

const completePostAction = async (db, event) => {
  // -------------------------------
  console.info(RESOURCE_NAME + '.completePostAction()');

  let queryStringParameters = event.queryStringParameters;

  if (queryStringParameters &&
    'presignedUrl' in queryStringParameters &&
    queryStringParameters.presignedUrl.toLowerCase() === 'true') {
    return getPutAttachmentSignedUrl(event);
  }

  return postDetailsDb(db, event);
}


const completePutAction = async (db, event) => {
  // -------------------------------
  console.info(RESOURCE_NAME + '.completePostAction()');

  let queryStringParameters = event.queryStringParameters;

  if (queryStringParameters &&
    'presignedUrl' in queryStringParameters &&
    queryStringParameters.presignedUrl.toLowerCase() === 'true') {
    return getPutAttachmentSignedUrl(event);
  }

  return putDetailsDb(db, event);
}


const getPutAttachmentSignedUrl = async (event) => {
  // ---------------------------------
  console.info(RESOURCE_NAME + '.getSignedUrl()');

  let payload = JSON.parse(event.body);

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET + '/public/cms-attachments',
    Key: payload.fileName
  };

  console.info('[INFO] presigned url request params: ', JSON.stringify(params));

  let url = await s3.getSignedUrl(`putObject`, params);

  return {
    ...payload,
    presignedUrl: url
  };
}

const postDetailsDb = async (db, event) => {
  // ---------------------------------
  console.info(RESOURCE_NAME + '.postAttachmentDb()')

  let payload = JSON.parse(event.body);
  payload.object_path = `public/cms-attachments/${payload.fileName}`;
  payload.storage_host = process.env.USER_ASSETS_BUCKET;

  let attachment = await db.cms_attachment.insert(payload);
  console.info('stringify result' + JSON.stringify(attachment));

  return attachment;
}

const putDetailsDb = async (db, event) => {
  console.info(RESOURCE_NAME + '.putAttachmentDb()')

  let payload = JSON.parse(event.body);
  payload.object_path = `public/cms-attachments/${payload.fileName}`;
  payload.storage_host = process.env.USER_ASSETS_BUCKET;

  let attachment = await db.cms_attachment.update({
    'id': event.pathParameters.attachmentId
  }, payload)

  console.info('stringify result' + JSON.stringify(attachment))

  return attachment;
}

const getAttachmentS3 = async (event) => {
  let payload = JSON.parse(event.body)

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET + '/public/cms-attachments',
    Key: payload.fileName
  };

  let getFile = await s3.getObject(params)

  return getFile
}


const getAttachmentDb = async (db, event) => {
  // ------------------------------------------
  console.info(RESOURCE_NAME + '.getAttachmentDb()');

  let attachmentDetails = await db.cms_attachment.find(event.pathParameters.attachmentId);
  return attachmentDetails;
}

const searchAttachmentsDb = async (db, event) => {
  console.info(RESOURCE_NAME + ' searcgAttachmentDb()')

  let fileName = (event.queryStringParameters &&
    'fileName' in event.queryStringParameters &&
    typeof event.queryStringParameters.fileName !== 'undefined') ?
    event.queryStringParameters.fileName : null;
  console.info('fileName:' + fileName)

  let criteria = {};
  if (fileName) {
    criteria["object_path LIKE"] = `%${fileName}%`;
  } else {
    criteria["object_path IS NOT"] = null;
  }

  let attachmentResultset = await db.cms_attachment.find(criteria)

  return attachmentResultset
}


const validatePostBusinessRules = async (customErrorFactory, db, event, governingCognitoGroup) => {
  // -----------------------------------------------------
  console.info(RESOURCE_NAME + '.validatePostBusinessRules()');

  let promises = [];
  let queryStringParameters = event.queryStringParameters;

  if (queryStringParameters &&
    'presignedUrl' in queryStringParameters &&
    queryStringParameters.presignedUrl === 'true') {
    // -------------------------------------------------------
    // Validate object path does not already exist in the datastore
    // -------------------------------------------------------
    promises.push(validateObjectDoesNotExistInDb(customErrorFactory, db, event));

    // -------------------------------------------------------
    // Validate object path does not exists in S3
    // -------------------------------------------------------
    promises.push(validateObjectDoesNotExistsInS3(customErrorFactory, db, event));
  }

  if (!queryStringParameters ||
    !'presignedUrl' in queryStringParameters ||
    queryStringParameters.presignedUrl === 'false') {
    // -------------------------------------------------------
    // Validate object path does not already exist in the datastore
    // -------------------------------------------------------
    promises.push(validateObjectDoesNotExistInDb(customErrorFactory, db, event));

    // -------------------------------------------------------
    // Validate object path exists in S3(ie. upload via presigned url was succesful)
    // -------------------------------------------------------
    promises.push(validateObjectExistsInS3(customErrorFactory, db, event));
  }

  let errors = await Promise.all(promises);

  return errors.filter(error => error);
}


const validatePutBusinessRules = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------------------
  console.info(RESOURCE_NAME + '.validatePutBusinessRules()');

  let promises = [];
  let queryStringParameters = event.queryStringParameters;

  if (!queryStringParameters ||
    'presignedUrl' in queryStringParameters || queryStringParameters.presignedUrl === 'true') {
    // -------------------------------------------------------
    // Validate object path exists in S3
    // -------------------------------------------------------
    promises.push(validateObjectExistsInS3(customErrorFactory, db, event));
  }

  let errors = await Promise.all(promises);

  return errors.filter(error => error);
}

const validateDeleteBusinessRules = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------------------
  console.info(RESOURCE_NAME + '.validateDeleteBusinessRules()');

  let promises = [];
  let queryStringParameters = event.queryStringParameters;
  // validate file details exist in Db
  promises.push(validateAttachmentExists(customErrorFactory, db, event, claims))
  // -------------------------------------------------------
  // Validate object path exists in S3
  // -------------------------------------------------------
  //  promises.push(validateObjectExistsInS3(customErrorFactory, db, event));


  let errors = await Promise.all(promises);

  return errors.filter(error => error);
}


const validateObjectDoesNotExistsInS3 = async (customErrorFactory, db, event) => {
  console.info(RESOURCE_NAME + '.validateObjectDoesNotExistsInS3()');

  let payload = JSON.parse(event.body);

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: `public/cms-attachments/${payload.fileName}`
  };

  console.log('params' + JSON.stringify(params));

  try {
    let isObjectPath = await s3.headObject(params).promise();
    console.log('isObjectPath:' + isObjectPath);
    return customErrorFactory.getError('ObjectAlreadyInS3', [payload.fileName, '/fileName']);

  } catch (err) {
    return null;
  }


}
const validateObjectExistsInS3 = async (customErrorFactory, db, event) => {
  // --------------------------------------------------------
  console.info(RESOURCE_NAME + '.validateObjectExistsInS3()');

  let payload = JSON.parse(event.body);

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: `public/cms-attachments/${payload.fileName}`
  };

  console.log('params' + JSON.stringify(params));

  try {
    let isObjectPath = await s3.headObject(params).promise();
    console.log('isobjectPAth result =' + JSON.stringify(isObjectPath));
    return null;
  } catch (err) {
    return customErrorFactory.getError('ObjectNotInS3', [payload.fileName, '/fileName']);
  }
}

const validateObjectDoesNotExistInDb = async (customErrorFactory, db, event) => {
  // --------------------------------------------------------
  console.info(RESOURCE_NAME + '.validateObjectDoesNotExistInDb()');

  let payload = JSON.parse(event.body)

  let isObjectResultSet = await db.ro_is_object_path(`public/cms-attachments/${payload.fileName}`);

  if (isObjectResultSet[0].ro_is_object_path) {
    return customErrorFactory.getError('ObjectPathAlreadyExists', [payload.fileName, '/fileName', ' in the datastore and contains fileName']);
  }
  return null
}

const validateAttachmentExists = async (customErrorFactory, db, event) => {
  // -----------------------------------------
  console.info(RESOURCE_NAME + ' validateAttachmentExists()')

  let IsAttachment = await db.ro_is_attachment(event.pathParameters.attachmentId)
  console.info('does attachment exist in db? ' + JSON.stringify(IsAttachment[0].ro_is_attachment))
  if (!IsAttachment[0].ro_is_attachment) {
    return customErrorFactory.getError('NotFoundError', ['attachmentId', event.pathParameters.attachmentId, 'pathParameters.attachmentId']);

  }
  return null
}


const completeDeleteAction = async (db, event) => {
  // ----------------------------------------------
  console.info(RESOURCE_NAME + ' deleteAttachment()');

  let id = event.pathParameters.attachmentId
  let record = await db.cms_attachment.find(id)
  console.log('record  =' + record)
  let fileName = record.object_path.substring(record.object_path.lastIndexOf("/") + 1)
  console.log('fileName from db object_path =' + fileName)

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: `public/cms-attachments/${fileName}`
  }
  let deleteAttachmentDb;
  let recordActionUpdate
  let deletionResult = {}
  console.log('deletion params' + JSON.stringify(params));
  // delete file in s3
  try {
    let s3DeleteResult = await s3.deleteObject(params).promise();
    console.log(`S3 deletion result: ${JSON.stringify(s3DeleteResult)}`);
    console.log('s3DeleteResult.DeleteMarker=' + s3DeleteResult.DeleteMarker)
    console.log('s3DeleteResult.DeleteMarker stringify=' + JSON.stringify(s3DeleteResult.DeleteMarker))

    if (s3DeleteResult.DeleteMarker === true) {
      console.log('now inside s3DeleteResult.DeleteMarker === true block ')
      console.info('id:' + id)

      deleteAttachmentDb = await db.cms_attachment.destroy(id);
      recordActionUpdate = await db.record_action.insert({
        db_action: 'DELETE',
        db_table: 'cms_attachment',
        db_table_identifier_name: 'id',
        db_table_identifier_value: id
      });

      console.info('stringify deleteAttachmentDb' + JSON.stringify(deleteAttachmentDb));
      console.info(`https://${deleteAttachmentDb.storage_host}.s3-ap-southeast-2.amazonaws.com/${deleteAttachmentDb.object_path}`);

      // Replace all links to this attachemnt in the cms_content table to redirect to the not-found page
      let replacementHref = `https://${deleteAttachmentDb.storage_host}.s3-ap-southeast-2.amazonaws.com/${deleteAttachmentDb.object_path}`;
      let replaceLinksResultset = await db.rw_remove_deleted_links_from_content(replacementHref);

      console.info(replaceLinksResultset);
    }

    deletionResult = {
      ...s3DeleteResult,
      ...deleteAttachmentDb,
      recordActionUpdate
    };
    console.log('deletionResult=' + JSON.stringify(deletionResult))

    return deletionResult

  }
  catch (error) {
    console.error(error);
    throw new BoilerPlate.ServiceError(`Error deleting from S3!`, error);
  }

}


const formatResponse = (event, method = 'get', res) => {
  // ----------------------------------------------------------------------------    
  console.info('stringify before format result' + JSON.stringify(res));

  switch (method) {
    case 'get': {
      console.info('fileNameResponse=' + res.object_path.substring(res.object_path.lastIndexOf("/") + 1))
      return {
        ...res,
        fileName: res.object_path.substring(res.object_path.lastIndexOf("/") + 1),
        attachment_permalink: `https://${res.storage_host}.s3-ap-southeast-2.amazonaws.com/${res.object_path}`
      };
    }
    case 'search': {
      return res.map(attachment => {
        return {
          ...attachment,
          fileName: attachment.object_path.substring(attachment.object_path.lastIndexOf("/") + 1),
          attachment_permalink: `https://${attachment.storage_host}.s3-ap-southeast-2.amazonaws.com/${attachment.object_path}`
        }
      });
    }
    case 'post': {
      if (event.queryStringParameters &&
        'presignedUrl' in event.queryStringParameters &&
        event.queryStringParameters.presignedUrl.toLowerCase() === 'true') {
        return res;
      }
      console.info('fileNameResponse=' + res.object_path.substring(res.object_path.lastIndexOf("/") + 1))
      return {
        ...res,
        fileName: res.object_path.substring(res.object_path.lastIndexOf("/") + 1),
        attachment_permalink: `https://${res.storage_host}.s3-ap-southeast-2.amazonaws.com/${res.object_path}`
      }
    }
    case 'put': {
      if (event.queryStringParameters &&
        'presignedUrl' in event.queryStringParameters &&
        event.queryStringParameters.presignedUrl.toLowerCase() === 'true') {
        return res;
      }
      return {
        ...res[0],
        fileName: res[0].object_path.substring(res[0].object_path.lastIndexOf("/") + 1),
        attachment_permalink: `https://${res[0].storage_host}.s3-ap-southeast-2.amazonaws.com/${res[0].object_path}`
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
  console.info(RESOURCE_NAME + ' post (deployed api_2 stack tuesday 9th june 5:11pm)')
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
      console.info('Validating business rules...latest update change object_path to fileName');
      return validatePostBusinessRules(customErrorFactory, db, event, governingCognitoGroup);
    })
    .then(errors => {
      // ---------------------
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length}  business validation error(s)!`, errors);
      }
      return completePostAction(db, event);
    })
    .then(res => {
      // ---------------------
      return formatResponse(event, 'post', res);
    })
    .then(res => {
      console.info("Returning with no errors");
      cb(null, {
        "statusCode": (event.queryStringParameters &&
          'presignedUrl' in event.queryStringParameters &&
          event.queryStringParameters.presignedUrl.toLowerCase() === 'true') ? 200 : 201,
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

//PUT
module.exports.put = (event, context, cb) => {
  console.info(RESOURCE_NAME + ' put')
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
    // Handle errors / Validate business rules
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body schema validation error(s)!`, errors);
      }
      console.info('Payload parameters OK. Check/renew connection to DB');
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
      return validateAttachmentExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Attachment not found', error);
      console.info('Validating business rules...');
      return validatePutBusinessRules(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(errors => {
      // ---------------------
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length}  business validation error(s)!`, errors);
      }
      return completePutAction(db, event);
    })
    .then(res => {
      return formatResponse(event, 'put', res);
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
      //     // Get Custom Errors schema from Dynamo
      console.info('customerrorschemaparams' + JSON.stringify(customErrorsSchemaParams))
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
      return validateAttachmentExists(customErrorFactory, db, event);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Attachment not found', error);
      console.info('Getting attachment...')
      return getAttachmentDb(db, event);
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

  // JSON Schemas
  var paramSchema = {};

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
    // Handle errors / get customerror schema
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('QueryString parameters OK. Check/renew connection to DB');
      //     // Get Custom Errors schema from Dynamo
      console.info('customerrorschemaparams' + JSON.stringify(customErrorsSchemaParams))
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
      console.info('searching attachment by object path...');
      return searchAttachmentsDb(db, event);
    })
    .then(res => {
      return formatResponse(event, 'search', res);
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

//# DELETE
module.exports.delete = (event, context, cb) => {
  console.info(RESOURCE_NAME + ' delete')
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

  // Do the actual work
  BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(() => {
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
    // Handle errors / get customerrorschema
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Querystring parameter validation error(s)!`, errors);
      }
      console.info('QueryString parameters OK. Check/renew connection to DB');
      //     // Get Custom Errors schema from Dynamo
      console.info('customerrorschemaparams' + JSON.stringify(customErrorsSchemaParams))
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
      return validateDeleteBusinessRules(customErrorFactory, db, event);
    })
    .then(errors => {
      console.info("validateDeleteBusinessRules errors:" + JSON.stringify(errors))
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length}  business validation error(s)!`, errors);
      }
      return completeDeleteAction(db, event);
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