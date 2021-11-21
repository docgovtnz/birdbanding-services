'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
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

const RESOURCE_NAME = "projects";

//############# GET A PROJECT
const getDB = (db, id = null) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getDB()");
  //console.info('pathParameters:'+JSON.stringify(pathParameters))
  console.info('id =' + id)
  let criteria = {
    'vw_project_list.id = ': id
  };

  return db.vw_project_list
    .join({
      pk: 'id',
      project_bander_membership: {
        type: 'LEFT OUTER',
        pk: 'id',
        on: {
          'project_id': 'vw_project_list.id'
        },
        bander: {
          type: 'LEFT OUTER',
          pk: 'id',
          on: {
            'id': 'project_bander_membership.bander_id'
          },
          decomposeTo: 'object'
        }
      }
    })
    .find(criteria, {
      order: [{
        field: 'vw_project_list.row_creation_timestamp_',
        direction: 'desc'
      }],
    });
}
//############# GET ALL PROJECTS
const searchDB = (db, id = null) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".searchDB()");

  // We've either got a definite project ID to find, or we want all of them
  let idOperation = id ? 'id =' : 'id BETWEEN';
  let idValue = id ? id : ['00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff'];

  // Build this up when we get significantly fancier with the 
  // breadth of querystring parameters we support. Right now,
  // we just return the whole lot.
  let criteria = {};

  // THIS RETURNS A PROMISE
  return db.vw_project_list.find(criteria, {
    order: [{
      field: 'name',
      direction: 'asc'
    }]
  });


}

const validateBusinessRules = async (db, event) => {
  let payload = JSON.parse(event.body);
  let projectId = (event.pathParameters && 'projectId' in event.pathParameters && event.pathParameters.projectId) ? event.pathParameters.projectId : null;
  let promises = [];

  let projectNameUniquenessCheckRequired = true;
  let projectNameChangeRes = await db.ro_is_project_name_different(projectId, payload.name);
  if (event.httpMethod === 'PUT' && projectId) {
    projectNameUniquenessCheckRequired = projectNameChangeRes[0].ro_is_project_name_different;
  }
  console.log(projectNameUniquenessCheckRequired);

  // Validate project name uniqueness for new projects or updated projects with name changes
  promises.push(db.ro_is_project_name_unique(payload.name)
    .then(res => {
      console.info(`Will name violate uniqueness constraint? ${!res[0].ro_is_project_name_unique}`);

      if (projectNameUniquenessCheckRequired && !res[0].ro_is_project_name_unique) {
        // ------------------------------
        return new BoilerPlate.ParameterValidationError('ProjectNameExists',
          {
            type: 'BUSINESS',
            message: `A project with this name already exists`,
            data: {
              path: `[object Object]/name`,
              value: payload.name,
              schema: 'BUSINESS_VALIDATION'
            }
          });
      }
    })
  );

  // Validate project-id exists for HTTP PUT
  if (event.httpMethod === 'PUT') {
    promises.push(db.ro_is_project(event.pathParameters.projectId)
      .then(res => {
        console.info(`Project exists? ${res[0].ro_is_project}`);

        if (!res[0].ro_is_project) {
          // ------------------------------
          return new BoilerPlate.NotFoundError('ProjectNotFound',
            {
              type: 'NOT_FOUND',
              message: `Project cannot be found with this id`,
              data: {
                path: `pathParameters.projectId`,
                value: event.pathParameters.projectId,
                schema: null
              }
            });
        }
      })
    );
  }

  // Validate that coordinator id exists
  promises.push(db.ro_is_bander(payload.coordinator_id)
    .then(res => {
      if (!res[0].ro_is_bander) {
        throw new BoilerPlate.NotFoundError('CoordinatorNotFound', { type: 'NOT_FOUND', message: `Coordinator cannot be found with this bander id`, data: { path: `[object Object]/coordinator_id` }, value: payload.coordinator_id, schema: null });
      }
    }));

  return Promise.all(promises)
    .then(res => res.filter(error => error));
}

const createProjectDb = (db, payload) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".createProjectDb()");

  console.log(payload);

  return db.project.save(payload)
    .then(res => {
      // Successfull DB update, cleanup the response object and return!
      console.info('DB update complete: ', res);
      return res;
    })
    .catch(err => {
      console.log(err + ' ' + err.stack)
      throw new BoilerPlate.DataAccessError('DatabaseProjectCreateError', 'Unable to create project in the database.' + err.stack);
    });
}

const formatResponse = (method = 'search', res) => {
  // ----------------------------------------------------------------------------    

  let response = res;

  switch (method) {
    case 'get': {
      // Filter out any removed team members
      response = response.map(project => {
        project.project_bander_membership = project.project_bander_membership.filter(pbm => !pbm.is_deleted);
        return project;
      })
      return (response.length > 0) ? response[0] : {};
    }
    case 'search': {
      return response;
    }
    case 'put':
    case 'post': {
      return response;
    }
    default: {
      return response;
    }
  }
}

// ============================================================================
// API METHODS
// ============================================================================

//POST create new project
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

  let queryStringParameters = event.queryStringParameters;

  console.info('event=' + JSON.stringify(event));

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
      console.info('Group membership authorisation OK. Validating path parameters...');
      return BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, paramSchema);
    })
    // Handle errors / Validate querystring      
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
      }
      console.info('Path parameters OK. Validating querysting parameters...');
      return BoilerPlate.validateJSON(queryStringParameters ? queryStringParameters : {}, paramSchema);
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
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      console.info('Payload parameters OK. Connecting to DB to validate business rules...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Validate bander is not suspended
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      console.info('Validating business rules...');
      return validateBusinessRules(db, event);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      console.info('creating project');
      return createProjectDb(db, payload);
    })
    .then(res => {
      return formatResponse('post', res);
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


module.exports.put = (event, context, cb) => {
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

  console.info('event=' + JSON.stringify(event));

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
      console.info('Group membership authorisation OK. Validating path parameters...');
      return BoilerPlate.validateJSON(event.pathParameters ? event.pathParameters : {}, paramSchema);
    })
    // Handle errors / Validate querystring      
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Path parameter validation error(s)!`, errors);
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
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body schema validation error(s)!`, errors);
      }
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      console.info('Payload parameters OK. Connecting to DB to validate business rules...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Validate bander is not suspended
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      // Update project id in payload if provided to match the path parameter
      payload.id = event.pathParameters.projectId;
      console.info('Validating business rules...');
      return validateBusinessRules(db, event);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      console.info('Updating project');
      return createProjectDb(db, payload);
    })
    .then(res => {
      return formatResponse('put', res);
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

}


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

  console.info('pathparameters:' + JSON.stringify(event.pathParameters))
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
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      console.info('Querystring parameters OK. Connecting to DB...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Getting project');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Validate bander is not suspended
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return getDB(db, event.pathParameters.projectId);
    })
    .then(res => {
      if (res.length === 0) throw new BoilerPlate.NotFoundError('ProjectNotFound', { type: 'NOT_FOUND', message: `Project cannot be found with this id`, data: { path: `pathParameters.projectId` }, value: event.pathParameters.projectId, schema: null });
      return formatResponse('get', res);
    })
    // .then(res => { return formatResponse(res); }) 
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
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      console.info('Querystring parameters OK. Connecting to DB...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      console.info('Searching projects');
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // Validate bander is not suspended
      return BBHelpers.validateBanderStatus(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) throw new BoilerPlate.SuspensionError('User is suspended', error);
      return searchDB(db);
    })
    .then(res => {
      return formatResponse('search', res);
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