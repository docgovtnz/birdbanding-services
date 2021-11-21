'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
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

const RESOURCE_NAME = "project_bander_membership";

const validateProjectMembershipUpdateAccess = (customErrorFactory, db, projectId, claims, governingCognitoGroup) => {
  // ---------------------------------------------------------------------------- 
  console.info("validateProjectMembershipUpdateAccess()");

  var errors = [];

  // The Authorisation logic is fairly complex, and contained 
  // almost entirely in the database, therefore a complex stored
  // procedure does the work for us. Alternatively, we could make multiple
  // calls to the DB, get all the data we need, then perform the logic
  // here. That'd be more straightforward (and flexible), but also less
  // optimal.

  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
    return [];
  }

  return db.ro_bander_is_project_manager(claims.sub, projectId)
    .then(auth => {

      // TEMPORARY / TESTING
      console.log(JSON.stringify(auth[0]));

      // If we returned nothing, or an empty array, or the returned value is FALSE, we're not authorized.
      if (!auth || auth.length <= 0 || !auth[0].ro_bander_is_project_manager) {
        errors.push(customErrorFactory.getError('ProjectMembershipAuthorisationError', [projectId, 'pathParameters.projectId']));
      }

      // All good
      console.log(`Found ${errors.length} metadata validation error(s).`);
      return errors;
    });
};

const validateBusinessRules = async (db, event) => {
  // ---------------------------------------------------
  let promises = [];
  let errors = [];
  let payload = JSON.parse(event.body);

  // Wrapping this directly in the Promise.all was cleverly implemented, I've kept most of it but added case for 
  // members that are not part of the project
  // 1) Check project membership
  promises = payload.map(async project_member => {
    // ---------------------------------------------
    let member_errors = [];

    let isBanderRes = await db.ro_is_bander(project_member.bander_id);
    if (!isBanderRes[0].ro_is_bander) {
      member_errors.push(new BoilerPlate.NotFoundError('BanderNotFound', 
        { 
          type: 'NOT_FOUND', 
          message: `Bander cannot be found with this id`, 
          data: { 
            path: `[object Object]/bander_id` 
          }, 
          value: project_member.bander_id , schema: 'BUSINESS_VALIDATION' 
        }
      ));
    }

    let banderIsInProjectRes = await db.ro_bander_is_in_project(project_member.bander_id, event.pathParameters.projectId);

    // Handle errors for additions where user already in project
    if (project_member.action === 'add' && banderIsInProjectRes[0].ro_bander_is_in_project) {
      member_errors.push(new BoilerPlate.ParameterValidationError('ProjectMembershipAlreadyExists', 
      { 
        type: 'BUSINESS', 
        message: `Cannot add member: ${project_member.bander_id}, they are already a member of this project`, 
        data: {
          path: `[object Object]/bander_id`,
          value: project_member.bander_id,
          schema: 'BUSINESS_VALIDATION'
        }
      }
      ));
    }

    // Handle errors for removal where banders is not in project
    if (project_member.action === 'remove' && !banderIsInProjectRes[0].ro_bander_is_in_project) {
      member_errors.push(new BoilerPlate.ParameterValidationError('ProjectMembershipDoesNotExist', 
      { 
        type: 'BUSINESS', 
        message:`Cannot remove member: ${project_member.bander_id}, they are not a member of this project`, 
        data: {
          path: `[object Object]/bander_id`,
          value: project_member.bander_id,
          schema: 'BUSINESS_VALIDATION'
        }
      }
      ));
    }

    return (member_errors.length > 0) ? member_errors : null;
  });

  console.log('Length: ', promises.length);

  // 2) Check for duplicate project membership actions
  let banderArray = payload.map(project_member => project_member.bander_id);
  let duplicate = null;
  let isDuplication = banderArray.some((bander_id, idx) => {
    if (banderArray.indexOf(bander_id) !== idx) {
      duplicate = bander_id;
      return true;
    }
    return false;
  });

  // 3) Check projectId is valid
  promises.push(db.ro_is_project(event.pathParameters.projectId).then(resultSet => {
    if (!resultSet[0].ro_is_project) {
      return new BoilerPlate.ParameterValidationError('InvalidProjectId', 
      { 
        type: 'BUSINESS', 
        message:`Project ID: ${event.pathParameters.projectId} does not exist`, 
        data: {
          path: `pathParameters.project_id`,
          value: event.pathParameters.projectId,
          schema: 'BUSINESS_VALIDATION'
        }
      });
    }
    return null;
  }));

  if (isDuplication) {
    promises.push(Promise.resolve(new BoilerPlate.ParameterValidationError('DuplicateBander', 
    { 
      type: 'BUSINESS', 
      message:`Duplicate bander_id in project_bander_membership array`, 
      data: {
        path: `[object Object]/bander_id`,
        value: duplicate,
        schema: 'BUSINESS_VALIDATION'
      }
    })));
  }

  console.log(promises);

  return Promise.all([].concat.apply([], promises))
    .then(res => res.filter(error => error));
}


const generatePatchOperations = async (db, event) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".preProcessPatchAddOperations()");

  let payload = JSON.parse(event.body);
  let promises = await payload.filter(project_member => project_member.action === 'add')
    .map(async project_member => {
      // ---------------------------------------------
      return db.ro_bander_ever_in_project(project_member.bander_id, event.pathParameters.projectId)
        .then(resultSet => {
          // If previously part of this project, update existing record
          if (resultSet[0].ro_bander_ever_in_project) {
            // ---------------------------------
            return {
              op: 'update',
              value: {
                bander_id: project_member.bander_id, 
                project_id: event.pathParameters.projectId,
                is_deleted: false
              }
            }
          }
          // Otherwise insert this new record
          return {
            op: 'insert',
            value: {
              bander_id: project_member.bander_id, 
              project_id: event.pathParameters.projectId,
              is_deleted: false
            }
          }
        });
  });

  payload.filter(project_member => project_member.action === 'remove')
    .map(project_member => {
      // ---------------------------------------------
      promises.push({
        op: 'delete',
        value: {
          bander_id: project_member.bander_id, 
          project_id: event.pathParameters.projectId,
          is_deleted: true
        }
      });
      return;
    });

  return Promise.all(promises);
}

const addProjectMembership = (tx, operation) => {
  // ----------------------------------------------------------------------------    
  return tx.project_bander_membership.insert(operation);
}

const updateProjectMembership = (tx, operation) => {
  // ----------------------------------------------------------------------------    
  let criteria = {
    'project_id = ': operation.project_id,
    'bander_id = ': operation.bander_id
  };

  console.log(operation);
  console.log(criteria);

  return tx.project_bander_membership.update(criteria, operation);
}

const patchProjectMembership = async (db, patchOperations) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".patchProjectMembership()");

  let addOperations = [];
  let updateOperations = [];

  console.log(patchOperations);

  patchOperations.map(patchOp => {
    console.log(patchOp);
    switch(patchOp.op) {
      case 'insert': {
        addOperations.push(patchOp.value);
        break;
      }
      case 'update':
      case 'delete': {
        updateOperations.push(patchOp.value);
        break;
      }
    }
  });

  console.log(addOperations);
  console.log(updateOperations);

  return db.withTransaction(async tx => {
    // -------------------------------------------
    const add = await addProjectMembership(tx, addOperations);
  
    return Promise.all(updateOperations.map(updateValue => { return updateProjectMembership(tx, updateValue) }));
  });
}

const formatResponse = (method = 'search', res, event) => {
  // ----------------------------------------------------------------------------    

  let response = res;

  switch (method) {
    case 'get': {
      return (response.length > 0) ? response[0] : {};
    }
    case 'search': {
      return response;
    }
    case 'post': {
      return response;
    }
    case 'patch': {
      return db.project_bander_membership
        .join({
          pk: 'id',
          bander: {
            type: 'LEFT OUTER',
            pk: 'id',
            on: { 'id': 'project_bander_membership.bander_id' },
            decomposeTo: 'object'
          }
        })
        .find({ 'project_id = ': event.pathParameters.projectId, 'is_deleted': false })
    }
    case 'put': {
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

//PATCH /projects/{project-id}/banders    ### ADD MEMBER TO LIST (partial update)
module.exports.patch = (event, context, cb) => {
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
  let pathParameters = event.pathParameters;

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
  BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => { 
      // Store highest claimed group for reference further on in the function
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
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
      console.info('Payload parameters OK. Connecting to DB to validate business rules...');
      // DB connection (container reuse of existing connection if available)
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      return validateProjectMembershipUpdateAccess(customErrorFactory, db, event.pathParameters.projectId, claims, governingCognitoGroup);
    })
    // Handle errors / Read workbook
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ForbiddenError(`${errors.length} Authorisation validation error(s)!`, errors);
      }
      console.info('Validating business rules...');
      return validateBusinessRules(db, event);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      return generatePatchOperations(db, event);
    })
    .then(patchOperations => {
      console.info('Completing patch operations');
      console.log(patchOperations);
      return patchProjectMembership(db, patchOperations);
    })
    .then(res => {
      return formatResponse('patch', res, event);
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
