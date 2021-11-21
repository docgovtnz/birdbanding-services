'use strict';

// Modules
const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
const BBHelpers = require('bb-helpers');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
const Helpers = require('helpers.js')
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "bander-projects";
const ALLOWABLE_FILTERS = {
  'projectState': {
    'filter_name': 'project_state ='
  }
};
const ALLOWABLE_SEARCHES = {};

// SERVICE FUNCTIONS

const validateBanderExists = async (customErrorFactory, db, event, claims) => {
  // -----------------------------------------
  console.info(`${RESOURCE_NAME}.validateBanderExists()`);

  let banderId = event.pathParameters.banderId;

  let banderExistsResultset = await db.ro_is_bander(banderId);

  console.log(banderExistsResultset);
  if (!banderExistsResultset[0].ro_is_bander) {
    return customErrorFactory.getError('NotFoundError', ['banderId', banderId, 'pathParameters.banderId']);
  } 
  else {
    console.log('No errors');
    return null;
  }  
}

const validateProjectMembershipAccess = async (customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------
  console.info(`${RESOURCE_NAME}.validateProjectMembershipAccess()`);

  // The Authorisation logic is fairly complex, we call a number of functions to capture each component
  //   of the process
  let banderId = event.pathParameters.banderId;

  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
    return null;
  }

  if (banderId !== claims.sub) {
    return customErrorFactory.getError('ForbiddenError', [`accessing projects for bander: ${banderId}`, claims.sub, 'claims.sub']);
  } 
  else {
    console.log('No errors');
    return null;
  }
};


const generateQueryStringFilterCriteria = (multiValueQueryStringParameters) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".generateQueryStringFilterCriteria()");

  let criteria = { 
    and: []
  };

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
          innerCriteria.or.push({ [ALLOWABLE_FILTERS[filterQueryStringParameterName].filter_name]: filterQueryStringParameterValue});
          return;
        });

        if (innerCriteria.or.length > 0) criteria.and.push(innerCriteria);
      }
    });
  }

  return criteria;
}


const searchDB = async (db, event, claims, governingCognitoGroup) => {
  // -----------------------------------------
  console.info(`${RESOURCE_NAME}.searchDB()`);

  let banderId = event.pathParameters.banderId;
  let userProjectList = await BBHelpers.getUserProjects(db, claims);

  let criteria = {
    or: userProjectList.map(projectId => { return { 'id =': projectId } })
  };

  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME) {
    criteria.or.push({
      'id IS NOT': null
    });
  }

  let filterCriteria = null;
  if (typeof event.multiValueQueryStringParameters !== 'undefined' 
        && event.multiValueQueryStringParameters 
        && 'projectState' in event.multiValueQueryStringParameters) {
    filterCriteria = generateQueryStringFilterCriteria(event.multiValueQueryStringParameters);
    criteria.and = filterCriteria.and;
  }

  console.log(JSON.stringify(criteria));

  if (criteria.or.length === 0) {
    return [];
  }

  console.log(JSON.stringify(criteria));

  return db.vw_project_list.find(criteria, {
    order: [{
      field: 'name',
      direction: 'asc'
    }]
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


// SEARCH
module.exports.search = (event, context, cb) => {
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
      return validateBanderExists(customErrorFactory, db, event, claims);
    })
    .then(error => {
      if (error) throw new BoilerPlate.NotFoundError('Bander not found', error);
      return validateProjectMembershipAccess(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      if (error) {
        throw new BoilerPlate.ForbiddenError(`Authorisation validation error(s)!!`, error);
      }
      return searchDB(db, event, claims, governingCognitoGroup); 
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
