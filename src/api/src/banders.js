'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const DBAccess = require('aurora-postgresql-access.js');
const BBHelpers = require('bb-helpers');
const BoilerPlate = require('api-boilerplate.js');
const BBBusinessValidationAndErrors = require('bb-business-validation-and-errors');
const CustomErrorFactory = BBBusinessValidationAndErrors.CustomErrorFactory;
const formatSchemaErrors = require('event-validation-lib').formatSchemaErrors;
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;
let customErrorFactory;

const RESOURCE_NAME = "bander";

const ADMIN_GROUP_NAME = `${process.env.ENVIRONMENT}-${process.env.SERVICE_NAME}-admin-group`;
const MANAGER_GROUP_NAME = `${process.env.ENVIRONMENT}-${process.env.SERVICE_NAME}-manager-group`;
const USER_GROUP_NAME = `${process.env.ENVIRONMENT}-${process.env.SERVICE_NAME}-user-group`;
const READ_ONLY_GROUP_NAME = `${process.env.ENVIRONMENT}-${process.env.SERVICE_NAME}-read-only-group`;


const validateGetAccess = async(customErrorFactory, db, event, claims, governingCognitoGroup) => {
  // ---------------------------------------------------------------------------- 
  console.info("validateGetAccess()");

  // The Authorisation logic is fairly complex, we call a number of functions to capture each component
  //   of the process
  let banderId = event.pathParameters.banderId;

  if (governingCognitoGroup === BBHelpers.ADMIN_GROUP_NAME
      || claims.sub === banderId) {
    return null;
  }
  else {
   return customErrorFactory.getError('ForbiddenError', [`accessing bander: /people-view/${banderId}`, claims.sub, 'claims.sub']);
  }
};


const getDB = (db, pathParameters = null, queryStringParameters = null, multiValueQueryStringParameters = null) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".getDB()");

  // We've either got a definite ID to find, or we want all of them
  let id = (pathParameters && pathParameters.banderId) ? pathParameters.banderId : null;

  // Default pagination to the most recent records and submits empty criteria object
  let criteria = {};
  let idOperation = 'id =';
  criteria[idOperation] = id;

  console.log(criteria);

  // THIS RETURNS A PROMISE
  return db.bander
    .join({
      pk: 'id',
      bander_certifications: {
        type: 'LEFT OUTER',
        pk: 'id',
        on: { 'bander_id': 'bander.id' },
      },
      bander_projects: {
        type: 'LEFT OUTER',
        pk: ['id', 'project_id'],
        relation: 'vw_bander_projects_and_coordinators',
        on: { 'id': 'bander.id' },
      }
    })
    .find(criteria, { order: [{ field: 'row_creation_idx', direction: 'asc' }] });
}

const searchDB = (db, pathParameters = null, queryStringParameters = null, multiValueQueryStringParameters = null) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".searchDB()");

  // We've either got a definite ID to find, or we want all of them
  let id = (pathParameters && pathParameters.banderId) ? pathParameters.banderId : null;
  // Maximum pagination limit of 20 applicable given we are, somewhat inefficiently, making up to 20 cognitoListUsers calls
  let limit = (queryStringParameters && queryStringParameters.limit) ? parseInt(queryStringParameters.limit) : null;
  let paginationToken = (queryStringParameters && 'paginationToken' in queryStringParameters && queryStringParameters.paginationToken) ?
    parseInt(queryStringParameters.paginationToken) : 0;

  let count = null;

  // We've either got a definite ID to find, 
  // or we want a paginated subset (ordered from 
  // the most recent record to the oldest)
  // We base the pagination subset on a specific 
  // previous row_creation_idx which was a oldest record 
  // in the previous paginated set
  // TODO : Look at other query string parameters to add to the search
  let idValue = id ? id : paginationToken;

  // Default pagination to the most recent records and submits empty criteria object
  let criteria = {};
  let idOperation;
  if (id) {
    idOperation = 'id =';
    criteria[idOperation] = idValue;
  }
  else if (limit) {
    // Pagination on a join is slightly trickier (cannot use limit due to amalgamation of resultset)
    idOperation = 'row_creation_idx BETWEEN';
    criteria[idOperation] = [(idValue + 1), (idValue + limit)];
  }

  // THIS RETURNS A PROMISE
  return db.bander.count({})
    .then(res => {
      count = res;
      return db.bander
        .join({
          pk: 'id',
          bander_certifications: {
            type: 'LEFT OUTER',
            pk: 'id',
            on: { 'bander_id': 'bander.id' },
          }
        })
        .find(criteria, { order: [{ field: 'row_creation_idx', direction: 'asc' }] });
    })
    .then(res => {
      console.log(res.length);
      return {
        data: res,
        count: parseInt(count),
        countFromPageToEnd: true,
        isLastPage: (parseInt(count) <= res.length) || res.length < limit,
        prev: (limit) ? paginationToken - limit : null
      }
    })
}

const searchCognito = (dbSearch, pathParameters = null, queryStringParameters = null, resourceDefinition = null) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".searchCognito()");

  let id = (pathParameters && pathParameters.banderId) ? pathParameters.banderId : null;
  let cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  // if (queryStringParameters && 'cognitoPaginationToken' in queryStringParameters) {
  //   console.info('Pagination token found!');
  //   params.PaginationToken = queryStringParameters.cognitoPaginationToken
  // }

  // For each of the users returned from the database query,
  // generate a cognitoListUsers call for their cognito details
  let promises = [];

  // ---------------------------
  //  Complete Cognito Search
  // ---------------------------
  if (id) {
    // If a specific id has been provided, we are using searchCognito for a detail request
    let filterStatement = 'sub = \"' + id + '\"';
    let params = {
      UserPoolId: process.env.COGNITO_USER_POOL_ID,
      Filter: filterStatement,
    }
    console.log(`Searching by id ${id}`)
    promises.push(cognitoidsp.listUsers(params).promise());
  }
  else {
    dbSearch.data.forEach(dbUser => {
      console.log(dbUser);
      // Otherwise, we want to construct a filterStatement for a list request,
      // from the results of a dbSearch
      let filterStatement = 'sub = \"' + dbUser.id + '\"';
      let params = {
        UserPoolId: process.env.COGNITO_USER_POOL_ID,
        Filter: filterStatement,
      }
      // Call listUsers to get back a subset of what we hope is a single user!
      promises.push(cognitoidsp.listUsers(params).promise());
    });
    // ---------------------------
  }

  console.log(promises);

  console.log('Queuing up promises...')
  return Promise.all(promises)
    .then(res => {
      console.log(res);
      let cognitoSearch = [];
      if (res[0] && res[0].Users.length > 0) {
        // Reduce the rapid Cognito ListUsers SDK calls into a single array of users
        cognitoSearch = res.reduce((accumulator, cognitoResultset) => {
          // ------------------------------------------
          console.log(cognitoResultset);
          if (cognitoResultset && 'Users' in cognitoResultset && cognitoResultset.Users[0]) accumulator.push(cognitoResultset.Users[0]);
          return accumulator;
        }, []);
      }
      return {
        detailSearch: id ? true : false,
        dbSearch: dbSearch,
        cognitoSearch,
        // cognitoPaginationToken: res.PaginationToken ? res.PaginationToken : null
      };
    })
    .catch(err => {
      console.error(err);
      throw new BoilerPlate.DataAccessError('DataAcqusitionError', 'Unable to acquire Cognito data. Contact technical support.');
    })
}

const validatePropertyCognito = (customErrorFactory, eventBody, propName) => {
  // ----------------------------------------------------------------------------   
  console.info(RESOURCE_NAME + ".validateUsernameCognito()");

  if (typeof eventBody === 'undefined' || !eventBody || !(propName in eventBody) || !eventBody[propName] || typeof eventBody[propName] === 'undefined') {
    throw new BoilerPlate.ParameterValidationError(`Invalid${propName}ValidationRequest`, { message: `A valid ${propName} (string) has not been submitted in request body.`, property: propName, value: 'null/undefined' });
  }

  let capitalizedPropName = propName.charAt(0).toUpperCase() + propName.slice(1);

  let cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  // If username not in event body, return bad request

  // ---------------------------
  //  Complete Cognito Search
  // ---------------------------
  // If a specific id has been provided, we are using searchCognito for a detail request
  let filterStatement = `${propName} = "${eventBody[propName]}"`;
  let params = {
    UserPoolId: process.env.COGNITO_USER_POOL_ID,
    Filter: filterStatement
  }

  return cognitoidsp.listUsers(params).promise()
    .then(res => {
      if (res && res.Users.length > 0) {
        return customErrorFactory.getError('UserPropertyExistsError', [propName, eventBody[propName], `/${propName}`]);
      }
      return null;
    });
}

const mergeBanderData = (userResultSet, userStatuses = ['CONFIRMED', 'FORCE_CHANGE_PASSWORD', 'RESET_REQUIRED'], resourceDefinition = null) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".mergeBanderData()");
  return new Promise((resolve, reject) => {

    // Only return uses who have a status that is within the set provided to the method
    // -> TODO -> add parameter to method call and integrate with query string parameters
    console.log(JSON.stringify(userResultSet));
    let cognitoUserSubset = (userResultSet.cognitoSearch.length > 0) ? userResultSet.cognitoSearch.filter(user => userStatuses.includes(user.UserStatus)) : [];

    let banders = [];
    banders = cognitoUserSubset.map(cognitoUser => {
      // -----------------------------------------------
      // Get the Cognito Attributes for this user and format into a single object
      // These come in from Cognito in the form: [{ 'NAME': '<NAME>', 'VALUE': '<VALUE>'}
      let cognitoUserAttributes = cognitoUser.Attributes.reduce((objectBuilder, item) => (objectBuilder[item.Name] = item.Value, objectBuilder), {});
      // Find the corresponding user in the db records
      let dbUserMatch = userResultSet.dbSearch.data.find(dbUser => dbUser.id === cognitoUserAttributes.sub);
      if (dbUserMatch !== undefined) {
        delete dbUserMatch.row_creation_user_;
        delete dbUserMatch.row_update_user_;
      }
      else {
        dbUserMatch = {};
      }
      // let bander = Object.assign({}, { profile: cognitoUserAttributes }, { bander: dbUserMatch });
      // Flatten the cognito/massivejs objects into a single bander object (a simplification in the long run, when compared to maintaining two sub-objects!)
      let bander = {
        id: cognitoUserAttributes.sub,
        username: cognitoUser.Username,
        email: cognitoUserAttributes.email,
        email_verified: cognitoUserAttributes.email_verified,
        person_name: dbUserMatch.person_name,
        name: cognitoUserAttributes.name,
        given_name: cognitoUserAttributes.given_name,
        family_name: cognitoUserAttributes.family_name,
        nznbbs_certification_number: dbUserMatch.nznbbs_certification_number,
        phone_number: ('phone_number' in cognitoUserAttributes) ? cognitoUserAttributes.phone_number : null,
        address: ('address' in cognitoUserAttributes) ? cognitoUserAttributes.address : null,
        last_login: dbUserMatch.last_login,
        is_hidden: dbUserMatch.is_hidden,
        bander_state: dbUserMatch.bander_state,
        primary_organisation: dbUserMatch.primary_organisation,
        row_creation_timestamp_: dbUserMatch.row_creation_timestamp_,
        row_update_timestamp_: dbUserMatch.row_update_timestamp_,
        row_creation_idx: dbUserMatch.row_creation_idx,
        bander_certifications: dbUserMatch.bander_certifications,
        bander_projects: dbUserMatch.bander_projects
      }

      // Cleanup the DB profile to remove unnecessary attributes
      return bander;
    })

    // Resolve detail query by returning single bander
    if (userResultSet.detailSearch) {
      // If the detail search does not find any users, return an empty object to reflect this
      let res = (banders.length) > 0 ? [banders[0]] : [];
      resolve(res);
    }
    // Otherwise return a list of banders
    resolve({ banders: banders });
  });
}

const validateBusinessRules = async (customErrorFactory, db, event, method, referenceData = null, claims = null, governingCognitoGroup = null) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".validateBusinessRules()");

  let payload = JSON.parse(event.body);
  let banderId = (method === 'put') ? event.pathParameters.banderId : null;
  let promises = [];

  // If this is an update request, we need to confirm
  //  that the user is admin OR that the user is updating their own profile
  console.log('Governing cognito group: ', governingCognitoGroup);
  console.log('Adming group name: ', BBHelpers.ADMIN_GROUP_NAME);
  if (method === 'put' && governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME && claims.sub !== banderId) {
    throw new BoilerPlate.AuthorisationError('Unauthorised', "Not a member of any authorised groups!");
  }

  if (banderId) {
    let isBanderResultSet = await db.ro_is_bander(banderId)
    if (!isBanderResultSet[0].ro_is_bander) {
      let error = customErrorFactory.getError('NotFoundError', ['banderId', banderId, 'pathParameters.banderId']);
      throw new BoilerPlate.NotFoundError('BanderNotFound', [error]);
    }
  }
  
  // If the users email is the same as it is at the moment, no uniqueness validation 
  if ((method === 'post' && !(event.queryStringParameters && !('resendInvite' in event.queryStringParameters))) || (method === 'put' && payload.email !== referenceData.email)){
    // Only need to validate a user's email when they are attempting to actually change their own email
    console.log('Validating email uniqueness');
    promises.push(validatePropertyCognito(customErrorFactory, JSON.parse(event.body), 'email'));
  }

  // If the users email is the same as it is at the moment, no uniqueness validation 
  if (method === 'post' && !(event.queryStringParameters && !('resendInvite' in event.queryStringParameters))){
    // Only need to validate a user's email when they are attempting to actually change their own email
    console.log('Validating username uniqueness');
    promises.push(validatePropertyCognito(customErrorFactory, JSON.parse(event.body), 'username'));
  }

  // Check if a certification number has been submitted that is different and that it is unique
  if ((method === 'post' && !(event.queryStringParameters && ('resendInvite' in event.queryStringParameters))) || (method === 'put' && payload.nznbbs_certification_number)) {
    promises.push(db.ro_is_unique_nznbbs_certification_number(banderId, payload.nznbbs_certification_number)
      .then(res => {
        console.log(res);
        if(!res[0].ro_is_unique_nznbbs_certification_number) {
          return customErrorFactory.getError('DuplicationError', ['nznbbs_certification_number', payload.nznbbs_certification_number, '/nznbbs_certification_number']);
        }
      }));
  }

  return Promise.all(promises)
    .then(res => res.filter(error => error));
}

const getCognitoUserAttributes = (customErrorFactory, id = null, banderObj) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".getCognitoUserAttributes()");

  let cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  let params = {
    UserPoolId: process.env.COGNITO_USER_POOL_ID,
    Username: banderObj.username
  }

  // Call adminUpdateUserAttributes to complete the update to the user cognito profile
  return cognitoidsp.adminGetUser(params).promise()
    .then(res => {
      // Check if the sub matches the id submitted with this request
      // (this prevent invalid username/user-id combinations causing updates)
      if (res.UserAttributes.find(attribute => attribute.Name === 'sub').Value !== id) {
        throw new BoilerPlate.DataAccessError('CognitoIdMismatchError', 'Invalid bander id.');
      }
      // Reformat response to be compatible with Cognito rollback
      let rollbackBander = banderObj;

      res.UserAttributes.forEach(attribute => {
        if (!(['sub', 'email_verified'].indexOf(attribute.Name) > -1)) {
          rollbackBander[attribute.Name] = attribute.Value;
        }
      });
      // return with the rollback user template for use if required after the DB update
      return rollbackBander;
    })
    .catch(err => {
      if (err.message === 'User does not exist.') {
        let error = customErrorFactory.getError('NotFoundError', ['username', banderObj.username, '/username']);
        throw new BoilerPlate.NotFoundError('BanderNotFound', [error]);
      }
      if (err.message === 'CognitoIdMismatchError') {
        let error = customErrorFactory.getError('NotFoundError', ['username,banderId', `${banderObj.username},${id}`, '/username,pathParameters.banderId']);
        throw new BoilerPlate.NotFoundError('BanderNotFound', [error]);
      }
      console.error(JSON.stringify(err));
      throw new BoilerPlate.DataAccessError('CognitoUpdateError', 'Unable to update Cognito data.');
    });
}

const createBanderCognito = (banderObj) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".createBanderCognito()");

  let cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  let params = {
    UserPoolId: process.env.COGNITO_USER_POOL_ID,
    Username: banderObj.username,
    ForceAliasCreation: false,
    DesiredDeliveryMediums: ['EMAIL'],
    MessageAction: 'SUPPRESS', // Remove after migration is completed (i.e. we want to email users when we admin create them)
    UserAttributes: [
      { Name: 'name', Value: banderObj.name },
      { Name: 'given_name', Value: banderObj.given_name },
      { Name: 'family_name', Value: banderObj.family_name },
      { Name: 'email', Value: banderObj.email },
      { Name: 'email_verified', Value: 'true' }
    ]
  };

  if (['prod'].includes(process.env.ENVIRONMENT)) {
    delete params.MessageAction;
  }

  if ('phone_number' in banderObj && typeof banderObj.phone_number !== 'undefined' && banderObj.phone_number && banderObj.phone_number !== '') {
    params.UserAttributes.push({ Name: 'phone_number', Value: banderObj.phone_number });
  }

  if ('address' in banderObj && typeof banderObj.address !== 'undefined' && banderObj.address && banderObj.address !== '') {
    params.UserAttributes.push({ Name: 'address', Value: banderObj.address });
  }

  console.log(JSON.stringify(params));

  // Call adminUpdateUserAttributes to complete the update to the user cognito profile
  return cognitoidsp.adminCreateUser(params).promise()
    .then(res => {
      // --------------------------------------------
      // Successful Update -> resolve with the bander profile
      console.info('Cognito admin creation complete: ', res);
      banderObj.id = res.User.Attributes.find(attribute => attribute.Name === 'sub').Value;
      banderObj.username = res.User.Username;
      return banderObj;
    })
    .catch(err => {
      console.error(JSON.stringify(err));

      if (err && 'code' in err && 'message' in err && err.code === 'UsernameExistsException' && err.message.includes('email')) {
        throw new BoilerPlate.ParameterValidationError('EmailExistsError', { type: 'BUSINESS', message: 'Email already exists. Unable to update Cognito data.', data: { path: `[object Object]/email`}, value: banderObj.email, schema: 'COGNITO' });
      }
      else if (err && 'code' in err && err.code === 'UsernameExistsException') {
        throw new BoilerPlate.ParameterValidationError('UsernameExistsError', { type: 'BUSINESS', message: 'Username already exists. Unable to update Cognito data.', data: { path: `[object Object]/username`}, value: banderObj.username, schema: 'COGNITO' });
      }
      else if (err && 'code' in err && err.code === 'InvalidParameterException') {
        throw new BoilerPlate.ParameterValidationError('InvalidParameterError', { type: 'BUSINESS', message: err.message, data: { path: null }, value: JSON.stringify(banderObj), schema: 'COGNITO' });
      }
      throw new BoilerPlate.DataAccessError('CognitoCreateError', 'There was an unexpected error when creating cognito data, the bander does not exist in Cognito or the database');
    });
}

const assignCognitoUserPoolGroup = (banderObj, groupName) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".assignCognitoUserPoolGroup()");

  let cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  let params = {
    UserPoolId: process.env.COGNITO_USER_POOL_ID,
    Username: banderObj.username,
    GroupName: groupName
  };

  console.log(JSON.stringify(params));

  // Call adminUpdateUserAttributes to complete the update to the user cognito profile
  return cognitoidsp.adminAddUserToGroup(params).promise()
    .then(res => {
      // --------------------------------------------
      // Successful Update -> resolve with the bander profile
      console.info('Cognito user added to group successfully: ', res);
      return banderObj;
    })
    .catch(err => {
      console.error(JSON.stringify(err));
      throw new BoilerPlate.DataAccessError('CognitoCreateError', 'Unable to update Cognito Groups.');
    });
}

const resendBanderInvitationEmail = (banderObj) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".resendBanderInvitationEmail()");

  // This function is to allow bander invitation emails to be resent
  // The banders email_verified status should always be true at this point so no further updates are required
  // At the point where user's are able to sign themselves up this may need to be revisited

  let cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  let params = {
    UserPoolId: process.env.COGNITO_USER_POOL_ID,
    Username: banderObj.username,
    DesiredDeliveryMediums: ["EMAIL"],
    MessageAction: 'RESEND',
  }

  // Call adminUpdateUserAttributes to complete the update to the user cognito profile
  return cognitoidsp.adminCreateUser(params).promise()
    .then(res => {
      // --------------------------------------------
      // Successful Update -> resolve with the bander profile
      console.info('Cognito email resent: ', res);
      return banderObj;
    })
    .catch(err => {
      console.error(JSON.stringify(err));
      throw new BoilerPlate.DataAccessError('CognitoUpdateError', 'Unable to resend cognito invite.');
    });
}

const updateBanderCognito = (banderObj) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".updateBanderCognito()");

  let cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  let params = {
    UserPoolId: process.env.COGNITO_USER_POOL_ID,
    Username: banderObj.username,
    UserAttributes: [
      { Name: 'name', Value: banderObj.name },
      { Name: 'given_name', Value: banderObj.given_name },
      { Name: 'family_name', Value: banderObj.family_name },
      { Name: 'email', Value: banderObj.email },
      { Name: 'email_verified', Value: 'true' } // This is because we are admin creating users, if we allow sign-ups we should verify email addresses
    ]
  }

  if ('phone_number' in banderObj && typeof banderObj.phone_number !== 'undefined' && banderObj.phone_number && banderObj.phone_number.length > 0) {
    params.UserAttributes.push({ Name: 'phone_number', Value: banderObj.phone_number });
  }

  if ('address' in banderObj && typeof banderObj.address !== 'undefined' && banderObj.address && banderObj.address.length > 0) {
    params.UserAttributes.push({ Name: 'address', Value: banderObj.address });
  }

  // Call adminUpdateUserAttributes to complete the update to the user cognito profile
  return cognitoidsp.adminUpdateUserAttributes(params).promise()
    .then(res => {
      // --------------------------------------------
      // Successful Update -> resolve with the bander profile
      console.info('Cognito update complete: ', res);
      return banderObj;
    })
    .catch(err => {
      console.error(JSON.stringify(err));
      throw new BoilerPlate.DataAccessError('CognitoUpdateError', 'Unable to update Cognito data.');
    });
}

const deleteBanderCognito = (banderObj) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".deleteBanderCognito()");

  let cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  let params = {
    UserPoolId: process.env.COGNITO_USER_POOL_ID,
    Username: banderObj.username,
  }

  // Call adminUpdateUserAttributes to complete the update to the user cognito profile
  return cognitoidsp.adminDeleteUser(params).promise()
    .then(res => {
      // --------------------------------------------
      // Successful Update -> resolve with the rollback bander object
      return banderObj;
    })
    .catch(err => {
      console.error(JSON.stringify(err));
      throw new BoilerPlate.DataAccessError('CognitoDeleteError', 'Unable to delete Cognito data. If database changes have been made, these will be rolled-back.');
    });
}

const createBanderDb = (db, cognitoRollback, eventBody) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".createBanderDb()");
  return new Promise((resolve, reject) => {

    let params = {
      id: cognitoRollback.id,
      nznbbs_certification_number: eventBody.nznbbs_certification_number,
      is_hidden: eventBody.is_hidden,
      bander_state: eventBody.bander_state,
      primary_organisation: eventBody.primary_organisation,
      person_name: `${(typeof eventBody.name !== 'undefined' && eventBody.name) ? eventBody.name : 'missing-name'} ${(typeof eventBody.family_name !== 'undefined' && eventBody.family_name) ? eventBody.family_name : 'missing-surname'}`,
      username: cognitoRollback.username
    };

    db.bander.insert(params)
      .then(res => {
        if (res.length <= 0) {
          // ------------------------------------
          // If the bander hasn't been updated here, we need to catch this error and rollback the Cognito update
          return reject(new BoilerPlate.DataAccessError('DatabaseBanderCreateError', 'Unable to create bander in the database. Cognito updates rolling-back.'));
        }
        // Successfull DB update, cleanup the response object and return!
        console.info('DB update complete: ', res);
        delete res.row_creation_user_;
        delete res.row_update_user_;
        return resolve({
          ...cognitoRollback,
          ...res
        });
      })
      .catch(err => {
        console.log(err)
        return reject(new BoilerPlate.DataAccessError('DatabaseBanderCreateError', 'Unable to create bander in the database. Cognito updates rolling-back.'));
      })
  });
}

const deleteBanderDb = (db, id = null) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".deleteBanderDb()");
  return new Promise((resolve, reject) => {

    let response = null;
    let banderParams = { id: id };
    let certificationParams = { bander_id: id };

    db.bander_certifications.destroy(certificationParams)
      .then(res => {
        return db.bander.destroy(banderParams);
      })
      .then(res => {
        if (res.length <= 0) {
          // ------------------------------------
          // If the bander hasn't been updated here, we need to catch this error and rollback the Cognito update
          return reject(new BoilerPlate.DataAccessError('DatabaseBanderDeleteError', 'Unable to delete bander in the database. Cognito updates rolling-back.'));
        }
        // Successfull DB update, cleanup the response object and return!
        console.info('DB deletion complete: ', res);
        response = res[0];
        let recordActionparams = [
          {
            db_action: 'DELETE',
            db_table: 'bander_certifications',
            db_table_identifier_name: 'bander_id',
            db_table_identifier_value: id
          },
          {
            db_action: 'DELETE',
            db_table: 'bander',
            db_table_identifier_name: 'id',
            db_table_identifier_value: id
          }
        ]
        return db.record_action.insert(recordActionparams);
      })
      .then(res => resolve(response))
      .catch(err => {
        console.log(err)
        return reject(new BoilerPlate.DataAccessError('DatabaseBanderDeleteError', 'Unable to delete bander in the database. Cognito updates rolling-back.'));
      })
  });
}

const updateBanderDb = (db, id = null, banderObj) => {
  // ----------------------------------------------------------------------------    
  console.info(RESOURCE_NAME + ".updateBanderDb()");
  return new Promise((resolve, reject) => {

    // We need to check if we are updating a bander's database details or not
    // If the bander subobject is submitted we want to update the database
    let criteria = {
      'id =': id
    }
    let banderUpdate = {
      nznbbs_certification_number: banderObj.nznbbs_certification_number,
      is_hidden: banderObj.is_hidden,
      bander_state: banderObj.bander_state,
      primary_organisation: banderObj.primary_organisation,
      person_name: `${(typeof banderObj.name !== 'undefined' && banderObj.name) ? banderObj.name : 'missing-name'} ${(typeof banderObj.family_name !== 'undefined' && banderObj.family_name) ? banderObj.family_name : 'missing-surname'}`,
      username: banderObj.username
    }

    // THIS RETURNS A PROMISE
    db.bander.update(criteria, banderUpdate)
      .then(res => {
        if (res.length <= 0) {
          // ------------------------------------
          // If the bander hasn't been updated here, we need to catch this error and rollback the Cognito update
          return reject(new BoilerPlate.DataAccessError('DatabaseBanderUpdateError', 'Unable to update bander in the database. Cognito updates rolling-back.'));
        }
        // Successfull DB update, cleanup the response object and return!
        console.info('DB update complete: ', res);
        delete res[0].row_creation_user_;
        delete res[0].row_update_user_;

        return resolve({
          ...banderObj,
          ...res[0]
        });
      })
      .catch(err => {
        console.log(err)
        return reject(new BoilerPlate.DataAccessError('DatabaseBanderUpdateError', 'Unable to update bander in the database. Cognito updates rolling-back.'));
      })
  });
}

const addCalculatedFields = (dbResult) => {
  // ----------------------------------------------------------------------------   
  return new Promise((resolve, reject) => {

    console.info(RESOURCE_NAME + ".addCalculatedFields()");

    console.log(JSON.stringify(dbResult));

    // Add maximum certification level
    let res = dbResult.map(dbResultItem => {
      // -----------------------------------
      let certificationHierachy = ['UNCERTIFIED', 'L1', 'L2', 'L3'];
      let maximumCertificationLevel = 'UNCERTIFIED';

      maximumCertificationLevel = dbResultItem.bander_certifications.reduce((accumulator, current) => {
        if (certificationHierachy.indexOf(current.competency_level) > certificationHierachy.indexOf(accumulator)) {
          return current.competency_level;
        }
        else {
          return accumulator;
        }
      }, 'UNCERTIFIED');

      if (!('name' in dbResultItem) && !('name' in dbResultItem) && !('name' in dbResultItem)) {
        let nameSplit = dbResultItem.person_name ? dbResultItem.person_name.split(' ') : null;
        let processedRecord = Object.assign({}, {
          ...dbResultItem,
          name: nameSplit ? nameSplit.slice(0, (nameSplit.length - 1)).join(' ') : nameSplit,
          given_name: nameSplit ? nameSplit.slice(0, (nameSplit.length - 1)).join(' ') : null,
          family_name: nameSplit ? nameSplit.slice(nameSplit.length - 1).join(' ') : null,
          maximum_certification_level: maximumCertificationLevel
        });
        delete processedRecord.bander_certifications;
        return processedRecord;
      }
      else {
        let processedRecord = Object.assign({}, {
          ...dbResultItem,
          maximum_certification_level: maximumCertificationLevel
        });
        return processedRecord;
      }

    });

    console.log(res);

    resolve(res);
  });
}


function formatCertFields(certs){
  const filteredCertFields = ['species_group_id', 'row_creation_timestamp_', 'row_creation_user_', 'row_update_timestamp_', 
  'row_update_user_', 'endorsement'];
  for(let c in certs){
    let cert = certs[c];
    cert.certification = cert['species_group_id'] || cert['endorsement'];
    cert.certification_type = cert['species_group_id'] ? 'SPECIES_GROUP': 'ENDORSEMENT';
    filteredCertFields.forEach(f => delete cert[f]);
  }
return certs;
}

const formatResponse = (method = 'search', res, queryStringParameters) => {
  // ----------------------------------------------------------------------------    
  return new Promise((resolve, reject) => {

    switch (method) {
        case 'get': {
          let response = (res.length > 0) ? res[0] : {};
          if(response.bander_certifications){
            response.bander_certifications = formatCertFields(response.bander_certifications); 
          }
          resolve(response);
      }
      case 'post': {
        // Add nulls for phone number and email if not provided
        let formattedResponse = { ...res };
        formattedResponse.last_login = null;
        if (!('phone_number' in res)) {
          formattedResponse.phone_number = null;
        }
        if (!('address' in res)) {
          formattedResponse.address = null;
        }
        return resolve(formattedResponse)
      }
      case 'put': {
        // Add nulls for phone number and email if not provided
        let formattedResponse = { ...res };
        if (!('phone_number' in res)) {
          formattedResponse.phone_number = null;
        }
        if (!('address' in res)) {
          formattedResponse.address = null;
        }
        return resolve(formattedResponse)
      }
      case 'search':
      default: {
        let formattedResponse = [...res];

        if (typeof queryStringParameters !== 'undefined' && queryStringParameters && 'format' in queryStringParameters && queryStringParameters.format === 'summary') {
          // ---------------------------------------------
          formattedResponse = formattedResponse.map(resItem => ({ id: resItem.id, nznbbs_certification_number: resItem.nznbbs_certification_number, person_name: resItem.person_name, row_creation_idx: resItem.row_creation_idx }));
        }
        return resolve(formattedResponse);
      }
    }
  })
};

const validateCognitoPropAndReturn = (cb, customErrorFactory, event, propName) => {
  // ----------------------------------------------------
  // Attempting to validate username
  console.info('Validating username...');

  return validatePropertyCognito(customErrorFactory, JSON.parse(event.body), propName)
    .then(err => {
      // --------------------------------
      if (err) throw new BoilerPlate.ParameterValidationError(`${propName} is not unique`, err);

      console.info("Returning with no errors");
      cb(null, {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*", // Required for CORS support to work
          "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
        },
        "body": JSON.stringify({ details: { message: 'SUCCESS', property: propName, value: JSON.parse(event.body)[propName] } }),
        "isBase64Encoded": false
      });
    })
    .catch(err => {
      // --------------------------------
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

const getAdminBanderListExport = (cb, governingCognitoGroup) => {
  // ----------------------------------------------------
  console.info('getAdminBanderListExport');

  if (governingCognitoGroup !== BBHelpers.ADMIN_GROUP_NAME) {
    throw new BoilerPlate.ForbiddenError(`Authorisation validation error(s)!`, []);
  }

  var s3 = new AWS.S3({ signatureVersion:'v4' });

  let params = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: process.env.ADMIN_BANDER_LIST_DOWNLOAD_PATH,
    VersionId: null
  };

  console.log('Presigned url generation params: ', params);

  let url = s3.getSignedUrl('getObject', params);

  let response = {
    presigned_url: url
  }

  cb(null, {
    "statusCode": 200,
    "headers": {
      "Access-Control-Allow-Origin": "*", // Required for CORS support to work
      "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
    },
    "body": JSON.stringify(response),
    "isBase64Encoded": false
  });
}

// ============================================================================
// API METHODS
// ============================================================================

// POST
module.exports.post = (event, context, cb) => {
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

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;  

  // Rollback Object
  var cognitoRollback;

  // Response Object
  var response = {};

  // Validations -> check whether specific validations are being requested
  let validationCall = false;
  let validationProp = null;
  // 
  if (typeof event.queryStringParameters !== 'undefined' &&
  event.queryStringParameters &&
  'validateUsername' in event.queryStringParameters &&
  event.queryStringParameters.validateUsername &&
  event.queryStringParameters.validateUsername === 'true') {
    validationCall = true;
    validationProp = 'username';
  }

  if (typeof event.queryStringParameters !== 'undefined' &&
  event.queryStringParameters &&
  'validateEmail' in event.queryStringParameters &&
  event.queryStringParameters.validateEmail &&
  event.queryStringParameters.validateEmail === 'true') {
    validationCall = true;
    validationProp = 'email';
  }

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => { 
      // Store highest claimed group for reference further on in the function
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
      return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version); })
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
    // Handle errors / Validate payload
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
      console.info('Path parameters OK. Validating payload...');
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      if (validationCall) {
        return validateCognitoPropAndReturn(cb, customErrorFactory, event, validationProp);
      }
      return BoilerPlate.validateJSON(JSON.parse(event.body), payloadSchema);
    })
    // Handle errors / Validate business rules
    .then(errors => {
      if (errors.length > 0) {
        let formattedErrors = formatSchemaErrors(customErrorFactory, errors);
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body schema validation error(s)!`, formattedErrors);
      }
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
      console.info('Payload structure OK. Validating business rules...');
      if (event.queryStringParameters && 'resendInvite' in event.queryStringParameters && event.queryStringParameters.resendInvite === 'true') {
        return [];
      }
      return validateBusinessRules(customErrorFactory, db, event, "post", null, claims, governingCognitoGroup);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      console.info('Payload structure OK. Admin creating Cognito User...');
      // If we are invoking this endpoint to resend an invitation
      if (event.queryStringParameters && 'resendInvite' in event.queryStringParameters && event.queryStringParameters.resendInvite === 'true') {
        return resendBanderInvitationEmail(JSON.parse(event.body));
      }
      // AdminCreateUser in Cognito User Pool
      return createBanderCognito(JSON.parse(event.body));
    })
    .then((res) => {
      console.info('User created in Cognito, now creating in bander database table');
      cognitoRollback = res;
      response = res;
      if (event.queryStringParameters && 'resendInvite' in event.queryStringParameters && event.queryStringParameters.resendInvite === 'true') {
        return { ...JSON.parse(event.body), 'resentInvitation': 'true' };
      }
      // Update database component of user also!
      return createBanderDb(db, cognitoRollback, JSON.parse(event.body));
    })
    .then(res => {
      // Assign default user group to a new user
      console.log(res);
      if (event.queryStringParameters && 'resendInvite' in event.queryStringParameters && event.queryStringParameters.resendInvite === 'true') {
        return res;
      }
      return assignCognitoUserPoolGroup(res, USER_GROUP_NAME);
    })
    .then(res => {
      console.info('User created in database - returning updated bander');
      return formatResponse('post', res, null);
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
      // In the case of an unexpected database create error (after a successful Cognito create)
      // -> We want to check for a DatabaseBanderCreateError and trigger a rollback for Cognito
      if (err.message === 'DatabaseBanderCreateError') {
        // ----------------------------------------------
        deleteBanderCognito(cognitoRollback)
          .then(res => {
            cb(null, {
              "statusCode": err.statusCode || 500,
              "headers": {
                "Access-Control-Allow-Origin": "*", // Required for CORS support to work
                "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
              },
              "body": JSON.stringify(err),
              "isBase64Encoded": false
            });
          })
      }
      else {
        // ----------------------------------------------
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
      }
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

  console.debug(JSON.stringify(event));

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

  // Cognito Search Object
  var cognitoSearch;

  // Rollback Object
  var dbRollback;

  // Response Object
  var response = {};

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
    // Handle errors / Admin delete user...
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
      console.info('Querying bander username from Cognito...');
      return searchCognito({}, event.pathParameters)
    })
    .then(res => {
      console.log(res);
      cognitoSearch = res.cognitoSearch[0];
      console.info('Query parameters OK. Deleting user in bander database...');
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
      // DELETE USER DB
      return deleteBanderDb(db, event.pathParameters.banderId);
    })
    .then(res => {
      console.info('User deleted in bander database, now deleting user from Cognito');
      dbRollback = res;
      // Add the username key from the Cognito search!
      // This is required so that we can delete from Cognito...
      dbRollback.username = cognitoSearch.Username;
      // DELETE USER COGNITO
      return deleteBanderCognito(dbRollback);
    })
    .then(res => {
      console.info('User deleted in Cognito - returning HTTP 204');
      return {};
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
      // In the case of an unexpected database create error (after a successful Cognito create)
      // -> We want to check for a DatabaseBanderCreateError and trigger a rollback for Cognito
      if (err.message === 'CognitoDeleteError') {
        // ----------------------------------------------
        console.log('Test: ', dbRollback);
        // Rollback DB deletion
        return DBAccess.getDBConnection(db, dbCreationTimestamp)
          .then(dbAccess => {
            db = dbAccess.db;
            dbCreationTimestamp = dbAccess.dbCreationTimestamp;
            createBanderDb(db, dbRollback, dbRollback)
              .then(res => {
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
          });
      }
      else {
        // ----------------------------------------------
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
      }
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

  console.debug(JSON.stringify(event));

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

  // Invocation claims
  var claims = event.requestContext.authorizer.claims;
  let governingCognitoGroup = null;  

  // Rollback Object
  var cognitoRollback;

  // Response Object
  var response = {};

  // Do the actual work
  return BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST)
    .then(res => { 
      governingCognitoGroup = BBHelpers.getGoverningCognitoGroup(res);
      return BoilerPlate.getSchemaFromDynamo(parameterSchemaParams.table, parameterSchemaParams.id, parameterSchemaParams.version); })
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
    // Handle errors / Validate payload
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
      console.info('Path parameters OK. Validating payload...');
      return BoilerPlate.getSchemaFromDynamo(payloadSchemaParams.table, payloadSchemaParams.id, payloadSchemaParams.version);
    })
    .then(schema => {
      payloadSchema = schema;
      return BoilerPlate.validateJSON(JSON.parse(event.body), payloadSchema);
    })
    // Handle errors / Validate business rules
    .then(errors => {
      if (errors.length > 0) {
        let formattedErrors = formatSchemaErrors(customErrorFactory, errors);
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body schema validation error(s)!`, formattedErrors);
      
      }
      return DBAccess.getDBConnection(db, dbCreationTimestamp);
    })
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // No need for suspension check because banders should always be able to see and edit their details
      console.info('Payload structure OK. Getting Cognito user in case of rollback...');
      return getCognitoUserAttributes(customErrorFactory, event.pathParameters.banderId, JSON.parse(event.body));
    })
    .then(res => {
      cognitoRollback = res;
      console.log('Initial rollback: ', cognitoRollback);
      console.info('Payload structure OK. Validating business rules...');
      return validateBusinessRules(customErrorFactory, db, event, "put", cognitoRollback, claims, governingCognitoGroup);
    })
    .then(errors => {
      if (errors.length > 0) {
        throw new BoilerPlate.ParameterValidationError(`${errors.length} Payload Body business validation error(s)!`, errors);
      }
      return updateBanderCognito(JSON.parse(event.body));
    })
    .then((res) => {
      console.info('Updated Cognito, now updating database');
      response.profile = res;
      return updateBanderDb(db, event.pathParameters.banderId, JSON.parse(event.body));
    })
    .then(res => {
      console.info('Updated database - returning updated bander');
      return formatResponse('put', res, null);
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
      // In the case of an unexpected database update error (after a successful Cognito update)
      // -> We want to check for a DatabaseBanderUpdateError and trigger a rollback for Cognito
      if (err.message === 'DatabaseBanderUpdateError') {
        // ----------------------------------------------
        updateBanderCognito(cognitoRollback)
          .then(res => {
            cb(null, {
              "statusCode": err.statusCode || 500,
              "headers": {
                "Access-Control-Allow-Origin": "*", // Required for CORS support to work
                "Access-Control-Allow-Credentials": true // Required for cookies, authorization headers with HTTPS
              },
              "body": JSON.stringify(err),
              "isBase64Encoded": false
            });
          })
      }
      else {
        // ----------------------------------------------
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
      }
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

  console.debug(JSON.stringify(event));

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

  // Hoisted variables
  let dbRes = null;

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
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;
      // No need for suspension check because banders should always be able to see and edit their details
      return getDB(db, event.pathParameters);
    })
    .then((res) => {
      if (res.length === 0) throw new BoilerPlate.NotFoundError('BanderNotFound', customErrorFactory.getError('NotFoundError', ['banderId', event.pathParameters.banderId, 'pathParameters.banderId']));
      dbRes = res;
      return validateGetAccess(customErrorFactory, db, event, claims, governingCognitoGroup);
    })
    .then(error => {
      console.log(error);
      if (error) {
        throw new BoilerPlate.ForbiddenError(`Authorisation validation error(s)!!`, error);
      }
      return searchCognito({ data: dbRes }, event.pathParameters); 
    })
    .then(res => { return mergeBanderData(res); })
    .then(res => { return addCalculatedFields(res); })
    .then(res => { return formatResponse('get', res); }) 
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

  console.debug(JSON.stringify(event));

  let count = null;
  let countType = 'TOTAL';
  let countFromPageToEnd = true;
  let isLastPage = false;
  let prevPaginationToken = null;

  // There's more to this than the usual search.
  // 1 - search the DB for all banders
  // 2 - List all Cognito users via the Cognito SDK: https://docs.aws.amazon.com/cognito/latest/developerguide/how-to-manage-user-accounts.html#cognito-user-pools-searching-for-users-listusers-api-examples
  // 3 - Filter the cognito list so that only the appropriate users are present (cognito:sub attribute == banders.id)
  // 4 - Concatenate the cognito properties into the object returned from the DB for each bander
  // 5 - Return the list of banders

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

      if (typeof event.queryStringParameters !== 'undefined' && event.queryStringParameters && 'format' in event.queryStringParameters && event.queryStringParameters.format === 'file') {
        return getAdminBanderListExport(cb, governingCognitoGroup)
      }
      // -------------------------
      return BoilerPlate.getSchemaFromDynamo(customErrorsSchemaParams.table, customErrorsSchemaParams.id, customErrorsSchemaParams.version);
    })
    // Handle errors / Validate business rules
    .then(customErrors => {
      console.log('Custom errors: ', customErrors.definitions);
      customErrorFactory = new CustomErrorFactory(customErrors.definitions);
      console.info('Querystring parameters OK. Connecting to DB...');
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
      return searchDB(db, event.pathParameters, event.queryStringParameters, event.multiValueQueryStringParameters);
    })
    .then(res => {
      count = res.count;
      countFromPageToEnd = res.countFromPageToEnd;
      isLastPage = res.isLastPage;
      prevPaginationToken = res.prev;
      return addCalculatedFields(res.data);
    })
    .then(res => {
      return formatResponse('search', res, event.queryStringParameters);
    })
    .then(res => {
      // ------------
      let params = {
        data: res, path: event.path,
        queryStringParameters: event.queryStringParameters,
        multiValueQueryStringParameters: event.multiValueQueryStringParameters,
        paginationPointerArray: ['row_creation_idx'],
        maxLimit: null, order: 'asc',
        count: count, countType: countType,
        countFromPageToEnd: countFromPageToEnd,
        isLastPage: isLastPage, prevPaginationToken: prevPaginationToken
      }
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


const banderStatusCheck = async(db, banderId) => {
  // -------------------------------------------------
  console.info('Post authentication -> banderStatusCheck');

  console.log(banderId);

  let isBanderResultset = await db.ro_is_bander(banderId);

  let isActiveBanderResultset = await db.ro_is_active_bander(banderId);

  let isSuspendedBanderResultset = await db.ro_is_suspended_bander(banderId);

  console.log(isBanderResultset);
  console.log(isActiveBanderResultset);
  console.log(isSuspendedBanderResultset);

  if (!isBanderResultset[0].ro_is_bander) {
    // --------------------------------------
    // Not bander in our datastore but has valid cognito login, return accordingly
    return 'NOT_IN_DB';
  }
  else if (isSuspendedBanderResultset[0].ro_is_suspended_bander) {
    return 'SUSPENDED';
  }
  else if (isBanderResultset[0].ro_is_bander && !isActiveBanderResultset[0].ro_is_active_bander) {
    // --------------------------------------
    // Not bander in our datastore but has valid cognito login, return accordingly
    return 'INACTIVE';
  }
  else {
    return '';
  }
}


// PostAuthentication Lambda Trigger
// Referring to: https://forums.aws.amazon.com/thread.jspa?threadID=252521
// Given initially we will be adminCreating users, the postauthentication
module.exports.postAuthentication = (event, context, cb) => {
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

  // We want to quickly reduce the duration of postAuthentication 
  // actions - to do so we filter out all non 'FORCE_CHANGE_PASSWORD' statuses 
  if (event.request.userAttributes['cognito:user_status'] === 'FORCE_CHANGE_PASSWORD') {
    // --------------------------------------------------------------------------
    // Handle Post Authentication Actions
    console.info('banders.postAuthentication[a.k.a. postConfirmation] Lambda triggered');

    // Setup DB upper scoped variable for multiple usages
    const bander = {
      banderId: event.request.userAttributes.sub,
      username: event.userName,
      person_name: `${event.request.userAttributes.given_name} ${event.request.userAttributes.family_name}`,
      is_hidden: false,
      bander_state: 'ACTIVE',
      primary_organisation: null
    }

    // Check whether the user exists in the Banders table yet or not,
    return DBAccess.getDBConnection(db, dbCreationTimestamp)
      .then(dbAccess => {
        db = dbAccess.db;
        dbCreationTimestamp = dbAccess.dbCreationTimestamp;
        return banderStatusCheck(db, bander.banderId);
      })
      .then(banderStatus => {
        console.log(banderStatus);

        switch(banderStatus) {
          case '':
            return null;
          case 'NOT_IN_DB':
            // If the bander is not currently present in the database, use the auth details to populate bander table
            console.info('[INFO] adding bander to relational datastore');
            console.log(JSON.stringify(bander));
            return db.rw_add_bander(bander.banderId, bander.username, bander.person_name, bander.is_hidden, bander.bander_state, bander.primary_organisation);
          case 'SUSPENDED':
            throw new BoilerPlate.AuthorisationError("Your account has been suspended", "Please contact the banding office to have your account status reviewed");
          case 'INACTIVE':
            // If the bander is not currently ACTIVE in the database, update the status to ACTIVE after login
            return db.rw_activate_bander(bander.banderId);
          default:
            // If we returned nothing, or an empty array, or the returned value is FALSE, we're not authorized.
            throw new BoilerPlate.AuthorisationError("Bander post sign-up error!", "Contact the banding office to request technical support!");
        }
      })
      .then(res => {
        console.log(res);
        // If the passthrough is 0 (existing bander)
        // or if the passthrough function result is 1 (new bander added), 
        // -> return successful response
        if ( !res 
              || (typeof res !== 'undefined'
                  && res
                  && res.length > 0 
                  && (( 'rw_add_bander' in res[0] && parseInt(res[0]['rw_add_bander']) === 1 )
                        || ( 'rw_activate_bander' in res[0] && parseInt(res[0]['rw_activate_bander']) === 1 )))) {
          return cb(null, event);
        }
        else {
          // - This error only gets called when a bander is:
          //    -> not found by ID in the database
          //    -> but still exists by username
          //   This is a violation of the uniqueness constrain of a username
          //    -> therefore, an authorisation error is the right result
          throw new BoilerPlate.AuthorisationError("Post Sign-up Error!", "Contact the banding office to request technical support!");
        }
      })
      .catch(err => {
          console.log(JSON.stringify(err));
          return cb(err.message);
      })
  }
  else {
    // --------------------------------------------------------------------------
    return DBAccess.getDBConnection(db, dbCreationTimestamp)
      .then(dbAccess => {
        db = dbAccess.db;
        dbCreationTimestamp = dbAccess.dbCreationTimestamp;
        return banderStatusCheck(db, event.request.userAttributes.sub);
      })
      .then(banderStatus => {
        console.log(banderStatus);

        // Setup DB upper scoped variable for multiple usages
        const bander = {
          banderId: event.request.userAttributes.sub,
          username: event.userName,
          person_name: `${event.request.userAttributes.given_name} ${event.request.userAttributes.family_name}`,
          is_hidden: false,
          bander_state: 'ACTIVE',
          primary_organisation: null
        }

        switch(banderStatus) {
          case '':
            return null;
          case 'NOT_IN_DB':
            // If the bander is not currently present in the database, use the auth details to populate bander table
            console.info('[INFO] adding bander to relational datastore');
            console.log(JSON.stringify(bander));
            return db.rw_add_bander(bander.banderId, bander.username, bander.person_name, bander.is_hidden, bander.bander_state, bander.primary_organisation);
          case 'SUSPENDED':
            // throw new BoilerPlate.AuthorisationError("Your account has been suspended", "Please contact the banding office to have your account status reviewed");
            // Planning to handle suspended users at the API call level from the UI so log the user in as for a typical user for now
            return null;
          case 'INACTIVE':
            // If the bander is not currently ACTIVE in the database, update the status to ACTIVE after login
            return db.rw_activate_bander(bander.banderId);
          default:
            // If we returned nothing, or an empty array, or the returned value is FALSE, we're not authorized.
            throw new BoilerPlate.AuthorisationError("Bander post sign-up error!", "Contact the banding office to request technical support!");
        }
      })
      .then(res => {
        console.log(res);
        return db.rw_update_last_login(event.request.userAttributes.sub);
      })
      .then(res => { return cb(null, event) })
      .catch(err => {
        console.error(err);
        return cb('Error completing sign-in - please contact support');
      })
  }
};
