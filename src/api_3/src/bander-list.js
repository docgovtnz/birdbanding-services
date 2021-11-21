'use strict';

const Promise = require('bluebird');
const Util = require('util');
const Moment = require('moment');
const MomentTimezone = require('moment-timezone');
const Stringify = require('csv-stringify');
const DBAccess = require('aurora-postgresql-access.js');
const BoilerPlate = require('api-boilerplate.js');
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));

// +++
let db;
let containerCreationTimestamp;
let dbCreationTimestamp;

const promiseTimeout = (timeout, idx) => {
  return new Promise((resolve, reject) => {
    setTimeout(() => { console.log(`Delay ${timeout} completed. Resolving delay batch: ${idx}`); resolve(); }, timeout);
  });
};

const getUsersFromDB = (db) => {
  // ---------------------------------
  return db.bander.find();
}

const getCertificationsFromDB = (db) => {
  // ---------------------------------
  return db.vw_bander_certifications.find();
}

const listUsersCognito = (dbUsers) => {
  // ---------------------------------
  console.log('.listUsersCognito');

  var cognitoidsp = new AWS.CognitoIdentityServiceProvider();

  let promises = dbUsers.map((dbUser, idx) => {
    // Otherwise, we want to construct a filterStatement for a list request,
    // from the results of a dbSearch
    let filterStatement = 'sub = \"' + dbUser.id + '\"';
    let params = {
      UserPoolId: process.env.COGNITO_USER_POOL_ID,
      Filter: filterStatement,
    }
    // Call listUsers to get back a subset of what we hope is a single user!
    return promiseTimeout(Math.floor(idx / 10)* 2000, Math.floor(idx / 10))
        .then(res => {
          console.log(`Calling listUsers index ${idx} and batch ${Math.floor(idx / 10)}`);
          return cognitoidsp.listUsers(params).promise();
        });

  });

return Promise.all(promises)
      .then(res => {
        return res
                .filter(userResultset => (userResultset !== 'undefined' 
                        && userResultset && 'Users' in userResultset
                        && userResultset.Users.length > 0)).map(userResultset => userResultset.Users[0])
      });
}

const mergeDbWithCognito = (dbBanders, dbCertifications, cognitoBanders) => {
  // -----------------------------------------------
  console.log('.mergeDbWithCognito');

  let banderMerge = [];
  
  dbBanders.map(dbBander => {
    // ----------------------------
    let cognitoMatch = cognitoBanders.find(cognitoBander => (cognitoBander.Username === dbBander.username));

    let given_name = null;
    let family_name = null;
    let email = null;

    if (typeof cognitoMatch !== 'undefined') {
      given_name = cognitoMatch.Attributes.find(attribute => attribute.Name === 'given_name');
      family_name = cognitoMatch.Attributes.find(attribute => attribute.Name === 'family_name');
      email = cognitoMatch.Attributes.find(attribute => attribute.Name === 'email');

      given_name = (typeof given_name !== 'undefined') ? given_name['Value'] : null;
      family_name = (typeof family_name !== 'undefined') ? family_name['Value'] : null;
      email = (typeof email !== 'undefined') ? email['Value'] : null;
    }

    // For users with certifications add multiple rows for each certification
    // -> if not certifications, still add a single row for the user
    let hasCertifications = dbCertifications.filter(certification => certification.bander_id === dbBander.id).length > 0;
    if (hasCertifications) {
      // -----------------------------------------------------
      dbCertifications.filter(certification => {
        return certification.bander_id === dbBander.id;
      })
      .map(banderCertification => {
        banderMerge.push({
          id: dbBander.id,
          username: dbBander.username,
          person_name: dbBander.person_name,
          given_name: given_name,
          family_name: family_name,
          email: email,
          nznbbs_certification_number: dbBander.nznbbs_certification_number,
          bander_state: dbBander.bander_state,
          primary_organisation: dbBander.primary_organisation,
          last_login: (dbBander.last_login) ? Moment(dbBander.last_login).format('DD/MM/YYYY HH:mm:ss') : null,
          user_creation_timestamp: Moment(dbBander.row_creation_timestamp_).format('DD/MM/YYYY HH:mm:ss'),
          certification_type: (banderCertification.name) ? banderCertification.name : banderCertification.endorsement,
          competency_level: banderCertification.competency_level, 
          certification_comment: banderCertification.certification_comment,
          valid_to_timestamp: (banderCertification.valid_to_timestamp) ? MomentTimezone(banderCertification.valid_to_timestamp).tz('NZ').format('DD/MM/YYYY hh:mm:ss A') : '',
          valid_from_timestamp: (banderCertification.valid_from_timestamp) ? MomentTimezone(banderCertification.valid_from_timestamp).tz('NZ').format('DD/MM/YYYY hh:mm:ss A') : ''
        })
      });
    }
    else {
      banderMerge.push({
        id: dbBander.id,
        username: dbBander.username,
        person_name: dbBander.person_name,
        given_name: given_name,
        family_name: family_name,
        email: email,
        nznbbs_certification_number: dbBander.nznbbs_certification_number,
        bander_state: dbBander.bander_state,
        primary_organisation: dbBander.primary_organisation,
        last_login: (dbBander.last_login) ? Moment(dbBander.last_login).format('DD/MM/YYYY HH:mm:ss') : null,
        user_creation_timestamp: Moment(dbBander.row_creation_timestamp_).format('DD/MM/YYYY HH:mm:ss'),
        certification_type: '',
        competency_level: '', 
        certification_comment: '',
        valid_to_timestamp: '',
        valid_from_timestamp: ''
      })
    }
  });

  return banderMerge;
}

const generateBanderCsvFile = (banderList) => {
  // -----------------------------------------------
  console.log('.generateCsvFile');
  
  let rows = [
    [
      'id', 
      'username', 
      'person_name', 
      'given_name', 
      'family_name', 
      'email', 
      'nznbbs_certification_number', 
      'bander_state',
      'primary_organisation',
      'last_login',
      'user_creation_timestamp',
      'certification_type',
      'competency_level',
      'certification_comment',
      'valid_to_timestamp',
      'valid_from_timestamp'
    ]
  ];

  banderList.map(bander => {
    rows.push([
      bander.id,
      bander.username,
      bander.person_name,
      bander.given_name,
      bander.family_name,
      bander.email,
      bander.nznbbs_certification_number,
      bander.bander_state,
      bander.primary_organisation,
      bander.last_login,
      bander.user_creation_timestamp,
      bander.certification_type,
      bander.competency_level,
      bander.certification_comment,
      bander.valid_to_timestamp,
      bander.valid_from_timestamp
    ]);
  })

  return new Promise((resolve, reject) => {
    Stringify(rows, function(err, output) {
      if (err) {
        reject(err);
      }
      resolve(output);
      });
    })
    .then(csv_output => {
      return csv_output;
    });
}


const writeCsvToS3 = (csv) => {
  // -----------------------------------------------
  console.log('.writeCsvToS3');

  var param = {
    Bucket: process.env.USER_ASSETS_BUCKET,
    Key: `admin-bander-list/bander_details_export.csv`,
    Body: csv
  };
  
  var s3 = new AWS.S3({ signatureVersion: 'v4' });
  return s3.putObject(param).promise();
}


// UPDATE BANDER LIST
module.exports.put = (event, context, cb) => {
  // ----------------------------------------------------------------------------
  context.callbackWaitsForEmptyEventLoop = false;

  if (typeof containerCreationTimestamp === 'undefined') {
    containerCreationTimestamp = Moment();
  }
  console.info(Moment().diff(containerCreationTimestamp, 'seconds') + ' seconds since container established...')

  console.log(JSON.stringify(event));

  if (event.env !== process.env.ENVIRONMENT) {
    // Passthrough
    cb(null, { 'status': 'success', 'body': 'passthrough' });
  }

  let dbBanders = [];
  let dbCertifications = [];
  let cognitoBanders = [];

  return DBAccess.getDBConnection(db, dbCreationTimestamp)
    .then(dbAccess => {
      db = dbAccess.db;
      dbCreationTimestamp = dbAccess.dbCreationTimestamp;

      return getUsersFromDB(db);
    })
    .then(res => {
      dbBanders = res;
      return getCertificationsFromDB(db);
    })
    .then(res => {
      // -----------------------------
      dbCertifications = res;
      return listUsersCognito(dbBanders);
    })
    .then(res => {
      // -----------------------------
      cognitoBanders = res;
      return mergeDbWithCognito(dbBanders, dbCertifications, cognitoBanders);
    })
    .then(res =>{
      // -----------------------------
      return generateBanderCsvFile(res);
    })
    .then(res => {
      // -----------------------------
      return writeCsvToS3(res);
    })
    .then(res => {
      // -----------------------------
      cb(null, { 'status': 'success', 'body': res });
    })
    .catch(err => {
      // -----------------------------
      console.error(err);
      cb({ 'status': 'success', 'body': err });
    })
};