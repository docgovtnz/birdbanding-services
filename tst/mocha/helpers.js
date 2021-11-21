'use strict'; 
 
// ============= 
// MODULES 
// ============= 
const Moment = require('moment');
var _ = require('lodash');
const Util = require('util'); 
const Promise = require('bluebird');
const AWS = require('aws-sdk');

// Use bluebird implementation of Promise
AWS.config.setPromisesDependency(require('bluebird'));

// ============= 
// CONSTS 
// ============= 
 
// ============= 
// FUNCTIONS 
// ============= 
 
 
const isEmptyObject = (obj) => { 
// ----------------------------------------------------       
  return !Object.keys(obj).length; 
}; 
 
const isUndefined = (obj) => { 
// ---------------------------------------------------- 
  for (var prop in obj) { 
    if (obj.hasOwnProperty(prop) && (obj[prop] != undefined || obj[prop] != null)) {return false;} 
  } 
  return true; 
}; 

const areAnyArrayItemsDefined = (arr) => {
// ----------------------------------------------------
  arr.forEach(element => {
    if (typeof element !== 'undefined') {
      return true;
    }
  });
  return false;
};
 
const objectProp = (obj, is, value) => { 
// ----------------------------------------------------         
    if (typeof is == 'string') 
      return objectProp(obj, is.split('/'), value); 
    else if (is.length==1 && value!==undefined) { 
      return obj[is[0]] = value; 
    } 
    else if (is.length==0) {    
      return obj; 
    } 
    else { 
      return objectProp(obj[is[0]], is.slice(1), value); 
    } 
} 

const stringToDate = (dateString) => { 
  // ---------------------------------------------------------------------------- 
    if(!isNaN(dateString)){return excelDaysToDate(dateString);}
    else if(Moment(dateString,"DD-MM-YYYY").isValid()) {return Moment(dateString,"DD-MM-YYYY").toDate();}
    else if(Moment(dateString,"MM-DD-YYYY").isValid()) {return Moment(dateString,"MM-DD-YYYY").toDate();}
    else if(Moment(dateString,"YYYY-MM-DD").isValid()) {return Moment(dateString,"YYYY-MM-DD").toDate();}
    else {return null;}
  }; 

const excelDaysToDate = (excelDays) => { 
  // ---------------------------------------------------------------------------- 
    return (new Date((excelDays - 25569) * 86400 * 1000));
  }; 

const excelDateToDate = (excelDate) => { 
// ---------------------------------------------------------------------------- 
  var date;  

  switch(typeof excelDate){
    // Could be a string!
    case "string":
      date = stringToDate(excelDate);
      break;

    // Could be a number!
    // Could also be anything, really!
    case "number":
    default:
        date = excelDaysToDate(excelDate);
      break;
  } 

  return date;
}; 
 
const excelDateToDateString = (excelDate) => { 
// ---------------------------------------------------------------------------- 
  var date = excelDateToDate(excelDate).toISOString();  
  return date.substring(0, date.indexOf('T')); 
}; 
 
const timestampToDateString = (timestamp) => { 
// ---------------------------------------------------------------------------- 
  var date = (new Date(timestamp * 1000)).toISOString(); 
  return date.substring(0, date.indexOf('T')); 
}; 
 
const escapeRegExp = (str) => { 
// ----------------------------------------------------------------------------   
  return str.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1"); 
}; 
 
const replaceAll = (str, find, value) => { 
// ----------------------------------------------------------------------------    
  return str.replace(new RegExp(escapeRegExp(find), 'g'), value); 
}; 
 
const camelize = (str) => { 
  // ----------------------------------------------------------------------------    
  return str.replace(/(?:^\w|[A-Z]|\b\w)/g, function(letter, index) { 
    return index == 0 ? letter.toLowerCase() : letter.toUpperCase(); 
  }).replace(/\s+/g, ''); 
} 
 
const padNumeric = (numberToPad, proposedLength, paddingChar) => { 
  paddingChar = paddingChar || '0'; 
  numberToPad = numberToPad.toString(); 
  return numberToPad.length >= proposedLength ? numberToPad : new Array(proposedLength - numberToPad.length + 1).join(paddingChar) + numberToPad; 
} 
 
const debugLogVariableAndType = (variable) => { 
    // ----------------------------------------------------------------------------    
  console.debug('**VARIABLE CHECK**'); 
  console.debug('Value: ' + variable); 
  console.debug('Type: ' + typeof variable); 
  return; 
} 
 
const flattenArray = (arr) => { 
  // ----------------------------------------------------------------------------    
  return arr.reduce(function (flat, toFlatten) { 
    return flat.concat(Array.isArray(toFlatten) ? flattenArray(toFlatten) : toFlatten); 
  }, []); 
} 

const getSecret = (secretName, region = "ap-southeast-2") => {
  // ----------------------------------------------------------------------------      

  // Create a Secrets Manager client in the appropriate region
  var smclient = new AWS.SecretsManager({
    endpoint: "https://secretsmanager." + region + ".amazonaws.com",
    region: region
  });

  // Get the secret!
  return smclient.getSecretValue({ SecretId: secretName }).promise()
    .then(res => {
      if (res.err) {
        // Throw an exception if the secret was not found. We don't handle it here, 
        // so need to handle it elsewhere.
        if (err.code === 'ResourceNotFoundException') { throw new ParameterError("The requested secret " + secretName + " was not found"); }
        else if (err.code === 'InvalidRequestException') { throw new ParameterError("The request was invalid due to: " + err.message); }
        else if (err.code === 'InvalidParameterException') { throw new ParameterError("The request had invalid params: " + err.message); }
      }
      else {
        // Decrypted secret using the associated KMS CMK
        if (res.SecretString == "") { throw new ParameterError("Permissions DB Connection Parameters are NULL!"); }

        // All good! resolve with A JSON object representation
        // of the secret (bit more useful than a string).
        return JSON.parse(res.SecretString);
      }
    });
}

// Helper method. This method allows you to retrieve the value of a nested object (or array)
// using the following syntax:
// 
//   const name = getNestedObject(user, ['personalInfo', 'name']);
//
// This will get the retrieve user.personalInfo.name, returning undefined
// if any sub-object on the path does not exist. Also works for arrays:
//
//   const city = getNestedObject(user, ['personalInfo', 'addresses', 0, 'city']);
//
// Originally from here: https://hackernoon.com/accessing-nested-objects-in-javascript-f02f1bd6387f
const getNestedObject = (nestedObj, pathArr) => {
  // ---------------------------------------------------------------------------- 
    return pathArr.reduce((obj, key) =>
        (obj && obj[key] !== 'undefined') ? obj[key] : undefined, nestedObj);
}

// Attribution: https://stackoverflow.com/a/38340730/7950322
const cleanNullProps = (obj) => {
  // ---------------------------------------------------------------------------- 
  return Object.keys(obj).forEach((key) => obj[key] === null && delete myObj[key]);
}

// Attribution: https://stackoverflow.com/a/38340730/7950322
const cleanNullPropsRecursive = (obj) => {
  // Clone for immutability
  let cleanedObj = _.cloneDeep(obj);

  Object.keys(cleanedObj).forEach(key => {
    if (cleanedObj[key] && typeof cleanedObj[key] === "object") cleanedObj[key] = cleanNullPropsRecursive(cleanedObj[key]); // recurse
    else if (cleanedObj[key] == null) delete cleanedObj[key]; // delete
  });
  return cleanedObj;
};
 
// ============= 
// EXPORTS 
// ============= 
 
module.exports = { 
  isEmptyObject, 
  isUndefined, 
  areAnyArrayItemsDefined,
  objectProp, 
  stringToDate,
  excelDaysToDate,
  excelDateToDate, 
  excelDateToDateString, 
  timestampToDateString, 
  escapeRegExp, 
  replaceAll, 
  camelize, 
  padNumeric, 
  debugLogVariableAndType, 
  flattenArray,
  getSecret,
  getNestedObject,
  cleanNullProps,
  cleanNullPropsRecursive
}; 
