'use strict';

const Promise = require('bluebird');
const AWS = require('aws-sdk');
const _ = require('lodash');
AWS.config.update({ region: "ap-southeast-2" });
AWS.config.setPromisesDependency(require('bluebird'));

const getRestApiId = (data, searchApiName) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {
    // -------------------------------------------
    console.log('documentation.getRestApiId(...)');
    let apiId = null;
    let apiDetail = data.items.filter((api) => {
      return api.name === searchApiName
    })

    // Check if the API in question has been found
    if (apiDetail.length > 0) {
      apiId = apiDetail[0].id;
      // We want to generate a Swagger document using the APIGateway Node SDK class
      resolve({ apiId: apiId })
    }
    else {
      reject(new Error('API not found in API gateway or more than 500 APIs!')) // An unexpected error has occurred
    }
  });
};


const generateRestApiDocs = (apiGateway, restApiId, apiStage) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {
    // -------------------------------------------
    console.log('documentation.generateRestApiDocs(...)');

    let params = {
      exportType: 'oas30',
      restApiId: restApiId,
      stageName: apiStage,
      accepts: 'application/json',
      parameters: {
        'extensions': 'apigateway'
      }
    }

    let getApiExportPromise = apiGateway.getExport(params).promise();

    getApiExportPromise
      .then((res) => {
        let openApiDocs = JSON.parse(res.body.toString('utf8'));
        return resolve(openApiDocs);
      })
      .catch(err => {
        console.error(err, err.stack);
        return reject(new Error('API Gateway documentation generation failed. Please review documentation.js.generateRestApiDocs'));
      })

  });
}


const supplementRestApiDocs = (openApiDocs) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {
    console.log('documentation.supplmentRestApiDocs(...)');
    // Supplementing the auto-generated API docs in order to ensure SwaggerHub Compatibility
    // Clone the incoming openApiDocs to make this a pure function
    let supplementedApiDocs = _.cloneDeep(openApiDocs);

    // (1) For all methods in the API docs provide a minimal responses object
    let responses = {
      '200': {
        description: '200 response'
      },
      '400': {
        description: 'Bad request'
      },
      '401': {
        description: 'Authorisation error'
      }
    };

    // (2) Prune all options methods for the documentation
    //    (a) To reduce verbose-ness
    //    (b) Given these are implicit calls
    for (var path in openApiDocs.paths) {
      // ----------------------------
      for (var method in openApiDocs.paths[path]) {
        // ----------------------------
        // Reset the found params array for each method
        if (method !== 'options') {
          // ----------------------------
          // Add the responses object for all non-options methods
          supplementedApiDocs.paths[path][method].responses = responses;
        }
        else if (method === 'options') {
          delete supplementedApiDocs.paths[path].options
        }
      }
    }
    console.log(JSON.stringify(supplementedApiDocs));
    return resolve(supplementedApiDocs);
  })
}


const s3PutRestApiDocs = (serviceName, bucketName, file) => {
  // ---------------------------------------------------------------------------- 
  return new Promise((resolve, reject) => {
    console.log('documentation.s3PutRestApiDocs(...)');

    // Generate a key for the object - this is designed to be versioned under the same filename
    let docsKey = `${serviceName}_documentation.json`;

    let params = {
      Bucket: bucketName,
      Key: docsKey,
      Body: JSON.stringify(file)
    }

    let s3 = new AWS.S3();
    let s3uploadPromise = s3.upload(params).promise();

    s3uploadPromise
      .then((res) => {
        return resolve(res)
      })
      .catch(err => {
        console.error(err, err.stack);
        return reject(new Error('API Gateway documentation generation failed. Please review documentation.js.generateRestApiDocs'));
      });
  });
}


const handler = () => {

  let docsBucketName = process.env.DOCUMENTATION_BUCKET_NAME;
  let searchApiName = `${process.env.ENV_LOWER}-${process.env.SERVICE_NAME}`;
  let restApiId = null;
  let apiStage = process.env.ENV_LOWER;

  // Get a list of all REST APIs for filtering
  let apiGateway = new AWS.APIGateway();
  let params = {
    limit: 500
  }
  let getRestApisPromise = apiGateway.getRestApis(params).promise();

  getRestApisPromise
    .then((res) => { return getRestApiId(res, searchApiName) })
    .then(res => { restApiId = res.apiId; return generateRestApiDocs(apiGateway, restApiId, apiStage) })
    .then(res => { return supplementRestApiDocs(res) })
    .then(res => { return s3PutRestApiDocs(searchApiName, docsBucketName, res) })
    .then(res => { return; })
    .catch(err => {
      // -------------------------------------------
      console.error(err, err.stack) // An unexpected error has occurred
      return process.exit(1);
    })
}

handler();

