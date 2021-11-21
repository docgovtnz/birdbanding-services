const Promise = require('bluebird');
const Moment = require('moment');
const BoilerPlate = require('api-boilerplate.js');
var AWSXRay = require('aws-xray-sdk');
var AWS = AWSXRay.captureAWS(require('aws-sdk'));
AWS.config.setPromisesDependency(require('bluebird'));
let containerCreationTimestamp;

const getSchemaList = async () => {
  // ------------------------------------------
  console.info('json-schema.getSchemaList()');

  let ddbClient = new AWS.DynamoDB.DocumentClient();

  let params = {
    TableName: process.env.PARAMETER_SCHEMA_TABLE
  };

  let result = await ddbClient.scan(params).promise();
  return result.Items;
}

function fetchSchema(schema) {
    return BoilerPlate.getSchemaFromDynamo(process.env.PARAMETER_SCHEMA_TABLE, schema.id, schema.version)
}

function processQueryString(schemas, q) {
    const schemaNames = q.split(',').map(s => s.split("="));
    let tmp = schemaNames.map(s => {
        if (s.length > 1) {
            return getSchemaWithVersion(schemas, s[0], s[1]);
        }
        else {
            return getLatestSchema(schemas, s[0])
        }
    })
    return tmp;
}

function getLatestSchema(schemas, name) {
    let latestVersion = schemas.reduce((latest, schema) => {
        if (schema.param_name == name && schema.version > latest.version) {
            return { name: schema.param_name, id: schema.id, version: schema.version };
        }
        else {
            return latest;
        }
    }, {name: name, version: -1, id: null });
    return latestVersion;
}

function getSchemaWithVersion(schemas, name, version) {
    const schema = schemas.find(schema => (schema.param_name == name && schema.version == version));
    if (schema == null){
        return { name: name, id: null, version: version};
    }
    else{
        return { name: schema.param_name, id: schema.id, version: schema.version };
    }
}

module.exports.get = async (event, context, cb) => {
    console.debug("__________________Retrieving schema(s)_______________");
    // ----------------------------------------------------------------------------
    context.callbackWaitsForEmptyEventLoop = false;

    if (typeof containerCreationTimestamp === 'undefined') {
        containerCreationTimestamp = Moment();
    }
    console.info(Moment().diff(containerCreationTimestamp, 'seconds') + ' seconds since container established...');

    let schemas = await getSchemaList();

    console.log(schemas); 

    // Respond to a ping request 
    if ('source' in event && event.source === 'serverless-plugin-warmup') {
        console.log('Lambda PINGED.');
        return cb(null, 'Lambda PINGED.');
    }
    console.log('DEBUG: ', JSON.stringify(event));
    let statusCode, returnMsgs = {};
    let schemaReqs, schemaDefs;
    
    try {
        await BoilerPlate.cognitoGroupAuthCheck(event, process.env.AUTHORIZED_GROUP_LIST);
        try {
            if (event.queryStringParameters && event.queryStringParameters.schemaList) {
                schemaReqs = processQueryString(schemas, event.queryStringParameters.schemaList);
            }
            else {
                // if no list, fetch them all
                schemaReqs = schemas.map(s => ({ name: s.param_name, id: s.id, version: s.version }));
            }
            console.log("Schemas to be requested: ");
            console.dir(schemaReqs);
            let validSchema = [];
            schemaReqs.forEach(s => {
                if(s.id == null){ 
                    console.log(`${s.name} v${s.version} not found`);
                    throw new BoilerPlate.NotFoundError('SchemaNotFound', { type: 'NOT_FOUND', message: `JSON schema cannot be found for name: ${s.name} version: ${s.version}`, data: { path: `queryStringParameters.schemaList` }, value: `${s.name} v${s.version}`, schema: null });
                }
                else{
                    validSchema.push(s);
                }
            });
            schemaDefs = await Promise.all(validSchema.map(s => fetchSchema(s)));
            schemaDefs.forEach((s, n) => {
                let name = validSchema[n].name;
                s.version = validSchema[n].version;
                if (event.queryStringParameters && event.queryStringParameters.schemaList && schemaDefs.length === 1) {
                  returnMsgs[name] = s;
                }
                else if(returnMsgs[name]) {
                    returnMsgs[name].push(s);

                }
                else{
                    returnMsgs[name] = [s];
                }
            });
        } catch (e) {
          if (e instanceof BoilerPlate.NotFoundError) {
            throw e;
          }else {
            console.dir(e);
            throw (new BoilerPlate.ServiceError("Schema retrieval error", e))
          }
        }
        console.log("Result set___________________");
        console.log(JSON.stringify(returnMsgs));

        statusCode = 200;
    }

    catch (e) {
      console.log(e);
        if (e instanceof BoilerPlate.AuthorisationError) {
            statusCode = e.statusCode;
            returnMsgs = e;
        }
        else if (e instanceof BoilerPlate.NotFoundError) {
          statusCode = e.statusCode;
          returnMsgs = e;
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