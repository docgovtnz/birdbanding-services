# doc-birdbanding-source

Background: https://docgovtnz.github.io/docs/projects/falcon

Serverless-based APIs for FALCON.

## Structure

* `./src/api`
  * The parent serverless framework API Gateway/Lambda REST API (i.e. `api_2`/`api_3` are dependent on this existing first)
  * This relay of linked API services was not ideal but a CloudFormation resource limit of 200 prevents extending the parent API any further
* `./src/api/cfg`
  * All raw JSON schema files deployed for the birdbanding service
  * Deployment of these are defined in environment specific txt files (e.g. deploy.dev.txt at ./src/api)
* `./src/api_2`
  * First child serverless framework API Gateway/Lambda REST API (i.e. child of `./src/api` and dependent on this existing)
* `./src/api_3`
  * Second child serverless framework API Gateway/Lambda REST API (i.e. child of `./src/api` and dependent on this existing)
* `./src/layers`
  * CloudFormation templates for the Lambda layers
  * Code for each of two Lambda layers for birdbanding:
    * `birdbanding-layer-common` common external dependencies
    * `birdbanding-layer-internal` internally developed code shared across functions
* `./tst`
  * Includes mocha tests setup for more complex code functionality
  * Newman collection and environments used for automated testing in the CI/CD pipeline 

## Licence

FALCON (New Zealand Bird Banding Database)  
Copyright (C) 2021 Department of Conservation | Te Papa Atawhai, Pikselin Limited, Fronde Systems Group Limited

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.