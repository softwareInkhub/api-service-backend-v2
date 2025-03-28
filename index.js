import express from 'express';
import { OpenAPIBackend } from 'openapi-backend';
import swaggerUi from 'swagger-ui-express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import yaml from 'js-yaml';
import { v4 as uuidv4 } from 'uuid';
import cors from 'cors'
import axios from 'axios';
import { handlers as pinterestHandlers } from './pinterest-handlers.js';
import { handlers as dynamodbHandlers } from './lib/dynamodb-handlers.js';
import dotenv from 'dotenv';
import { handlers as awsMessagingHandlers } from './aws-messaging-handlers.js';
import { saveSingleExecutionLog, savePaginatedExecutionLogs } from './executionHandler.js';

dotenv.config();  

const app = express();
app.use(express.json());
app.use(cors());
// File storage configuration
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);



app.get("/test",(req,res)=>{res.send("hello! world");
})

// Initialize main OpenAPI backend
const mainApi = new OpenAPIBackend({
  definition: './openapi.yaml',
  quick: true,
  handlers: {
    validationFail: async (c, req, res) => ({
      statusCode: 400,
      error: c.validation.errors
    }),
    notFound: async (c, req, res) => ({
      statusCode: 404,
      error: 'Not Found'
    }),

    // Account handlers
    getAllAccounts: async (c, req, res) => {
      console.log('[getAllAccounts] Request:', {
        method: 'GET',
        url: '/tables/brmh-namespace-accounts/items'
      });

      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'brmh-namespace-accounts'
            }
          }
        });

        console.log('[getAllAccounts] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (!response.body || !response.body.items) {
          return {
            statusCode: 200,
            body: []
          };
        }

        return {
          statusCode: 200,
          body: response.body.items.map(item => item.data)
        };
      } catch (error) {
        console.error('[getAllAccounts] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get all accounts', details: error.message }
        };
      }
    },

    getAccountById: async (c, req, res) => {
      const accountId = c.request.params.accountId;
      console.log('[getAccountById] Request:', {
        method: 'GET',
        url: `/tables/brmh-namespace-accounts/items/${accountId}`,
        params: { accountId }
      });

      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'brmh-namespace-accounts'
            },
            requestBody: {
              TableName: 'brmh-namespace-accounts',
              FilterExpression: "id = :accountId",
              ExpressionAttributeValues: {
                ":accountId": accountId
              }
            }
          }
        });

        console.log('[getAccountById] DynamoDB Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (!response.body || !response.body.items || response.body.items.length === 0) {
          console.log('[getAccountById] Account not found');
          return {
            statusCode: 404,
            body: { error: 'Account not found' }
          };
        }

        const accountData = response.body.items[0].data;
        console.log('[getAccountById] Returning account data:', accountData);

        return {
          statusCode: 200,
          body: accountData
        };
      } catch (error) {
        console.error('[getAccountById] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get account', details: error.message }
        };
      }
    },

    // Method handlers
    getAllMethods: async (c, req, res) => {
      console.log('[getAllMethods] Request:', {
        method: 'GET',
        url: '/tables/brmh-namespace-methods/items'
      });

      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'brmh-namespace-methods'
            }
          }
        });

        console.log('[getAllMethods] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (!response.body || !response.body.items) {
          return {
            statusCode: 200,
            body: []
          };
        }

        return {
          statusCode: 200,
          body: response.body.items.map(item => item.data)
        };
      } catch (error) {
        console.error('[getAllMethods] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get all methods', details: error.message }
        };
      }
    },

    getMethodById: async (c, req, res) => {
      const methodId = c.request.params.methodId;
      console.log('[getMethodById] Request:', {
        method: 'GET',
        url: `/tables/brmh-namespace-methods/items/${methodId}`,
        params: { methodId }
      });

      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'brmh-namespace-methods'
            },
            requestBody: {
              TableName: 'brmh-namespace-methods',
              FilterExpression: "id = :methodId",
              ExpressionAttributeValues: {
                ":methodId": methodId
              }
            }
          }
        });

        console.log('[getMethodById] DynamoDB Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (!response.body || !response.body.items || response.body.items.length === 0) {
          console.log('[getMethodById] Method not found');
          return {
            statusCode: 404,
            body: { error: 'Method not found' }
          };
        }

        const methodData = response.body.items[0].data;
        console.log('[getMethodById] Returning method data:', methodData);

        return {
          statusCode: 200,
          body: methodData
        };
      } catch (error) {
        console.error('[getMethodById] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get method', details: error.message }
        };
      }
    },

    getNamespaceAccounts: async (c, req, res) => {
      const namespaceId = c.request.params.namespaceId;
      console.log('[getNamespaceAccounts] Request for namespace:', namespaceId);

      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'brmh-namespace-accounts'
            },
            requestBody: {
              TableName: 'brmh-namespace-accounts',
              FilterExpression: "#data.#nsid.#S = :namespaceId",
              ExpressionAttributeNames: {
                "#data": "data",
                "#nsid": "namespace-id",
                "#S": "S"
              },
              ExpressionAttributeValues: {
                ":namespaceId": { "S": namespaceId }
              }
            }
          }
        });

        console.log('[getNamespaceAccounts] DynamoDB Response:', {
          statusCode: response.statusCode,
          body: JSON.stringify(response.body, null, 2),
          items: response.body?.items?.length || 0
        });

        if (!response.body || !response.body.items) {
          console.log('[getNamespaceAccounts] No accounts found for namespace:', namespaceId);
          return {
            statusCode: 200,
            body: []
          };
        }

        // Convert DynamoDB format to regular objects
        const accounts = response.body.items
          .filter(item => {
            const data = item.data?.M;
            return data && data['namespace-id']?.S === namespaceId;
          })
          .map(item => {
            const data = item.data.M;
            return {
              'namespace-id': data['namespace-id'].S,
              'namespace-account-id': data['namespace-account-id'].S,
              'namespace-account-name': data['namespace-account-name'].S,
              'namespace-account-url-override': data['namespace-account-url-override']?.S || '',
              'namespace-account-header': data['namespace-account-header']?.L?.map(header => ({
                key: header.M.key.S,
                value: header.M.value.S
              })) || [],
              'variables': data['variables']?.L?.map(variable => ({
                key: variable.M.key.S,
                value: variable.M.value.S
              })) || [],
              'tags': data['tags']?.L?.map(tag => tag.S) || []
            };
          });

        console.log('[getNamespaceAccounts] Found accounts:', accounts.length);
        console.log('[getNamespaceAccounts] Account data:', JSON.stringify(accounts, null, 2));

        return {
          statusCode: 200,
          body: accounts
        };
      } catch (error) {
        console.error('[getNamespaceAccounts] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get namespace accounts', details: error.message }
        };
      }
    },

    getNamespaceMethods: async (c, req, res) => {
      const namespaceId = c.request.params.namespaceId;
      console.log('[getNamespaceMethods] Request:', {
        method: 'GET',
        url: `/tables/brmh-namespace-methods/items`,
        params: { namespaceId }
      });

      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'brmh-namespace-methods'
            },
            requestBody: {
              TableName: 'brmh-namespace-methods',
              FilterExpression: "#data.#nsid.#S = :namespaceId",
              ExpressionAttributeNames: {
                "#data": "data",
                "#nsid": "namespace-id",
                "#S": "S"
              },
              ExpressionAttributeValues: {
                ":namespaceId": { "S": namespaceId }
              }
            }
          }
        });

        console.log('[getNamespaceMethods] DynamoDB Response:', {
          statusCode: response.statusCode,
          body: JSON.stringify(response.body, null, 2),
          items: response.body?.items?.length || 0
        });

        if (!response.body || !response.body.items) {
          console.log('[getNamespaceMethods] No methods found for namespace:', namespaceId);
          return {
            statusCode: 200,
            body: []
          };
        }

        // Convert DynamoDB format to regular objects
        const methods = response.body.items
          .filter(item => {
            const data = item.data?.M;
            return data && data['namespace-id']?.S === namespaceId;
          })
          .map(item => {
            const data = item.data.M;
            return {
              'namespace-id': data['namespace-id'].S,
              'namespace-method-id': data['namespace-method-id'].S,
              'namespace-method-name': data['namespace-method-name'].S,
              'namespace-method-type': data['namespace-method-type'].S,
              'namespace-method-url-override': data['namespace-method-url-override']?.S || '',
              'namespace-method-queryParams': data['namespace-method-queryParams']?.L?.map(param => ({
                key: param.M.key.S,
                value: param.M.value.S
              })) || [],
              'namespace-method-header': data['namespace-method-header']?.L?.map(header => ({
                key: header.M.key.S,
                value: header.M.value.S
              })) || [],
              'save-data': data['save-data']?.BOOL || false,
              'isInitialized': data['isInitialized']?.BOOL || false,
              'tags': data['tags']?.L?.map(tag => tag.S) || [],
              'sample-request': data['sample-request']?.M || null,
              'sample-response': data['sample-response']?.M || null,
              'request-schema': data['request-schema']?.M || null,
              'response-schema': data['response-schema']?.M || null
            };
          });

        console.log('[getNamespaceMethods] Found methods:', methods.length);
        console.log('[getNamespaceMethods] Method data:', JSON.stringify(methods, null, 2));

        return {
          statusCode: 200,
          body: methods
        };
      } catch (error) {
        console.error('[getNamespaceMethods] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get namespace methods', details: error.message }
        };
      }
    },

    // Namespace handlers using DynamoDB
    getNamespaces: async (c, req, res) => {
      console.log('[getNamespaces] Request:', {
        method: 'GET',
        url: '/tables/brmh-namespace/items'
      });

      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'brmh-namespace'
            }
          }
        });

        console.log('[getNamespaces] Full Response:', JSON.stringify(response.body, null, 2));

        if (!response.body || !response.body.items) {
          return {
            statusCode: 200,
            body: []
          };
        }

        // Convert DynamoDB format to regular objects
        const items = response.body.items.map(item => {
          const converted = {};
          Object.entries(item).forEach(([key, value]) => {
            // Extract the actual value from DynamoDB attribute type
            converted[key] = Object.values(value)[0];
          });
          return converted;
        });

        console.log('[getNamespaces] Converted items:', JSON.stringify(items, null, 2));
        
        return {
          statusCode: 200,
          body: items
        };
      } catch (error) {
        console.error('[getNamespaces] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get namespaces', details: error.message }
        };
      }
    },

    createNamespace: async (c, req, res) => {
      const namespaceId = uuidv4();
      const item = {
        id: namespaceId,
        type: 'namespace',
        data: {
          'namespace-id': namespaceId,
          'namespace-name': c.request.requestBody['namespace-name'],
          'namespace-url': c.request.requestBody['namespace-url'],
          'tags': c.request.requestBody['tags'] || [],
          'namespace-accounts': [],
          'namespace-methods': []
        }
      };

      console.log('[createNamespace] Request:', {
        method: 'POST',
        url: '/tables/brmh-namespace/items',
        body: item
      });

      try {
        // First, create the namespace in DynamoDB
        const response = await dynamodbHandlers.createItem({
          request: {
            params: {
              tableName: 'brmh-namespace'
            },
            requestBody: item
          }
        });

        console.log('[createNamespace] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        // Generate OpenAPI specification
        console.log('[createNamespace] Generating OpenAPI specification');
        const openApiSpec = {
          openapi: '3.0.0',
          info: {
            title: `${item.data['namespace-name']} API`,
            version: '1.0.0',
            description: `API specification for ${item.data['namespace-name']} namespace`
          },
          servers: [
            {
              url: item.data['namespace-url'],
              description: 'Base API URL'
            }
          ],
          paths: {},
          components: {
            schemas: {},
            securitySchemes: {}
          },
          tags: item.data.tags.map(tag => ({
            name: tag,
            description: `${tag} related operations`
          }))
        };

        // Convert to YAML
        console.log('[createNamespace] Converting OpenAPI spec to YAML');
        const yamlSpec = yaml.dump(openApiSpec, {
          indent: 2,
          lineWidth: -1,
          noRefs: true
        });

        // Save YAML to DynamoDB
        console.log('[createNamespace] Saving YAML to DynamoDB');
        const yamlId = uuidv4();
        const yamlItem = {
          id: yamlId,
          type: 'yaml',
          data: {
            'yaml-id': yamlId,
            'namespace-id': namespaceId,
            'namespace-name': item.data['namespace-name'],
            'yaml-content': yamlSpec,
            'created-at': new Date().toISOString()
          }
        };

        const yamlResponse = await dynamodbHandlers.createItem({
          request: {
            params: {
              tableName: 'yaml'
            },
            requestBody: yamlItem
          }
        });

        console.log('[createNamespace] YAML saved:', {
          statusCode: yamlResponse.statusCode,
          yamlId
        });

        // Add YAML ID to namespace data
        item.data['yaml-id'] = yamlId;

        return {
          statusCode: 201,
          body: item.data
        };
      } catch (error) {
        console.error('[createNamespace] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to create namespace', details: error.message }
        };
      }
    },

    getNamespaceById: async (c, req, res) => {
      const namespaceId = c.request.params.namespaceId;
      
      console.log('[getNamespaceById] Request:', {
        method: 'GET',
        url: `/tables/brmh-namespace/items/${namespaceId}`,
        params: { namespaceId }
      });

      try {
        const response = await dynamodbHandlers.getItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace',
              id: namespaceId
            }
          }
        });

        console.log('[getNamespaceById] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (!response.body || !response.body.items || response.body.items.length === 0) {
          console.log('[getNamespaceById] Namespace not found');
          return {
            statusCode: 404,
            body: { error: 'Namespace not found' }
          };
        }

        const namespaceData = response.body.items[0].data;
        console.log('[getNamespaceById] Returning namespace data:', namespaceData);

        return {
          statusCode: 200,
          body: namespaceData
        };
      } catch (error) {
        console.error('[getNamespaceById] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get namespace', details: error.message }
        };
      }
    },

    updateNamespace: async (c, req, res) => {
      const namespaceId = c.request.params.namespaceId;
      
      // First, get the existing namespace to ensure it exists
      try {
        const getResponse = await dynamodbHandlers.getItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace',
              id: namespaceId
            }
          }
        });

        if (!getResponse.body?.items?.[0]) {
          return {
            statusCode: 404,
            body: { error: 'Namespace not found' }
          };
        }

        const updateExpression = {
          UpdateExpression: "SET #data = :value",
          ExpressionAttributeNames: {
            "#data": "data"
          },
          ExpressionAttributeValues: {
            ":value": {
              'namespace-id': namespaceId,
              'namespace-name': c.request.requestBody['namespace-name'],
              'namespace-url': c.request.requestBody['namespace-url'],
              'tags': c.request.requestBody['tags'] || []
            }
          }
        };

        console.log('[updateNamespace] Request:', {
          method: 'PUT',
          url: `/tables/brmh-namespace/items/${namespaceId}`,
          body: updateExpression,
          params: { namespaceId }
        });

        const response = await dynamodbHandlers.updateItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace',
              id: namespaceId
            },
            requestBody: updateExpression
          }
        });

        console.log('[updateNamespace] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (response.statusCode === 404) {
          return {
            statusCode: 404,
            body: { error: 'Namespace not found' }
          };
        }

        return {
          statusCode: 200,
          body: updateExpression.ExpressionAttributeValues[":value"]
        };
      } catch (error) {
        console.error('[updateNamespace] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to update namespace', details: error.message }
        };
      }
    },

    deleteNamespace: async (c, req, res) => {
      const namespaceId = c.request.params.namespaceId;

      console.log('[deleteNamespace] Request:', {
        method: 'DELETE',
        url: `/tables/brmh-namespace/items/namespace#${namespaceId}`,
        params: { namespaceId }
      });

      try {
        const response = await dynamodbHandlers.deleteItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace',
              id: namespaceId
            }
          }
        });

        console.log('[deleteNamespace] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (response.statusCode === 404) {
          return {
            statusCode: 404,
            body: { error: 'Namespace not found' }
          };
        }

        return {
          statusCode: 204
        };
      } catch (error) {
        console.error('[deleteNamespace] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to delete namespace' }
        };
      }
    },

    // Namespace Account handlers using DynamoDB
    createNamespaceAccount: async (c, req, res) => {
      const namespaceId = c.request.params.namespaceId;
      const accountId = uuidv4();
      
      const item = {
        id: accountId,
        type: 'account',
        data: {
          'namespace-id': namespaceId,
          'namespace-account-id': accountId,
          'namespace-account-name': c.request.requestBody['namespace-account-name'],
          'namespace-account-url-override': c.request.requestBody['namespace-account-url-override'],
          'namespace-account-header': c.request.requestBody['namespace-account-header'] || [],
          'variables': c.request.requestBody['variables'] || [],
          'tags': c.request.requestBody['tags'] || []
        }
      };
      console.log('[createNamespaceAccount] Request:', {
        method: 'POST',
        url: '/tables/brmh-namespace-accounts/items',
        body: item
      });

      try {
        const response = await dynamodbHandlers.createItem({
          request: {
            params: {
              tableName: 'brmh-namespace-accounts'
            },
            requestBody: item
          }
        });

        console.log('[createNamespaceAccount] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        return {
          statusCode: 201,
          body: item.data
        };
      } catch (error) {
        console.error('[createNamespaceAccount] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to create namespace account', details: error.message }
        };
      }
    },

    updateNamespaceAccount: async (c, req, res) => {
      const accountId = c.request.params.accountId;
      
      // First, get the existing account to preserve namespace-id
      try {
        const getResponse = await dynamodbHandlers.getItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace-accounts',
              id: accountId
            }
          }
        });

        if (!getResponse.body?.items?.[0]) {
          return {
            statusCode: 404,
            body: { error: 'Account not found' }
          };
        }

        const existingAccount = getResponse.body.items[0];
        const namespaceId = existingAccount.data['namespace-id'];

        const updateExpression = {
          UpdateExpression: "SET #data = :value",
          ExpressionAttributeNames: {
            "#data": "data"
          },
          ExpressionAttributeValues: {
            ":value": {
              'namespace-id': namespaceId,
              'namespace-account-id': accountId,
              'namespace-account-name': c.request.requestBody['namespace-account-name'],
              'namespace-account-url-override': c.request.requestBody['namespace-account-url-override'],
              'namespace-account-header': c.request.requestBody['namespace-account-header'] || [],
              'variables': c.request.requestBody['variables'] || [],
              'tags': c.request.requestBody['tags'] || []
            }
          }
        };

        console.log('[updateNamespaceAccount] Request:', {
          method: 'PUT',
          url: `/tables/brmh-namespace-accounts/items/${accountId}`,
          body: updateExpression
        });

        const response = await dynamodbHandlers.updateItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace-accounts',
              id: accountId
            },
            requestBody: updateExpression
          }
        });

        console.log('[updateNamespaceAccount] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        return {
          statusCode: 200,
          body: updateExpression.ExpressionAttributeValues[":value"]
        };
      } catch (error) {
        console.error('[updateNamespaceAccount] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to update namespace account', details: error.message }
        };
      }
    },

    deleteNamespaceAccount: async (c, req, res) => {
      const accountId = c.request.params.accountId;

      console.log('[deleteNamespaceAccount] Request:', {
        method: 'DELETE',
        url: `/tables/brmh-namespace-accounts/items/${accountId}`,
        params: { accountId }
      });

      try {
        const response = await dynamodbHandlers.deleteItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace-accounts',
              id: accountId
            }
          }
        });

        console.log('[deleteNamespaceAccount] DynamoDB Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (response.statusCode === 404) {
          console.log('[deleteNamespaceAccount] Account not found:', accountId);
          return {
            statusCode: 404,
            body: { error: 'Account not found' }
          };
        }

        return {
          statusCode: 204
        };
      } catch (error) {
        console.error('[deleteNamespaceAccount] Error:', error);
        return {
          statusCode: 500,
          body: { 
            error: 'Failed to delete namespace account',
            details: error.message,
            accountId: accountId
          }
        };
      }
    },

    // Namespace Method handlers using DynamoDB
    createNamespaceMethod: async (c, req, res) => {
      const namespaceId = c.request.params.namespaceId;
      const methodId = uuidv4();
      const item = {
        id: methodId,
        type: 'method',
        data: {
          'namespace-id': namespaceId,
          'namespace-method-id': methodId,
          'namespace-method-name': c.request.requestBody['namespace-method-name'],
          'namespace-method-type': c.request.requestBody['namespace-method-type'],
          'namespace-method-url-override': c.request.requestBody['namespace-method-url-override'],
          'namespace-method-queryParams': c.request.requestBody['namespace-method-queryParams'] || [],
          'namespace-method-header': c.request.requestBody['namespace-method-header'] || [],
          'save-data': c.request.requestBody['save-data'] !== undefined ? c.request.requestBody['save-data'] : false,
          'isInitialized': c.request.requestBody['isInitialized'] !== undefined ? c.request.requestBody['isInitialized'] : false,
          'tags': c.request.requestBody['tags'] || [],
          'sample-request': c.request.requestBody['sample-request'],
          'sample-response': c.request.requestBody['sample-response'],
          'request-schema': c.request.requestBody['request-schema'],
          'response-schema': c.request.requestBody['response-schema']
        }
      };

      console.log('[createNamespaceMethod] Request:', {
        method: 'POST',
        url: '/tables/brmh-namespace-methods/items',
        body: item
      });

      try {
        const response = await dynamodbHandlers.createItem({
          request: {
            params: {
              tableName: 'brmh-namespace-methods'
            },
            requestBody: item
          }
        });

        console.log('[createNamespaceMethod] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        return {
          statusCode: 201,
          body: item.data
        };
      } catch (error) {
        console.error('[createNamespaceMethod] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to create namespace method' }
        };
      }
    },

    updateNamespaceMethod: async (c, req, res) => {
      const methodId = c.request.params.methodId;
      
      // First, get the existing method to preserve namespace-id
      try {
        const getResponse = await dynamodbHandlers.getItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace-methods',
              id: methodId
            }
          }
        });

        if (!getResponse.body?.items?.[0]) {
          return {
            statusCode: 404,
            body: { error: 'Method not found' }
          };
        }

        const existingMethod = getResponse.body.items[0];
        const namespaceId = existingMethod.data['namespace-id'];

        const updateExpression = {
          UpdateExpression: "SET #data = :value",
          ExpressionAttributeNames: {
            "#data": "data"
          },
          ExpressionAttributeValues: {
            ":value": {
              'namespace-id': namespaceId,
              'namespace-method-id': methodId,
              'namespace-method-name': c.request.requestBody['namespace-method-name'],
              'namespace-method-type': c.request.requestBody['namespace-method-type'],
              'namespace-method-url-override': c.request.requestBody['namespace-method-url-override'],
              'namespace-method-queryParams': c.request.requestBody['namespace-method-queryParams'] || [],
              'namespace-method-header': c.request.requestBody['namespace-method-header'] || [],
              'save-data': c.request.requestBody['save-data'] !== undefined ? c.request.requestBody['save-data'] : false,
              'isInitialized': c.request.requestBody['isInitialized'] !== undefined ? c.request.requestBody['isInitialized'] : false,
              'tags': c.request.requestBody['tags'] || [],
              'sample-request': c.request.requestBody['sample-request'],
              'sample-response': c.request.requestBody['sample-response'],
              'request-schema': c.request.requestBody['request-schema'],
              'response-schema': c.request.requestBody['response-schema']
            }
          }
        };

        console.log('[updateNamespaceMethod] Request:', {
          method: 'PUT',
          url: `/tables/brmh-namespace-methods/items/${methodId}`,
          body: updateExpression
        });

        const response = await dynamodbHandlers.updateItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace-methods',
              id: methodId
            },
            requestBody: updateExpression
          }
        });

        console.log('[updateNamespaceMethod] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        return {
          statusCode: 200,
          body: updateExpression.ExpressionAttributeValues[":value"]
        };
      } catch (error) {
        console.error('[updateNamespaceMethod] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to update namespace method', details: error.message }
        };
      }
    },

    deleteNamespaceMethod: async (c, req, res) => {
      const methodId = c.request.params.methodId;

      console.log('[deleteNamespaceMethod] Request:', {
        method: 'DELETE',
        url: `/tables/brmh-namespace-methods/items/${methodId}`,
        params: { methodId }
      });

      try {
        const response = await dynamodbHandlers.deleteItemsByPk({
          request: {
            params: {
              tableName: 'brmh-namespace-methods',
              id: methodId
            }
          }
        });

        console.log('[deleteNamespaceMethod] Response:', {
          statusCode: response.statusCode,
          body: response.body
        });

        if (response.statusCode === 404) {
          return {
            statusCode: 404,
            body: { error: 'Method not found' }
          };
        }

        return {
          statusCode: 204
        };
      } catch (error) {
        console.error('[deleteNamespaceMethod] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to delete namespace method', details: error.message }
        };
      }
    },

    // Execute request handlers
    executeNamespaceRequest: async (c, req, res) => {
      console.log('Executing request with params:', {
        method: c.request.requestBody.method,
        url: c.request.requestBody.url
      });

      const { method, url, queryParams = {}, headers = {}, body = null } = c.request.requestBody;
      const execId = uuidv4();
      
      try {
        // Build URL with query parameters
        const urlObj = new URL(url);
        Object.entries(queryParams).forEach(([key, value]) => {
          if (key && value && key.trim() !== '') {
            urlObj.searchParams.append(key.trim(), value.toString().trim());
          }
        });

        console.log('Final URL:', urlObj.toString());

        // Make the request
        const response = await axios({
          method: method.toUpperCase(),
          url: urlObj.toString(),
          headers: headers,
          data: body,
          validateStatus: () => true // Don't throw on any status
        });

        console.log('Response received:', {
          status: response.status,
          statusText: response.statusText
        });

        // Save execution log
        await saveSingleExecutionLog({
          execId,
          method,
          url: urlObj.toString(),
          queryParams,
          headers,
          responseStatus: response.status,
          responseData: response.data
        });

        // Handle authentication errors specifically
        if (response.status === 401 || response.status === 403) {
          return {
            statusCode: response.status,
            body: {
              error: 'Authentication Failed',
              status: response.status,
              statusText: response.statusText,
              details: response.data,
              suggestions: [
                'Check if the authentication token/key is correct and complete',
                'Verify the token has not expired',
                'Ensure the token has the necessary permissions',
                'Verify you are using the correct authentication method'
              ]
            }
          };
        }

        // Handle other errors
        if (response.status >= 400) {
          return {
            statusCode: response.status,
            body: {
              error: 'API Request Failed',
              status: response.status,
              statusText: response.statusText,
              details: response.data
            }
          };
        }

        return {
          statusCode: response.status,
          body: response.data
        };
      } catch (error) {
        console.error('Request execution error:', {
          message: error.message,
          code: error.code
        });

        // Handle specific error types
        if (error.code === 'ECONNREFUSED') {
          return {
            statusCode: 500,
            body: {
              error: 'Connection Failed',
              details: 'Could not connect to the server. The service might be down or the URL might be incorrect.',
              code: error.code
            }
          };
        }

        return {
          statusCode: 500,
          body: { 
            error: 'Failed to execute request',
            details: error.message,
            code: error.code,
            suggestions: [
              'Verify the URL is correct and accessible',
              'Check if all required headers are properly formatted',
              'Verify the HTTP method is supported',
              'Ensure the request body is properly formatted (if applicable)',
              'Check your network connection'
            ]
          }
        };
      }
    },

<<<<<<< HEAD
     executeNamespacePaginatedRequest: async (c, req, res) => {
      console.log('\n=== PAGINATED REQUEST START ===');
      console.log('Request details:', {
        method: c.request.requestBody.method,
        url: c.request.requestBody.url,
        maxIterations: c.request.requestBody.maxIterations || 10,
        queryParams: c.request.requestBody.queryParams,
        headers: c.request.requestBody.headers,
        tableName: c.request.requestBody.tableName,
        saveData: c.request.requestBody.saveData
      });

      const { 
        method, 
        url, 
        maxIterations = 10,
        queryParams = {}, 
        headers = {}, 
        body = null,
        tableName,
        saveData
      } = c.request.requestBody;

      let currentUrl = url;
      let lastError = null;
      const execId = uuidv4();
      let executionLogs;
=======
    // Add this new function before executeNamespacePaginatedRequest
    saveExecutionLog: async (executionId, pageData, pageCount, totalItemsProcessed, currentUrl, paginationType, isLastPage = false) => {
      try {
        const logId = uuidv4();
        const timestamp = new Date().toISOString();
        
        // Determine if this is a parent log (pageCount === 0) or child log
        const isParentLog = pageCount === 0;
        
        // Create the log item with properly formatted DynamoDB attributes
        const requestBody = {
          'exec-id': isParentLog ? executionId : executionId, // Parent log uses executionId as primary key
          'child-exec-id': isParentLog ? executionId : logId, // Parent log uses same ID, child log uses unique UUID
          type: 'execution_log',
          data: {
            'execution-id': executionId,
            'iteration-no': pageCount, // 0 for parent, 1,2,3... for children
            'total-items-processed': totalItemsProcessed,
            'items-in-current-page': pageData?.items?.length || 0,
            'request-url': currentUrl,
            'response-status': pageData?.data?.status || 200,
            'pagination-type': paginationType || 'none',
            'timestamp': timestamp,
            'status': isParentLog ? ['started'] : 
                     isLastPage ? ['completed'] : 
                     ['progress'],
            'is-last': isLastPage
          }
        };

        console.log('[saveExecutionLog] Saving log:', {
          executionId,
          logId,
          pageCount,
          isParentLog,
          isLastPage,
          itemsCount: pageData?.items?.length || 0,
          requestBody
        });

        // Make the POST request to save the log
        const response = await axios({
          method: 'POST',
          url: 'http://localhost:5000/api/dynamodb/tables/executions/items',
          headers: {
            'Content-Type': 'application/json'
          },
          data: requestBody
        });
>>>>>>> 8730986f34f81e5d482a592b073431e68bf02769

        console.log('[saveExecutionLog] Response:', {
          statusCode: response.status,
          logId,
          data: response.data
        });

        if (response.status === 201 || response.status === 200) {
          return logId;
        } else {
          console.error('[saveExecutionLog] Failed to save log:', response.data);
          return null;
        }
      } catch (error) {
        console.error('[saveExecutionLog] Error:', error.response?.data || error.message);
        return null;
      }
    },

    executeNamespacePaginatedRequest: async (c, req, res) => {
      try {
<<<<<<< HEAD
        // Initialize execution logs
        executionLogs = await savePaginatedExecutionLogs({
          execId,
          method,
          url,
          queryParams,
          headers,
          maxIterations,
          tableName,
          saveData
        });

        if (!executionLogs) {
          throw new Error('Failed to initialize execution logs');
        }

        // Return immediately with execution ID and initial status
        const initialResponse = {
          statusCode: 200,
          body: {
            status: 200,
            data: {
              executionId: execId,
              status: 'initialized',
              method,
              url,
              maxIterations,
              timestamp: new Date().toISOString()
            }
          }
        };

        // Start processing in the background
        (async () => {
          try {
            const pages = [];
            let pageCount = 1;
            let hasMorePages = true;
            let detectedPaginationType = null;
            let totalItemsProcessed = 0;

            // Update parent execution status to inProgress
            await executionLogs.updateParentStatus('inProgress', false);

            // Function to save items to DynamoDB
            const saveItemsToDynamoDB = async (items, pageData) => {
              if (!saveData || !tableName || items.length === 0) return;

              console.log(`\nSaving ${items.length} items to DynamoDB table: ${tableName}`);
              
              const timestamp = new Date().toISOString();
              const baseRequestDetails = {
                method,
                url: pageData.url,
                queryParams,
                headers,
                body
              };

              const BATCH_SIZE = 5;
              const batches = [];
              
              for (let i = 0; i < items.length; i += BATCH_SIZE) {
                batches.push(items.slice(i, i + BATCH_SIZE));
              }

              for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
                const batch = batches[batchIndex];
                console.log(`Processing batch ${batchIndex + 1}/${batches.length} (${batch.length} items)`);

                const savePromises = batch.map(async (item, index) => {
                  // Create a clean copy of the item
                  const cleanedItem = { ...item };
                  
                  // Ensure id is a string
                  if (typeof cleanedItem.id === 'number') {
                    cleanedItem.id = cleanedItem.id.toString();
                  }

                  // Remove bookmark and url fields from the item
                  const { bookmark, url, ...itemWithoutBookmark } = cleanedItem;

                  // Keep only essential fields and primitive values
                  const simplifiedItem = Object.entries(itemWithoutBookmark).reduce((acc, [key, value]) => {
                    if (typeof value === 'string' || 
                        typeof value === 'number' || 
                        typeof value === 'boolean' ||
                        value === null ||
                        Array.isArray(value) ||
                        (typeof value === 'object' && value !== null)) {
                      acc[key] = value;
                    }
                    return acc;
                  }, {});
                  
                  const itemData = {
                    id: cleanedItem.id || `item_${timestamp}_${batchIndex}_${index}`,
                    Item: simplifiedItem,
                    timestamp,
                    _metadata: {
                      requestDetails: baseRequestDetails,
                      status: pageData.status,
                      itemIndex: batchIndex * BATCH_SIZE + index,
                      totalItems: items.length,
                      originalId: item.id
                    }
                  };

                  try {
                    const dbResponse = await dynamodbHandlers.createItem({
                      request: {
                        params: {
                          tableName
                        },
                        requestBody: itemData
                      }
                    });

                    if (!dbResponse.ok) {
                      console.error('Failed to save item:', dbResponse);
                      return null;
                    }

                    console.log(`Successfully saved item ${batchIndex * BATCH_SIZE + index + 1}/${items.length}`);
                    return index;
                  } catch (error) {
                    console.error(`Error saving item ${batchIndex * BATCH_SIZE + index + 1}:`, error);
                    return null;
                  }
                });

                await Promise.all(savePromises);
                console.log(`Completed batch ${batchIndex + 1}/${batches.length}`);
              }

              console.log(`Completed saving ${items.length} items to DynamoDB`);
            };

            // Function to detect pagination type from response
            const detectPaginationType = (response) => {
              // Check for Link header pagination (Shopify style)
              if (response.headers.link && response.headers.link.includes('rel="next"')) {
                return 'link';
              }
              
              // Check for bookmark pagination (Pinterest style)
              if (response.data && response.data.bookmark) {
                return 'bookmark';
              }

              // Check for cursor-based pagination
              if (response.data && (response.data.next_cursor || response.data.cursor)) {
                return 'cursor';
              }

              // Check for offset/limit pagination
              if (response.data && (response.data.total_count !== undefined || response.data.total !== undefined)) {
                return 'offset';
              }

              return null;
            };

            // Extract next URL from Link header (Shopify)
            const extractNextUrl = (linkHeader) => {
              if (!linkHeader) return null;
              const matches = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
              return matches ? matches[1] : null;
            };

            // Extract bookmark from response (Pinterest)
            const extractBookmark = (responseData) => {
              if (!responseData) return null;
              return responseData.bookmark || null;
            };

            // Extract cursor from response
            const extractCursor = (responseData) => {
              if (!responseData) return null;
              return responseData.next_cursor || responseData.cursor || null;
            };

            while (hasMorePages && pageCount <= maxIterations) {
              console.log(`\n=== PAGE ${pageCount} START ===`);
              
              // Build URL with query parameters
              const urlObj = new URL(currentUrl);
              
              // Only add query parameters if they're not already in the URL and it's the first page
              if (pageCount === 1) {
                Object.entries(queryParams).forEach(([key, value]) => {
                  if (value && !urlObj.searchParams.has(key)) {
                    urlObj.searchParams.append(key, value);
                  }
                });
              }

              // Make request
              console.log('Making request to:', urlObj.toString());
              const response = await axios({
                method: method.toUpperCase(),
                url: urlObj.toString(),
                headers: headers,
                data: !['GET', 'HEAD'].includes(method.toUpperCase()) ? body : undefined,
                validateStatus: () => true
              });

              console.log('Response received:', {
                status: response.status,
                statusText: response.statusText,
                headers: response.headers,
                dataLength: response.data ? JSON.stringify(response.data).length : 0,
                data: response.data
              });

              // Handle API errors
              if (response.status >= 400) {
                lastError = {
                  status: response.status,
                  statusText: response.statusText,
                  data: response.data,
                  url: urlObj.toString()
                };
                console.error(`\nAPI Error on page ${pageCount}:`, lastError);
                
                // For Shopify API, check if it's a rate limit error
                if (response.status === 429 || 
                    (response.data && 
                     response.data.errors && 
                     (Array.isArray(response.data.errors) ? 
                       response.data.errors.some(err => err.includes('rate limit')) :
                       response.data.errors.toString().includes('rate limit')))) {
                  console.log('Rate limit detected, waiting before retry...');
                  await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
                  continue; // Retry the same page
                }
                
                // For other errors, stop pagination
                hasMorePages = false;
                break;
              }

              // Detect pagination type on first request if not specified
              if (pageCount === 1) {
                detectedPaginationType = detectPaginationType(response);
                console.log('Detected pagination type:', detectedPaginationType);
              }

              // Process response data
              let currentPageItems = [];
              if (response.data) {
                // Handle different response structures
                if (Array.isArray(response.data)) {
                  currentPageItems = response.data;
                } else if (response.data.data && Array.isArray(response.data.data)) {
                  currentPageItems = response.data.data;
                } else if (response.data.items && Array.isArray(response.data.items)) {
                  currentPageItems = response.data.items;
                } else if (response.data.orders && Array.isArray(response.data.orders)) {
                  currentPageItems = response.data.orders;
                } else {
                  currentPageItems = [response.data];
                }
              }

              // Log current page data count
              console.log('\nPage Data Summary:', {
                itemsInCurrentPage: currentPageItems.length,
                totalItemsSoFar: totalItemsProcessed + currentPageItems.length,
                currentPage: pageCount,
                maxIterations,
                responseData: response.data
              });

              // Store page data
              const pageData = {
                items: currentPageItems,
                data: response.data, // Keep the original response data for reference
                url: urlObj.toString(),
                headers: response.headers
              };
              pages.push(pageData);
              totalItemsProcessed += currentPageItems.length;

              // After processing each page's items
              if (currentPageItems.length > 0) {
                // Save items to DynamoDB if saveData is true
                await saveItemsToDynamoDB(currentPageItems, {
                  url: urlObj.toString(),
                  status: response.status,
                  headers: response.headers
                });

                // Save child execution log
                await executionLogs.saveChildExecution({
                  pageNumber: pageCount,
                  totalItemsProcessed,
                  itemsInCurrentPage: currentPageItems.length,
                  url: urlObj.toString(),
                  status: response.status,
                  paginationType: detectedPaginationType || 'none',
                  isLast: !hasMorePages || pageCount === maxIterations
                });
              }

              // Check for next page based on detected pagination type
              if (detectedPaginationType === 'link') {
                const nextUrl = extractNextUrl(response.headers.link);
                if (!nextUrl) {
                  hasMorePages = false;
                  console.log('\nNo more pages (Link header):', `Page ${pageCount} is the last page`);
                } else {
                  // For Shopify, we need to handle page_info parameter correctly
                  const nextUrlObj = new URL(nextUrl);
                  // Only remove status parameter, keep limit
                  nextUrlObj.searchParams.delete('status');
                  // Add limit parameter if it's not already present
                  if (!nextUrlObj.searchParams.has('limit') && queryParams.limit) {
                    nextUrlObj.searchParams.append('limit', queryParams.limit);
                  }
                  currentUrl = nextUrlObj.toString();
                  console.log('\nNext page URL:', currentUrl);
                }
              } else if (detectedPaginationType === 'bookmark') {
                const bookmark = extractBookmark(response.data);
                if (!bookmark) {
                  hasMorePages = false;
                  console.log('\nNo more pages (Bookmark):', `Page ${pageCount} is the last page`);
                } else {
                  urlObj.searchParams.set('bookmark', bookmark);
                  currentUrl = urlObj.toString();
                  console.log('\nNext page bookmark:', bookmark);
                }
              } else if (detectedPaginationType === 'cursor') {
                const cursor = extractCursor(response.data);
                if (!cursor) {
                  hasMorePages = false;
                  console.log('\nNo more pages (Cursor):', `Page ${pageCount} is the last page`);
                } else {
                  urlObj.searchParams.set('cursor', cursor);
                  currentUrl = urlObj.toString();
                  console.log('\nNext page cursor:', cursor);
                }
              } else if (detectedPaginationType === 'offset') {
                const totalCount = response.data.total_count || response.data.total;
                const currentOffset = parseInt(urlObj.searchParams.get('offset') || '0');
                const limit = parseInt(urlObj.searchParams.get('limit') || '10');
                
                if (currentOffset + limit >= totalCount) {
                  hasMorePages = false;
                  console.log('\nNo more pages (Offset):', `Page ${pageCount} is the last page`);
                } else {
                  urlObj.searchParams.set('offset', (currentOffset + limit).toString());
                  currentUrl = urlObj.toString();
                  console.log('\nNext page offset:', currentOffset + limit);
                }
              } else {
                hasMorePages = false;
                console.log('\nNo pagination detected:', `Page ${pageCount} is the last page`);
              }

              console.log(`\n=== PAGE ${pageCount} SUMMARY ===`);
              console.log({
                status: response.status,
                hasMorePages,
                totalItemsProcessed,
                currentPageItems: currentPageItems.length,
                nextUrl: currentUrl,
                paginationType: detectedPaginationType,
                responseData: response.data
              });

              pageCount++;
            }

            // Update parent execution status to completed
            await executionLogs.updateParentStatus('completed', true);

            // Log final summary
            console.log('\n=== PAGINATED REQUEST COMPLETED ===');
            console.log({
              totalPages: pageCount - 1,
              totalItems: totalItemsProcessed,
              executionId: execId,
              paginationType: detectedPaginationType || 'none',
              finalUrl: currentUrl,
              lastError: lastError
            });

          } catch (error) {
            console.error('Background processing error:', error);
            if (executionLogs) {
              await executionLogs.updateParentStatus('error', true);
            }
          }
        })();

        return initialResponse;

=======
        const executionId = uuidv4();
        const timestamp = new Date().toISOString();
        let totalItemsProcessed = 0;
        let currentPage = 1;
        let nextUrl = c.request.requestBody.url;
        let isLastPage = false;
        let paginationType = 'none';
        const maxIterations = c.request.requestBody.maxIterations || 10;

        // Create initial execution log entry
        await saveExecutionLog(executionId, null, 0, 0, nextUrl, 'none', false);

        while (currentPage <= maxIterations && !isLastPage) {
          console.log(`[executeNamespacePaginatedRequest] Processing page ${currentPage}`);
          
          try {
            const response = await axios({
              method: c.request.requestBody.method,
              url: nextUrl,
              headers: c.request.requestBody.headers,
              data: c.request.requestBody.body
            });

            // Process the response
            const pageData = response.data;
            paginationType = detectPaginationType(response);
            
            // Update total items processed
            const itemsInCurrentPage = Array.isArray(pageData) ? pageData.length : 1;
            totalItemsProcessed += itemsInCurrentPage;

            // Save execution log for this page
            await saveExecutionLog(
              executionId,
              { data: pageData, items: Array.isArray(pageData) ? pageData : [pageData] },
              currentPage,
              totalItemsProcessed,
              nextUrl,
              paginationType,
              false
            );

            // Get next page URL based on pagination type
            switch (paginationType) {
              case 'link':
                nextUrl = extractNextUrl(response.headers.link);
                break;
              case 'bookmark':
                const bookmark = extractBookmark(pageData);
                if (bookmark) {
                  const url = new URL(nextUrl);
                  url.searchParams.set('page_info', bookmark);
                  nextUrl = url.toString();
                }
                break;
              case 'cursor':
                const cursor = extractCursor(pageData);
                if (cursor) {
                  const url = new URL(nextUrl);
                  url.searchParams.set('cursor', cursor);
                  nextUrl = url.toString();
                }
                break;
              default:
                nextUrl = null;
            }

            // Check if this is the last page
            isLastPage = !nextUrl || currentPage >= maxIterations;
            
            if (isLastPage) {
              // Save final execution log entry
              await saveExecutionLog(
                executionId,
                { data: pageData, items: Array.isArray(pageData) ? pageData : [pageData] },
                currentPage,
                totalItemsProcessed,
                nextUrl,
                paginationType,
                true
              );
            }

            currentPage++;
          } catch (error) {
            console.error(`[executeNamespacePaginatedRequest] Error processing page ${currentPage}:`, error);
            
            // Save error execution log
            await saveExecutionLog(
              executionId,
              { error: error.message },
              currentPage,
              totalItemsProcessed,
              nextUrl,
              paginationType,
              true
            );

            throw error;
          }
        }

        return {
          statusCode: 200,
          body: {
            executionId,
            status: 'completed',
            totalPages: currentPage - 1,
            totalItemsProcessed,
            paginationType
          }
        };
>>>>>>> 8730986f34f81e5d482a592b073431e68bf02769
      } catch (error) {
        console.error('[executeNamespacePaginatedRequest] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to execute paginated request', details: error.message }
        };
      }
    },

    getExecutions: async (c, req, res) => {
      console.log('[getExecutions] Request received');
      
      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'executions'
            },
            requestBody: {
              TableName: 'executions',
              // Get active executions from the last 24 hours
              FilterExpression: "#data.#ts >= :dayAgo AND #data.#status[0] <> :completed",
              ExpressionAttributeNames: {
                "#data": "data",
                "#ts": "timestamp",
                "#status": "status"
              },
              ExpressionAttributeValues: {
                ":dayAgo": new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
                ":completed": "completed"
              }
            }
          }
        });

<<<<<<< HEAD
=======
        console.log('[getExecutions] Response:', {
          statusCode: response.statusCode,
          itemCount: response.body?.items?.length || 0
        });

        if (!response.body || !response.body.items) {
          return {
            statusCode: 200,
            body: []
          };
        }

>>>>>>> 8730986f34f81e5d482a592b073431e68bf02769
        return {
          statusCode: 200,
          body: response.body.items.map(item => ({
            'exec-id': item['exec-id'],
            'child-exec-id': item['child-exec-id'],
            data: item.data
          }))
        };
      } catch (error) {
        console.error('[getExecutions] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get executions', details: error.message }
        };
      }
    },

    // Add this new handler in the handlers object
    getExecutionById: async (c, req, res) => {
      const executionId = c.request.params.executionId;
      console.log('[getExecutionById] Request received for execution:', executionId);
      
      try {
        const response = await dynamodbHandlers.getItems({
          request: {
            params: {
              tableName: 'executions'
            },
            requestBody: {
              TableName: 'executions',
              FilterExpression: "#execId = :execId",
              ExpressionAttributeNames: {
                "#execId": "exec-id"
              },
              ExpressionAttributeValues: {
                ":execId": executionId
              }
            }
          }
        });

        console.log('[getExecutionById] Response:', {
          statusCode: response.statusCode,
          itemCount: response.body?.items?.length || 0
        });

        if (!response.body || !response.body.items || response.body.items.length === 0) {
          return {
            statusCode: 404,
            body: { error: 'Execution not found' }
          };
        }

        // Check if all items in this execution are completed
        const allCompleted = response.body.items.every(item => 
          item.data.status.includes('completed') && item.data['is-last']
        );

        // If all items are completed, return 404 to stop polling
        if (allCompleted) {
          return {
            statusCode: 404,
            body: { error: 'Execution completed' }
          };
        }

        return {
          statusCode: 200,
          body: response.body.items.map(item => ({
            'exec-id': item['exec-id'],
            'child-exec-id': item['child-exec-id'],
            data: item.data
          }))
        };
      } catch (error) {
        console.error('[getExecutionById] Error:', error);
        return {
          statusCode: 500,
          body: { error: 'Failed to get execution', details: error.message }
        };
      }
    }
  }
});

// Initialize AWS DynamoDB OpenAPI backend
const awsApi = new OpenAPIBackend({
  definition: './swagger/aws-dynamodb.yaml',
  quick: true,
  handlers: {
    validationFail: async (c, req, res) => ({
      statusCode: 400,
      error: c.validation.errors
    }),
    notFound: async (c, req, res) => ({
      statusCode: 404,
      error: 'Not Found'
    }),
    // Table Operations
    listTables: dynamodbHandlers.listTables,
    createTable: dynamodbHandlers.createTable,
    deleteTable: dynamodbHandlers.deleteTable,
    // Item Operations
    getItems: dynamodbHandlers.getItems,
    createItem: dynamodbHandlers.createItem,
    getItem: dynamodbHandlers.getItem,
    updateItem: dynamodbHandlers.updateItem,
    deleteItem: dynamodbHandlers.deleteItem,
    queryItems: dynamodbHandlers.queryItems,
    // New PK-only Operations
    getItemsByPk: dynamodbHandlers.getItemsByPk,
    updateItemsByPk: dynamodbHandlers.updateItemsByPk,
    deleteItemsByPk: dynamodbHandlers.deleteItemsByPk,
    getExecutions: dynamodbHandlers.getExecutions
  }
});

// Initialize Pinterest OpenAPI backend
const pinterestApi = new OpenAPIBackend({
  definition: './pinterest-api.yaml',
  quick: true,
  handlers: {
    validationFail: async (c, req, res) => ({
      statusCode: 400,
      error: c.validation.errors
    }),
    notFound: async (c, req, res) => ({
      statusCode: 404,
      error: 'Not Found'
    }),
    // Map the Pinterest handlers
    getPinterestToken: pinterestHandlers.getPinterestToken,
    testPinterestApi: pinterestHandlers.testPinterestApi
  }
});

// Initialize YAML OpenAPI backend


// Initialize AWS Messaging OpenAPI backend
const awsMessagingApi = new OpenAPIBackend({
  definition: './aws-messaging.yaml',
  quick: true,
  handlers: {
    validationFail: async (c, req, res) => ({
      statusCode: 400,
      error: c.validation.errors
    }),
    notFound: async (c, req, res) => ({
      statusCode: 404,
      error: 'Not Found'
    }),
    // SNS Handlers
    listSnsTopics: awsMessagingHandlers.listSnsTopics,
    createSnsTopic: awsMessagingHandlers.createSnsTopic,
    deleteSnsTopic: awsMessagingHandlers.deleteSnsTopic,
    publishToSnsTopic: awsMessagingHandlers.publishToSnsTopic,
    // SQS Handlers
    listSqsQueues: awsMessagingHandlers.listSqsQueues,
    createSqsQueue: awsMessagingHandlers.createSqsQueue,
    deleteSqsQueue: awsMessagingHandlers.deleteSqsQueue,
    sendMessage: awsMessagingHandlers.sendMessage,
    receiveMessages: awsMessagingHandlers.receiveMessages,
    deleteMessage: awsMessagingHandlers.deleteMessage
  }
});

// Initialize all APIs
await Promise.all([
  mainApi.init(),
  awsApi.init(),
  pinterestApi.init(),
  awsMessagingApi.init()
]);

// Helper function to handle requests
const handleRequest = async (handler, req, res) => {
  try {
    const response = await handler(
      { request: { ...req, requestBody: req.body, params: req.params } },
      req,
      res
    );
    res.status(response.statusCode).json(response.body || response);
  } catch (error) {
    console.error('Request handler error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

// Serve Swagger UI for all APIs
const mainOpenapiSpec = yaml.load(fs.readFileSync(path.join(__dirname, 'openapi.yaml'), 'utf8'));
const awsOpenapiSpec = yaml.load(fs.readFileSync(path.join(__dirname, 'swagger/aws-dynamodb.yaml'), 'utf8'));
const pinterestOpenapiSpec = yaml.load(fs.readFileSync(path.join(__dirname, 'pinterest-api.yaml'), 'utf8'));

// Configure route handlers for API documentation
app.get('/api-docs/swagger.json', (req, res) => {
  res.json(mainOpenapiSpec);
});

app.get('/pinterest-api-docs/swagger.json', (req, res) => {
  res.json(pinterestOpenapiSpec);
});

// Serve main API docs
app.use('/api-docs', swaggerUi.serve);
app.get('/api-docs', (req, res) => {
  res.send(
    swaggerUi.generateHTML(mainOpenapiSpec, {
      customSiteTitle: "Main API Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/api-docs/swagger.json"
    })
  );
});

// Serve AWS API docs
app.use('/aws-api-docs', swaggerUi.serve);
app.get('/aws-api-docs', (req, res) => {
  res.send(
    swaggerUi.generateHTML(awsOpenapiSpec, {
      customSiteTitle: "AWS DynamoDB API Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/aws-api-docs/swagger.json"
    })
  );
});

// Serve AWS API docs at the DynamoDB base URL
app.use('/api/dynamodb', swaggerUi.serve);
app.get('/api/dynamodb', (req, res) => {
  res.send(
    swaggerUi.generateHTML(awsOpenapiSpec, {
      customSiteTitle: "AWS DynamoDB API Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/api/dynamodb/swagger.json"
    })
  );
});

// Serve DynamoDB OpenAPI specification
app.get('/api/dynamodb/swagger.json', (req, res) => {
  res.json(awsOpenapiSpec);
});

// Serve Pinterest API docs
app.use('/pinterest-api-docs', swaggerUi.serve);
app.get('/pinterest-api-docs', (req, res) => {
  res.send(
    swaggerUi.generateHTML(pinterestOpenapiSpec, {
      customSiteTitle: "Pinterest API Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/pinterest-api-docs/swagger.json"
    })
  );
});

// Serve YAML service API docs
app.use('/yaml-service-docs', swaggerUi.serve);
app.get('/yaml-service-docs', (req, res) => {
  res.send(
    swaggerUi.generateHTML(yamlOpenapiSpec, {
      customSiteTitle: "YAML Service API Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/yaml-service-docs/swagger.json"
    })
  );
});

// Serve YAML service OpenAPI specification


// Handle AWS DynamoDB routes
app.all('/api/dynamodb/*', async (req, res, next) => {
  // Skip if this is a documentation request
  if (req.method === 'GET' && req.path === '/api/dynamodb') {
    return next();
  }

  try {
    // console.log('[DynamoDB Request]:', {
    //   method: req.method,
    //   path: req.path,
    //   body: req.body
    // });

    // Adjust the path to remove the /api/dynamodb prefix
    const adjustedPath = req.path.replace('/api/dynamodb', '');
    
    const response = await awsApi.handleRequest(
      {
        method: req.method,
        path: adjustedPath || '/',
        body: req.body,
        query: req.query,
        headers: req.headers
      },
      req,
      res
    );

    if (!response || !response.body) {
      console.error('[DynamoDB Response] Invalid response:', response);
      return res.status(500).json({
        error: 'Invalid response from handler'
      });
    }

    console.log('[DynamoDB Response]:', {
      statusCode: response.statusCode,
      body: response.body
    });

    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('[DynamoDB Error]:', error);
    res.status(500).json({
      error: 'Failed to handle DynamoDB request',
      message: error.message
    });
  }
});

// Handle main API routes
app.all('/api/*', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: req.method,
        path: req.path.replace('/api', '') || '/',  // Remove /api prefix
        body: req.body,
        query: req.query,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Main API request error:', error);
    res.status(500).json({ error: 'Failed to handle main API request' });
  }
});

// Add direct route for namespaces
app.get('/namespaces', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'GET',
        path: '/namespaces',
        query: req.query,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Namespace request error:', error);
    res.status(500).json({ error: 'Failed to handle namespace request' });
  }
});

// Add GET route for individual namespace
app.get('/namespaces/:namespaceId', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'GET',
        path: `/namespaces/${req.params.namespaceId}`,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Get namespace by ID error:', error);
    res.status(500).json({ error: 'Failed to get namespace' });
  }
});

// Add PUT route for updating namespace
app.put('/namespaces/:namespaceId', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'PUT',
        path: `/namespaces/${req.params.namespaceId}`,
        body: req.body,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Update namespace error:', error);
    res.status(500).json({ error: 'Failed to update namespace' });
  }
});

// Add GET route for all accounts
app.get('/accounts', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'GET',
        path: '/accounts',
        query: req.query,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Get all accounts error:', error);
    res.status(500).json({ error: 'Failed to get all accounts' });
  }
});

// Add GET route for all methods
app.get('/methods', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'GET',
        path: '/methods',
        query: req.query,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Get all methods error:', error);
    res.status(500).json({ error: 'Failed to get all methods' });
  }
});

// Add POST route for namespaces
app.post('/namespaces', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'POST',
        path: '/namespaces',
        body: req.body,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Create namespace error:', error);
    res.status(500).json({ error: 'Failed to create namespace' });
  }
});

// Add route for namespace accounts
app.post('/namespaces/:namespaceId/accounts', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'POST',
        path: `/namespaces/${req.params.namespaceId}/accounts`,
        body: req.body,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Create namespace account error:', error);
    res.status(500).json({ error: 'Failed to create namespace account' });
  }
});

// Add GET route for namespace accounts
app.get('/namespaces/:namespaceId/accounts', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'GET',
        path: `/namespaces/${req.params.namespaceId}/accounts`,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Get namespace accounts error:', error);
    res.status(500).json({ error: 'Failed to get namespace accounts' });
  }
});

// Add route for namespace methods
app.post('/namespaces/:namespaceId/methods', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'POST',
        path: `/namespaces/${req.params.namespaceId}/methods`,
        body: req.body,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Create namespace method error:', error);
    res.status(500).json({ error: 'Failed to create namespace method' });
  }
});

// Add GET route for namespace methods
app.get('/namespaces/:namespaceId/methods', async (req, res) => {
  try {
    console.log('[GET Namespace Methods] Request:', {
      namespaceId: req.params.namespaceId,
      path: `/namespaces/${req.params.namespaceId}/methods`
    });

    const response = await mainApi.handleRequest(
      {
        method: 'GET',
        path: `/namespaces/${req.params.namespaceId}/methods`,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );

    console.log('[GET Namespace Methods] Response:', {
      statusCode: response.statusCode,
      bodyLength: response.body?.length || 0
    });

    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Get namespace methods error:', error);
    res.status(500).json({ error: 'Failed to get namespace methods' });
  }
});

// Add GET route for individual account
app.get('/accounts/:accountId', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'GET',
        path: `/accounts/${req.params.accountId}`,
        params: req.params,
        headers: req.headers,
        query: req.query
      },
      req,
      res
    );

    if (!response) {
      console.error('No response from handler');
      return res.status(500).json({ error: 'Internal server error' });
    }

    console.log('[GET Account] Response:', {
      statusCode: response.statusCode,
      body: response.body
    });

    return res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Get account by ID error:', error);
    return res.status(500).json({ error: 'Failed to get account' });
  }
});

// Add GET route for individual method
app.get('/methods/:methodId', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'GET',
        path: `/methods/${req.params.methodId}`,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Get method by ID error:', error);
    res.status(500).json({ error: 'Failed to get method' });
  }
});

// Handle Pinterest routes
app.all('/api/pinterest/*', async (req, res) => {
  try {
    const response = await pinterestApi.handleRequest(
      {
        method: req.method,
        path: req.path,
        body: req.body,
        query: req.query,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('[Pinterest Proxy] Error:', error.message);
    res.status(500).json({
      error: 'Failed to handle Pinterest request',
      message: error.message
    });
  }
});

// Add DELETE route for accounts
app.delete('/accounts/:accountId', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'DELETE',
        path: `/accounts/${req.params.accountId}`,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Delete account error:', error);
    res.status(500).json({ error: 'Failed to delete account' });
  }
});

// Add PUT route for accounts
app.put('/accounts/:accountId', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'PUT',
        path: `/accounts/${req.params.accountId}`,
        params: req.params,
        body: req.body,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Update account error:', error);
    res.status(500).json({ error: 'Failed to update account' });
  }
});

// Add PUT route for methods
app.put('/methods/:methodId', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'PUT',
        path: `/methods/${req.params.methodId}`,
        params: req.params,
        body: req.body,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Update method error:', error);
    res.status(500).json({ error: 'Failed to update method' });
  }
});

// Add DELETE route for methods
app.delete('/methods/:methodId', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'DELETE',
        path: `/methods/${req.params.methodId}`,
        params: req.params,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Delete method error:', error);
    res.status(500).json({ error: 'Failed to delete method' });
  }
});

// Add direct route for paginated execution
app.post('/execute/paginated', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'POST',
        path: '/execute/paginated',
        body: req.body,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Paginated execution error:', error);
    res.status(500).json({ error: 'Failed to execute paginated request' });
  }
});

// Add direct route for execute
app.post('/execute', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'POST',
        path: '/execute',
        body: req.body,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Execute request error:', error);
    res.status(500).json({ error: 'Failed to execute request' });
  }
});

// Load AWS Messaging OpenAPI specification
const awsMessagingOpenapiSpec = yaml.load(fs.readFileSync(path.join(__dirname, 'aws-messaging.yaml'), 'utf8'));

// Serve AWS Messaging API docs
app.use('/aws-messaging-docs', swaggerUi.serve);
app.get('/aws-messaging-docs', (req, res) => {
  res.send(
    swaggerUi.generateHTML(awsMessagingOpenapiSpec, {
      customSiteTitle: "AWS Messaging Service Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/aws-messaging-docs/swagger.json"
    })
  );
});

// Serve AWS Messaging OpenAPI specification
app.get('/aws-messaging-docs/swagger.json', (req, res) => {
  res.json(awsMessagingOpenapiSpec);
});

// Handle AWS Messaging routes
app.all('/api/aws-messaging/*', async (req, res) => {
  try {
    // Remove the /api/aws-messaging prefix from the path
    const adjustedPath = req.path.replace('/api/aws-messaging', '');
    
    const response = await awsMessagingApi.handleRequest(
      {
        method: req.method,
        path: adjustedPath || '/',
        body: req.body,
        query: req.query,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('[AWS Messaging Service] Error:', error.message);
    res.status(500).json({
      error: 'Failed to handle AWS messaging service request',
      message: error.message
    });
  }
});

// Add this route after the existing routes
app.get('/api/dynamodb/tables/executions/items/:executionId', async (req, res) => {
  const result = await handlers.getExecutionById({
    request: { params: { executionId: req.params.executionId } }
  }, req, res);
  res.status(result.statusCode).json(result.body);
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on port ${PORT}`);
  console.log(`Main API documentation available at http://localhost:${PORT}/api-docs`);
  console.log(`Pinterest API documentation available at http://localhost:${PORT}/pinterest-api-docs`);
  console.log(`AWS DynamoDB service available at http://localhost:${PORT}/api/dynamodb`);
  console.log(`AWS Messaging Service documentation available at http://localhost:${PORT}/aws-messaging-docs`);
});

