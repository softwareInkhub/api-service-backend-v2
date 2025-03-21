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

     executeNamespacePaginatedRequest: async (c, req, res) => {
      console.log('[Paginated] Starting request:', {
        method: c.request.requestBody.method,
        url: c.request.requestBody.url,
        maxIterations: c.request.requestBody.maxIterations || 10
      });

      const { 
        method, 
        url, 
        maxIterations = 10,
        queryParams = {}, 
        headers = {}, 
        body = null
      } = c.request.requestBody;

      try {
        const executionId = uuidv4();
        let currentUrl = url;
        const aggregatedData = [];
        let pageCount = 1;
        let hasMorePages = true;
        let detectedPaginationType = null;

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
          // Build URL with query parameters
          const urlObj = new URL(currentUrl);
          Object.entries(queryParams).forEach(([key, value]) => {
            if (value) urlObj.searchParams.append(key, value);
          });

          // Make request
          console.log(`[Paginated] Making request ${pageCount}:`, urlObj.toString());
          const response = await axios({
            method: method.toUpperCase(),
            url: urlObj.toString(),
            headers: headers,
            data: !['GET', 'HEAD'].includes(method.toUpperCase()) ? body : undefined,
            validateStatus: () => true
          });

          // Detect pagination type on first request if not specified
          if (pageCount === 1) {
            detectedPaginationType = detectPaginationType(response);
            console.log(`[Paginated] Detected pagination type: ${detectedPaginationType}`);
          }

          // Handle authentication errors
          if (response.status === 401 || response.status === 403) {
            return {
              statusCode: response.status,
              body: {
                error: 'Authentication Failed',
                status: response.status,
                statusText: response.statusText,
                details: response.data
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

          // Add response data to aggregated results
          if (response.data) {
            // Handle different response structures
            if (Array.isArray(response.data)) {
              aggregatedData.push(...response.data);
            } else if (response.data.data && Array.isArray(response.data.data)) {
              aggregatedData.push(...response.data.data);
            } else if (response.data.items && Array.isArray(response.data.items)) {
              aggregatedData.push(...response.data.items);
            } else {
              aggregatedData.push(response.data);
            }
          }

          // Check for next page based on detected pagination type
          if (detectedPaginationType === 'link') {
            const nextUrl = extractNextUrl(response.headers.link);
            if (!nextUrl) {
              hasMorePages = false;
            } else {
              currentUrl = nextUrl;
            }
          } else if (detectedPaginationType === 'bookmark') {
            const bookmark = extractBookmark(response.data);
            if (!bookmark) {
              hasMorePages = false;
            } else {
              urlObj.searchParams.set('bookmark', bookmark);
              currentUrl = urlObj.toString();
            }
          } else if (detectedPaginationType === 'cursor') {
            const cursor = extractCursor(response.data);
            if (!cursor) {
              hasMorePages = false;
            } else {
              urlObj.searchParams.set('cursor', cursor);
              currentUrl = urlObj.toString();
            }
          } else if (detectedPaginationType === 'offset') {
            // For offset pagination, check if we've reached the total count
            const totalCount = response.data.total_count || response.data.total;
            const currentOffset = parseInt(urlObj.searchParams.get('offset') || '0');
            const limit = parseInt(urlObj.searchParams.get('limit') || '10');
            
            if (currentOffset + limit >= totalCount) {
              hasMorePages = false;
            } else {
              urlObj.searchParams.set('offset', (currentOffset + limit).toString());
              currentUrl = urlObj.toString();
            }
          } else {
            // If no pagination type detected, assume single page
            hasMorePages = false;
          }

          console.log(`[Paginated] Page ${pageCount} complete:`, {
            status: response.status,
            hasMorePages,
            itemCount: aggregatedData.length,
            nextUrl: currentUrl,
            paginationType: detectedPaginationType
          });

          pageCount++;
        }

        return {
          statusCode: 200,
          body: {
            status: 200,
            metadata: {
              totalPages: pageCount - 1,
              totalItems: aggregatedData.length,
              executionId,
              paginationType: detectedPaginationType || 'none'
            },
            data: aggregatedData
          }
        };

      } catch (error) {
        console.error('[Paginated] Request failed:', {
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
            error: 'Failed to execute paginated request',
            details: error.message,
            code: error.code
          }
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
    deleteItemsByPk: dynamodbHandlers.deleteItemsByPk
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

// Initialize all APIs
await Promise.all([
  mainApi.init(),
  awsApi.init(),
  pinterestApi.init()
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

// Handle AWS DynamoDB routes
app.all('/api/dynamodb/*', async (req, res, next) => {
  // Skip if this is a documentation request
  if (req.method === 'GET' && req.path === '/api/dynamodb') {
    return next();
  }

  try {
    console.log('[DynamoDB Request]:', {
      method: req.method,
      path: req.path,
      body: req.body
    });

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

const PORT = process.env.PORT || 5000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on port ${PORT}`);
  console.log(`Main API documentation available at http://localhost:${PORT}/api-docs`);
  console.log(`Pinterest API documentation available at http://localhost:${PORT}/pinterest-api-docs`);
  console.log(`AWS DynamoDB service available at http://localhost:${PORT}/api/dynamodb`);
});

