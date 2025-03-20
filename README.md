# AWS DynamoDB API Documentation

This document provides detailed information about the available DynamoDB operations and their usage.

## Base URL
```
http://localhost:5000/api/dynamodb
```

## Table Operations

### 1. List Tables
Lists all DynamoDB tables.

```http
GET /tables
```

**Response**
```json
{
  "tables": ["table1", "table2"],
  "count": 2
}
```

### 2. Create Table
Creates a new DynamoDB table.

```http
POST /tables
```

**Request Body**
```json
{
  "TableName": "test",
  "KeySchema": [
    { "AttributeName": "id", "KeyType": "HASH" }
  ],
  "AttributeDefinitions": [
    { "AttributeName": "id", "AttributeType": "S" }
  ],
  "BillingMode": "PROVISIONED",
  "ProvisionedThroughput": {
    "ReadCapacityUnits": 5,
    "WriteCapacityUnits": 5
  }
}
```

**Response**
```json
{
  "message": "Table created successfully",
  "table": {
    "TableName": "test",
    "TableStatus": "CREATING",
    ...
  }
}
```

### 3. Delete Table
Deletes a DynamoDB table.

```http
DELETE /tables/{tableName}
```

**Response**
```json
{
  "message": "Table deleted successfully"
}
```

## Item Operations

### 1. Get All Items
Retrieves all items from a table.

```http
GET /tables/{tableName}/items
```

**Response**
```json
{
  "items": [
    {
      "id": "123",
      "test": "test value",
      "data": { ... }
    }
  ],
  "count": 1
}
```

### 2. Get Item by ID
Retrieves a specific item using its ID.

```http
GET /tables/{tableName}/items/{id}
```

**Response**
```json
{
  "id": "123",
  "test": "test value",
  "data": { ... }
}
```

### 3. Create Item
Creates a new item in the table.

```http
POST /tables/{tableName}/items
```

**Request Body**
```json
{
  "id": "123",
  "test": "test value",
  "data": {
    "field1": "value1",
    "field2": "value2"
  }
}
```

**Response**
```json
{
  "message": "Item created successfully",
  "item": {
    "id": "123",
    "test": "test value",
    "data": {
      "field1": "value1",
      "field2": "value2"
    }
  }
}
```

### 4. Update Item
Updates an existing item.

```http
PUT /tables/{tableName}/items/{id}
```

**Request Body**
```json
{
  "UpdateExpression": "set #test = :value",
  "ExpressionAttributeNames": {
    "#test": "test"
  },
  "ExpressionAttributeValues": {
    ":value": "new test value"
  }
}
```

**Response**
```json
{
  "id": "123",
  "test": "new test value",
  "data": {
    "field1": "value1",
    "field2": "value2"
  }
}
```

### 5. Delete Item
Deletes an item from the table.

```http
DELETE /tables/{tableName}/items/{id}
```

**Response**
```
204 No Content
```

### 6. Query Items
Queries items using key conditions.

```http
POST /tables/{tableName}/query
```

**Request Body**
```json
{
  "KeyConditionExpression": "#id = :idValue",
  "ExpressionAttributeNames": {
    "#id": "id"
  },
  "ExpressionAttributeValues": {
    ":idValue": "123"
  }
}
```

**Response**
```json
{
  "items": [
    {
      "id": "123",
      "test": "test value",
      "data": { ... }
    }
  ],
  "count": 1
}
```

## Error Handling

All endpoints return appropriate HTTP status codes:

- `200`: Success
- `201`: Created successfully
- `204`: Deleted successfully
- `400`: Bad request (invalid parameters)
- `404`: Resource not found
- `500`: Server error

Error Response Format:
```json
{
  "error": "Error message",
  "details": "Detailed error information"
}
```

## Common Use Cases

### 1. Creating a New Item
```bash
curl -X POST http://localhost:5000/api/dynamodb/tables/test/items \
  -H "Content-Type: application/json" \
  -d '{
    "id": "123",
    "test": "test value",
    "data": {
      "field1": "value1",
      "field2": "value2"
    }
  }'
```

### 2. Updating an Item
```bash
curl -X PUT http://localhost:5000/api/dynamodb/tables/test/items/123 \
  -H "Content-Type: application/json" \
  -d '{
    "UpdateExpression": "set #test = :value",
    "ExpressionAttributeNames": {
      "#test": "test"
    },
    "ExpressionAttributeValues": {
      ":value": "new test value"
    }
  }'
```

### 3. Querying Items
```bash
curl -X POST http://localhost:5000/api/dynamodb/tables/test/query \
  -H "Content-Type: application/json" \
  -d '{
    "KeyConditionExpression": "#id = :idValue",
    "ExpressionAttributeNames": {
      "#id": "id"
    },
    "ExpressionAttributeValues": {
      ":idValue": "123"
    }
  }'
```

## Development Setup

1. Start local DynamoDB:
```bash
docker run -d -p 8000:8000 amazon/dynamodb-local
```

2. Set environment variables in `.env`:
```env
NODE_ENV=development
AWS_REGION=us-east-1
```

3. Create a test table:
```bash
curl -X POST http://localhost:5000/api/dynamodb/tables \
  -H "Content-Type: application/json" \
  -d '{
    "TableName": "test",
    "KeySchema": [
      { "AttributeName": "id", "KeyType": "HASH" }
    ],
    "AttributeDefinitions": [
      { "AttributeName": "id", "AttributeType": "S" }
    ],
    "BillingMode": "PROVISIONED",
    "ProvisionedThroughput": {
      "ReadCapacityUnits": 5,
      "WriteCapacityUnits": 5
    }
  }'
```
## Data Model

### Basic Item Structure
- id: Primary key (String)
```json
{
  "id": "123",
  "test": "test value",
  "data": {
    "field1": "value1",
    "field2": "value2"
  }
}
```

## Namespace API Documentation

This section describes the operations available for managing namespaces, accounts, and methods using DynamoDB.

### Base URL
```
http://localhost:5000/api/dynamodb
```

### Namespace Operations

#### 1. Create Namespace
Creates a new namespace.

```http
POST /tables/brmh-namespace/items
```

**Request Body**
```json
{
  "id": "namespace#123",
  "type": "namespace",
  "data": {
    "namespace-id": "123",
    "namespace-name": "shopify",
    "namespace-url": "https://api.shopify.com/v1",
    "tags": ["ecommerce", "retail"]
  }
}
```

**Response**
```json
{
  "message": "Item created successfully",
  "item": {
    "id": "namespace#123",
    "type": "namespace",
    "data": {
      "namespace-id": "123",
      "namespace-name": "shopify",
      "namespace-url": "https://api.shopify.com/v1",
      "tags": ["ecommerce", "retail"]
    }
  }
}
```

#### 2. Get All Namespaces
Retrieves all namespaces.

```http
POST /tables/brmh-namespace/query
```

**Request Body**
```json
{
  "KeyConditionExpression": "begins_with(id, :prefix)",
  "FilterExpression": "#type = :itemType",
  "ExpressionAttributeNames": {
    "#type": "type"
  },
  "ExpressionAttributeValues": {
    ":prefix": "namespace#",
    ":itemType": "namespace"
  }
}
```

#### 3. Get Namespace by ID
Retrieves a specific namespace.

```http
GET /tables/brmh-namespace/items/namespace#{namespaceId}
```

#### 4. Update Namespace
Updates an existing namespace.

```http
PUT /tables/brmh-namespace/items/namespace#{namespaceId}
```

**Request Body**
```json
{
  "UpdateExpression": "set #data = :value",
  "ExpressionAttributeNames": {
    "#data": "data"
  },
  "ExpressionAttributeValues": {
    ":value": {
      "namespace-id": "123",
      "namespace-name": "updated-shopify",
      "namespace-url": "https://api.shopify.com/v2",
      "tags": ["updated", "ecommerce"]
    }
  }
}
```

#### 5. Delete Namespace
Deletes a namespace.

```http
DELETE /tables/brmh-namespace/items/namespace#{namespaceId}
```

### Namespace Account Operations

#### 1. Create Account
Creates a new account in a namespace.

```http
POST /tables/brmh-namespace/items
```

**Request Body**
```json
{
  "id": "namespace#123#account#456",
  "type": "account",
  "data": {
    "namespace-id": "123",
    "namespace-account-id": "456",
    "namespace-account-name": "my-store",
    "namespace-account-url-override": "https://my-store.myshopify.com",
    "namespace-account-header": [
      {
        "key": "X-Shopify-Access-Token",
        "value": "your-access-token"
      }
    ],
    "tags": ["production"]
  }
}
```

#### 2. Get All Accounts in Namespace
Retrieves all accounts in a namespace.

```http
POST /tables/brmh-namespace/query
```

**Request Body**
```json
{
  "KeyConditionExpression": "begins_with(id, :prefix)",
  "FilterExpression": "#type = :itemType",
  "ExpressionAttributeNames": {
    "#type": "type"
  },
  "ExpressionAttributeValues": {
    ":prefix": "namespace#123#account#",
    ":itemType": "account"
  }
}
```

#### 3. Update Account
Updates an existing account.

```http
PUT /tables/brmh-namespace/items/namespace#{namespaceId}#account#{accountId}
```

**Request Body**
```json
{
  "UpdateExpression": "set #data = :value",
  "ExpressionAttributeNames": {
    "#data": "data"
  },
  "ExpressionAttributeValues": {
    ":value": {
      "namespace-id": "123",
      "namespace-account-id": "456",
      "namespace-account-name": "updated-store",
      "namespace-account-url-override": "https://updated-store.myshopify.com",
      "namespace-account-header": [
        {
          "key": "X-Shopify-Access-Token",
          "value": "updated-token"
        }
      ],
      "tags": ["staging"]
    }
  }
}
```

#### 4. Delete Account
Deletes an account from a namespace.

```http
DELETE /tables/brmh-namespace/items/namespace#{namespaceId}#account#{accountId}
```

### Namespace Method Operations

#### 1. Create Method
Creates a new method in a namespace.

```http
POST /tables/brmh-namespace/items
```

**Request Body**
```json
{
  "id": "namespace#123#method#789",
  "type": "method",
  "data": {
    "namespace-id": "123",
    "method-id": "789",
    "namespace-account-method-name": "getProducts",
    "namespace-account-method-type": "GET",
    "namespace-account-method-url-override": "/admin/api/products.json",
    "namespace-account-method-queryParams": [
      {
        "key": "limit",
        "value": "250"
      }
    ],
    "namespace-account-method-header": [
      {
        "key": "Content-Type",
        "value": "application/json"
      }
    ],
    "save-data": true,
    "isInitialized": true,
    "tags": ["products", "inventory"],
    "sample-request": {},
    "sample-response": {
      "products": []
    },
    "request-schema": {},
    "response-schema": {}
  }
}
```

#### 2. Get All Methods in Namespace
Retrieves all methods in a namespace.

```http
POST /tables/brmh-namespace/query
```

**Request Body**
```json
{
  "KeyConditionExpression": "begins_with(id, :prefix)",
  "FilterExpression": "#type = :itemType",
  "ExpressionAttributeNames": {
    "#type": "type"
  },
  "ExpressionAttributeValues": {
    ":prefix": "namespace#123#method#",
    ":itemType": "method"
  }
}
```

#### 3. Update Method
Updates an existing method.

```http
PUT /tables/brmh-namespace/items/namespace#{namespaceId}#method#{methodId}
```

**Request Body**
```json
{
  "UpdateExpression": "set #data = :value",
  "ExpressionAttributeNames": {
    "#data": "data"
  },
  "ExpressionAttributeValues": {
    ":value": {
      "namespace-id": "123",
      "method-id": "789",
      "namespace-account-method-name": "getProducts",
      "namespace-account-method-type": "GET",
      "namespace-account-method-url-override": "/admin/api/2024-01/products.json",
      "namespace-account-method-queryParams": [
        {
          "key": "limit",
          "value": "250"
        }
      ],
      "namespace-account-method-header": [
        {
          "key": "Content-Type",
          "value": "application/json"
        }
      ],
      "save-data": true,
      "isInitialized": true,
      "tags": ["products", "updated"],
      "sample-request": {},
      "sample-response": {
        "products": []
      },
      "request-schema": {},
      "response-schema": {}
    }
  }
}
```

#### 4. Delete Method
Deletes a method from a namespace.

```http
DELETE /tables/brmh-namespace/items/namespace#{namespaceId}#method#{methodId}
```

### Namespace Data Models

#### 1. Namespace Item Structure
```json
{
  "id": "namespace#123",
  "type": "namespace",
  "data": {
    "namespace-id": "123",
    "namespace-name": "shopify",
    "namespace-url": "https://api.shopify.com/v1",
    "tags": ["ecommerce"]
  }
}
```

#### 2. Account Item Structure
```json
{
  "id": "namespace#123#account#456",
  "type": "account",
  "data": {
    "namespace-id": "123",
    "namespace-account-id": "456",
    "namespace-account-name": "my-store",
    "namespace-account-url-override": "https://my-store.myshopify.com",
    "namespace-account-header": [
      {
        "key": "X-Shopify-Access-Token",
        "value": "token"
      }
    ],
    "tags": ["production"]
  }
}
```

#### 3. Method Item Structure
```json
{
  "id": "namespace#123#method#789",
  "type": "method",
  "data": {
    "namespace-id": "123",
    "method-id": "789",
    "namespace-account-method-name": "getProducts",
    "namespace-account-method-type": "GET",
    "namespace-account-method-url-override": "/admin/api/products.json",
    "namespace-account-method-queryParams": [],
    "namespace-account-method-header": [],
    "save-data": false,
    "isInitialized": false,
    "tags": ["products"]
  }
}
```

### Namespace Setup

1. Create the namespace table:
```bash
curl -X POST http://localhost:5000/api/dynamodb/tables \
  -H "Content-Type: application/json" \
  -d '{
    "TableName": "brmh-namespace",
    "KeySchema": [
      { "AttributeName": "id", "KeyType": "HASH" }
    ],
    "AttributeDefinitions": [
      { "AttributeName": "id", "AttributeType": "S" }
    ],
    "BillingMode": "PAY_PER_REQUEST"
  }'
```
