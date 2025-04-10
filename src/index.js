/**
 * Transparent Proxy Worker for Cloudflare R2 Storage
 * 
 * This worker handles S3-compatible API requests, implements read/write operations,
 * performs access control, and implements lazy loading of data from source storage.
 */

// Constants for configuration
const CONFIG = {
  // Default cache control settings
  DEFAULT_CACHE_CONTROL: 'public, max-age=86400',
  // Hostname patterns for identifying read vs write operations
  READ_HOSTNAME_PATTERN: /^read-/,
  WRITE_HOSTNAME_PATTERN: /^write-/,
  // Source storage settings (for lazy loading)
  SOURCE_STORAGE: {
    // Will be loaded from KV
    endpoint: '',
    region: '',
    accessKeyId: '',
    secretAccessKey: '',
    bucketName: ''
  }
};

// Error handling utility
class StorageError extends Error {
  constructor(message, status = 500, code = 'InternalError') {
    super(message);
    this.name = 'StorageError';
    this.status = status;
    this.code = code;
  }
}

/**
 * Main entry point for the worker
 */
export default {
  /**
   * Bindings defined in wrangler.toml are passed as env
   */
  async fetch(request, env, ctx) {
    // Initialize configuration from KV if needed
    await initializeConfig(env);

    try {
      // Parse request
      const url = new URL(request.url);
      const method = request.method;
      const hostname = url.hostname;
      
      // Determine if this is a read or write operation based on hostname
      const isWriteOperation = CONFIG.WRITE_HOSTNAME_PATTERN.test(hostname);
      const isReadOperation = CONFIG.READ_HOSTNAME_PATTERN.test(hostname) || !isWriteOperation;
      
      // Extract bucket name and object key from the path
      const pathParts = url.pathname.split('/').filter(p => p);
      const objectKey = pathParts.join('/');
      
      // Validate authentication and authorization
      await validateAuth(request, env, isWriteOperation, objectKey);
      
      // Route the request based on the HTTP method
      if (method === 'GET' || method === 'HEAD') {
        return await handleGetRequest(request, env, ctx, objectKey);
      } else if (method === 'PUT') {
        if (!isWriteOperation) {
          throw new StorageError('Write operations not allowed on this endpoint', 403, 'AccessDenied');
        }
        return await handlePutRequest(request, env, ctx, objectKey);
      } else if (method === 'DELETE') {
        if (!isWriteOperation) {
          throw new StorageError('Delete operations not allowed on this endpoint', 403, 'AccessDenied');
        }
        return await handleDeleteRequest(request, env, ctx, objectKey);
      } else if (method === 'OPTIONS') {
        return handleOptionsRequest(request);
      } else {
        throw new StorageError(`Method ${method} not supported`, 405, 'MethodNotAllowed');
      }
    } catch (error) {
      return formatErrorResponse(error);
    }
  }
};

/**
 * Initialize configuration from KV store
 */
async function initializeConfig(env) {
  try {
    const sourceConfig = await env.CONFIG_KV.get('source_storage_config', { type: 'json' });
    if (sourceConfig) {
      CONFIG.SOURCE_STORAGE = { ...CONFIG.SOURCE_STORAGE, ...sourceConfig };
    }
    
    // Load other configurations as needed
    const readPattern = await env.CONFIG_KV.get('read_hostname_pattern');
    if (readPattern) {
      CONFIG.READ_HOSTNAME_PATTERN = new RegExp(readPattern);
    }
    
    const writePattern = await env.CONFIG_KV.get('write_hostname_pattern');
    if (writePattern) {
      CONFIG.WRITE_HOSTNAME_PATTERN = new RegExp(writePattern);
    }
  } catch (error) {
    console.error('Failed to load configuration:', error);
    // Continue with default configuration
  }
}

/**
 * Validate authentication and authorization
 */
async function validateAuth(request, env, isWriteOperation, objectKey) {
  // For simplicity, assuming public read access, authenticated write access
  if (isWriteOperation) {
    // Implement S3 signature validation or other auth mechanisms
    const authHeader = request.headers.get('Authorization');
    if (!authHeader) {
      throw new StorageError('Authentication required', 401, 'AccessDenied');
    }
    
    // TODO: Implement proper S3 signature validation
    // For now, we'll just check if the Authorization header exists
    
    // Check authorization from KV or other sources
    // This is a placeholder for actual authorization logic
  }
  
  // Additional validation can be implemented here
  return true;
}

/**
 * Handle GET/HEAD requests
 */
async function handleGetRequest(request, env, ctx, objectKey) {
  if (!objectKey) {
    // Handle bucket listing (not implemented in this example)
    throw new StorageError('Bucket listing not implemented', 501, 'NotImplemented');
  }
  
  // Try to get the object from R2
  let object = await env.R2_BUCKET.get(objectKey);
  
  // If not found in R2, try to get it from source storage (lazy loading)
  if (!object && CONFIG.SOURCE_STORAGE.endpoint) {
    object = await fetchFromSourceStorage(env, ctx, objectKey);
    
    // If found in source storage, store in R2 for future requests
    if (object) {
      await storeObjectInR2(env, object, objectKey);
      
      // Queue a task to update metadata or confirm synchronization
      await queueSyncTask(env, objectKey, 'LAZY_LOADED');
    }
  }
  
  // If not found in any storage, return 404
  if (!object) {
    throw new StorageError('Object not found', 404, 'NoSuchKey');
  }
  
  // For HEAD requests, return just the headers
  if (request.method === 'HEAD') {
    return new Response(null, {
      headers: getResponseHeaders(object),
    });
  }
  
  // For GET requests, return the object with headers
  return new Response(object.body, {
    headers: getResponseHeaders(object),
  });
}

/**
 * Handle PUT requests
 */
async function handlePutRequest(request, env, ctx, objectKey) {
  if (!objectKey) {
    throw new StorageError('Invalid object key', 400, 'InvalidKey');
  }
  
  // Extract relevant headers
  const contentType = request.headers.get('Content-Type') || 'application/octet-stream';
  const contentLength = request.headers.get('Content-Length');
  const cacheControl = request.headers.get('Cache-Control') || CONFIG.DEFAULT_CACHE_CONTROL;
  
  // Custom metadata
  const metadata = {};
  for (const [key, value] of request.headers.entries()) {
    if (key.toLowerCase().startsWith('x-amz-meta-')) {
      metadata[key.substring(11)] = value;
    }
  }
  
  // Upload to R2
  try {
    const object = await env.R2_BUCKET.put(objectKey, request.body, {
      httpMetadata: {
        contentType,
        cacheControl,
      },
      customMetadata: metadata,
    });
    
    // Queue a task to sync with other storages if needed
    await queueSyncTask(env, objectKey, 'UPLOADED');
    
    // Return success response
    return new Response(null, {
      status: 200,
      headers: {
        'ETag': object.httpEtag,
        'Content-Type': 'application/xml',
      },
    });
  } catch (error) {
    console.error('Upload failed:', error);
    throw new StorageError('Upload failed', 500, 'InternalError');
  }
}

/**
 * Handle DELETE requests
 */
async function handleDeleteRequest(request, env, ctx, objectKey) {
  if (!objectKey) {
    throw new StorageError('Invalid object key', 400, 'InvalidKey');
  }
  
  // Delete from R2
  try {
    await env.R2_BUCKET.delete(objectKey);
    
    // Queue a task to sync deletion with other storages if needed
    await queueSyncTask(env, objectKey, 'DELETED');
    
    // Return success response
    return new Response(null, {
      status: 204,
    });
  } catch (error) {
    console.error('Delete failed:', error);
    throw new StorageError('Delete failed', 500, 'InternalError');
  }
}

/**
 * Handle OPTIONS requests for CORS
 */
function handleOptionsRequest(request) {
  return new Response(null, {
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, PUT, DELETE, HEAD, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Amz-Date, X-Amz-Content-Sha256, Content-MD5',
      'Access-Control-Max-Age': '86400',
    },
  });
}

/**
 * Fetch object from source storage
 */
async function fetchFromSourceStorage(env, ctx, objectKey) {
  // This is a placeholder for actual implementation
  // In a real implementation, you would use proper S3 client library or APIs
  
  if (!CONFIG.SOURCE_STORAGE.endpoint) {
    return null;
  }
  
  try {
    // Construct source storage URL
    const sourceUrl = `${CONFIG.SOURCE_STORAGE.endpoint}/${CONFIG.SOURCE_STORAGE.bucketName}/${objectKey}`;
    
    // Create a signed request to source storage
    // This is simplified; actual implementation would involve proper S3 signature generation
    const response = await fetch(sourceUrl, {
      headers: {
        // Add authorization headers as needed
      },
    });
    
    if (!response.ok) {
      console.error(`Source storage returned ${response.status}: ${await response.text()}`);
      return null;
    }
    
    // Extract headers and body
    const headers = {};
    for (const [key, value] of response.headers.entries()) {
      headers[key] = value;
    }
    
    // Create a simulated R2 object
    return {
      body: response.body,
      httpEtag: response.headers.get('ETag'),
      httpMetadata: {
        contentType: response.headers.get('Content-Type'),
        contentLength: response.headers.get('Content-Length'),
        cacheControl: response.headers.get('Cache-Control'),
      },
      customMetadata: {
        source: 'lazy-loaded',
      },
    };
  } catch (error) {
    console.error('Error fetching from source storage:', error);
    return null;
  }
}

/**
 * Store object in R2
 */
async function storeObjectInR2(env, object, objectKey) {
  try {
    await env.R2_BUCKET.put(objectKey, object.body, {
      httpMetadata: object.httpMetadata,
      customMetadata: {
        ...object.customMetadata,
        lazyLoaded: 'true',
        lazyLoadedAt: new Date().toISOString(),
      },
    });
    return true;
  } catch (error) {
    console.error('Failed to store object in R2:', error);
    return false;
  }
}

/**
 * Queue a sync task
 */
async function queueSyncTask(env, objectKey, action) {
  try {
    await env.SYNC_QUEUE.send({
      key: objectKey,
      action: action,
      timestamp: new Date().toISOString(),
    });
    return true;
  } catch (error) {
    console.error('Failed to queue sync task:', error);
    return false;
  }
}

/**
 * Get response headers from object
 */
function getResponseHeaders(object) {
  const headers = new Headers();
  
  // Standard headers
  headers.set('Content-Type', object.httpMetadata?.contentType || 'application/octet-stream');
  if (object.httpMetadata?.contentLength) {
    headers.set('Content-Length', object.httpMetadata.contentLength);
  }
  if (object.httpEtag) {
    headers.set('ETag', object.httpEtag);
  }
  headers.set('Cache-Control', object.httpMetadata?.cacheControl || CONFIG.DEFAULT_CACHE_CONTROL);
  headers.set('Last-Modified', object.uploaded ? new Date(object.uploaded).toUTCString() : new Date().toUTCString());
  
  // Custom metadata as headers
  if (object.customMetadata) {
    for (const [key, value] of Object.entries(object.customMetadata)) {
      headers.set(`x-amz-meta-${key}`, value);
    }
  }
  
  // CORS headers
  headers.set('Access-Control-Allow-Origin', '*');
  
  return headers;
}

/**
 * Format error response
 */
function formatErrorResponse(error) {
  const status = error.status || 500;
  const code = error.code || 'InternalError';
  const message = error.message || 'Internal Server Error';
  
  const xmlBody = `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>${code}</Code>
  <Message>${message}</Message>
  <RequestId>worker-${Date.now()}</RequestId>
</Error>`;
  
  return new Response(xmlBody, {
    status: status,
    headers: {
      'Content-Type': 'application/xml',
    },
  });
} 