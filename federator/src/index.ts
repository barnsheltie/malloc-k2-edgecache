/**
 * Cloudflare Worker entry point for EdgeCache Federator
 *
 * Routes requests to the ClusterFederator Durable Object
 */

import { ClusterFederator } from './federator';

export { ClusterFederator };

interface Env {
  FEDERATOR: DurableObjectNamespace;
  JWT_SECRET?: string;
}

// CORS headers for dashboard
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders,
    },
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // Health check at root
    if (url.pathname === '/' || url.pathname === '/health') {
      return jsonResponse({
        service: 'edgecache-federator',
        status: 'ok',
        version: '1.0.0',
      });
    }

    // Route all /ws, /status, and /api/* requests to the federator Durable Object
    // Use a fixed ID so all nodes connect to the same federator
    // For multi-cluster, extract cluster_id from URL or headers
    const clusterId = url.searchParams.get('cluster') || 'default';
    const federatorId = env.FEDERATOR.idFromName(clusterId);
    const federator = env.FEDERATOR.get(federatorId);

    // Forward the request to the Durable Object
    return federator.fetch(request);
  },
};
