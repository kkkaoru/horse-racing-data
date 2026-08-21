// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { mcpResourceUrl } from "./mcp-oauth-origin";

export const MCP_OAUTH_SCOPE: string = "mcp";

export const buildProtectedResourceMetadata = (origin: string): Record<string, unknown> => ({
  authorization_servers: [origin],
  bearer_methods_supported: ["header"],
  resource: mcpResourceUrl(origin),
  scopes_supported: [MCP_OAUTH_SCOPE],
});

export const buildAuthorizationServerMetadata = (origin: string): Record<string, unknown> => ({
  authorization_endpoint: `${origin}/oauth/authorize`,
  authorization_response_iss_parameter_supported: true,
  client_id_metadata_document_supported: true,
  code_challenge_methods_supported: ["S256"],
  grant_types_supported: ["authorization_code", "refresh_token"],
  issuer: origin,
  registration_endpoint: `${origin}/oauth/register`,
  response_types_supported: ["code"],
  scopes_supported: [MCP_OAUTH_SCOPE],
  token_endpoint: `${origin}/oauth/token`,
  token_endpoint_auth_methods_supported: ["none"],
});
