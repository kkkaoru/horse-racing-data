// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  buildAuthorizationServerMetadata,
  buildProtectedResourceMetadata,
} from "./mcp-oauth-metadata";

it("builds protected resource metadata for the MCP resource", () => {
  expect(buildProtectedResourceMetadata("https://viewer.example.test")).toStrictEqual({
    authorization_servers: ["https://viewer.example.test"],
    bearer_methods_supported: ["header"],
    resource: "https://viewer.example.test/mcp",
    scopes_supported: ["mcp"],
  });
});

it("builds authorization server metadata with PKCE S256 and DCR", () => {
  expect(buildAuthorizationServerMetadata("https://viewer.example.test")).toStrictEqual({
    authorization_endpoint: "https://viewer.example.test/oauth/authorize",
    authorization_response_iss_parameter_supported: true,
    client_id_metadata_document_supported: true,
    code_challenge_methods_supported: ["S256"],
    grant_types_supported: ["authorization_code", "refresh_token"],
    issuer: "https://viewer.example.test",
    registration_endpoint: "https://viewer.example.test/oauth/register",
    response_types_supported: ["code"],
    scopes_supported: ["mcp"],
    token_endpoint: "https://viewer.example.test/oauth/token",
    token_endpoint_auth_methods_supported: ["none"],
  });
});
