/** Opaque metadata pagination never pins a data generation or expands actor authority. */
export const mcpRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_mcp_cursors (
  token_hash text PRIMARY KEY,environment text NOT NULL REFERENCES moria.environments(id),
  actor_hash text NOT NULL,profile_hash text NOT NULL,method text NOT NULL,
  security_epoch bigint NOT NULL,instance_epoch bigint NOT NULL,after_key text NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE INDEX runtime_mcp_cursor_expiry ON moria.runtime_mcp_cursors(expires_at);
`;
