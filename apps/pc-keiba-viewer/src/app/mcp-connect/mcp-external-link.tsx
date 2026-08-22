// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

interface McpExternalLinkProps {
  href: string;
  label: string;
}

export function McpExternalLink({ href, label }: McpExternalLinkProps) {
  return (
    <a className="mcp-connect-link" href={href} rel="noopener noreferrer" target="_blank">
      {label}
    </a>
  );
}
