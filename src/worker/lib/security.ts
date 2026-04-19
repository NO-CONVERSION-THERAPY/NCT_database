import type { Context } from 'hono';

function readToken(request: Request): string | null {
  const authorization = request.headers.get('authorization');
  if (authorization?.startsWith('Bearer ')) {
    return authorization.slice('Bearer '.length).trim();
  }

  return request.headers.get('x-api-token');
}

export function assertToken(
  context: Context,
  expectedToken: string | undefined,
  label: string,
): Response | null {
  if (!expectedToken) {
    return null;
  }

  const providedToken = readToken(context.req.raw);
  if (providedToken === expectedToken) {
    return null;
  }

  return context.json(
    {
      error: `${label} token is invalid.`,
    },
    401,
  );
}
