export interface ApiRequestOptions {
  method?: 'GET' | 'POST';
  token?: string;
  body?: unknown;
}

export async function apiRequest<T>(
  path: string,
  options: ApiRequestOptions = {},
): Promise<T> {
  const response = await fetch(path, {
    method: options.method ?? 'GET',
    headers: {
      'content-type': 'application/json',
      ...(options.token
        ? {
            authorization: `Bearer ${options.token}`,
          }
        : {}),
    },
    body:
      options.body === undefined
        ? undefined
        : JSON.stringify(options.body),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(
      message || `Request failed with status ${response.status}`,
    );
  }

  if (response.status === 204) {
    return null as T;
  }

  return response.json<T>();
}
