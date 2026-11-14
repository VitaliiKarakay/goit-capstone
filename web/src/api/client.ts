export type FieldError = { field: string; code: string };

export class ApiError extends Error {
  code: string;
  details?: FieldError[];
  status: number;
  constructor(status: number, code: string, message: string, details?: FieldError[]) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export async function apiFetch<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal,
): Promise<T> {
  const res = await fetch(path, { ...init, signal });
  const text = await res.text();
  let body: unknown = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
  }
  if (!res.ok) {
    const err = body as { error?: { code?: string; message?: string; details?: FieldError[] } };
    throw new ApiError(
      res.status,
      err?.error?.code ?? "error",
      err?.error?.message ?? res.statusText,
      err?.error?.details,
    );
  }
  return body as T;
}

export async function healthCheck(signal?: AbortSignal): Promise<boolean> {
  try {
    const res = await apiFetch<{ status: string }>("/healthz", undefined, signal);
    return res.status === "ok";
  } catch {
    return false;
  }
}