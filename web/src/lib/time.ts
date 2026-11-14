const FMT = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  timeZoneName: "short",
});

export function formatApiTime(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : FMT.format(d);
}

/** RFC3339 with offset for API (microsecond precision, Z if UTC). */
export function toApiTime(date: Date): string {
  const pad = (n: number, w = 2) => String(n).padStart(w, "0");
  const y = date.getUTCFullYear();
  const mo = pad(date.getUTCMonth() + 1);
  const da = pad(date.getUTCDate());
  const h = pad(date.getUTCHours());
  const mi = pad(date.getUTCMinutes());
  const s = pad(date.getUTCSeconds());
  const micro = pad(date.getUTCMilliseconds(), 3) + "000";
  return `${y}-${mo}-${da}T${h}:${mi}:${s}.${micro}Z`;
}

export function localInputToApiTime(value: string): string {
  const d = new Date(value);
  return toApiTime(d);
}

export function delayToApiTime(seconds: number): string {
  return toApiTime(new Date(Date.now() + seconds * 1000));
}