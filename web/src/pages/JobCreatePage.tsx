import { FormEvent, useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { ApiError, apiFetch } from "../api/client";
import { delayToApiTime, localInputToApiTime } from "../lib/time";

type ScheduleMode = "absolute" | "delay";

type PendingCreate = {
  idempotencyKey: string;
  body: string;
  scheduledAt: string;
};

export function JobCreatePage() {
  const nav = useNavigate();
  const [recipientId, setRecipientId] = useState("user-demo");
  const [to, setTo] = useState("user@example.com");
  const [subject, setSubject] = useState("Demo subject");
  const [body, setBody] = useState("Hello from the UI");
  const [mode, setMode] = useState<ScheduleMode>("delay");
  const [delaySec, setDelaySec] = useState(30);
  const [localTime, setLocalTime] = useState("");
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [message, setMessage] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [pending, setPending] = useState<PendingCreate | null>(null);
  const lockedKeyRef = useRef<string | null>(null);

  const formFingerprint = useMemo(
    () => JSON.stringify({ recipientId, to, subject, body, mode, delaySec, localTime }),
    [recipientId, to, subject, body, mode, delaySec, localTime],
  );
  const lastFp = useRef(formFingerprint);

  function buildBody(fixedScheduled?: string): { key: string; raw: string; scheduledAt: string } {
    let scheduledAt = fixedScheduled;
    if (!scheduledAt) {
      scheduledAt = mode === "delay" ? delayToApiTime(delaySec) : localInputToApiTime(localTime);
    }
    const key = lockedKeyRef.current ?? `ui-job-${crypto.randomUUID()}`;
    const payload = {
      idempotency_key: key,
      recipient_id: recipientId,
      scheduled_at: scheduledAt,
      sender: "mock",
      payload: { to, subject, body },
    };
    return { key, raw: JSON.stringify(payload), scheduledAt };
  }

  async function submit(fixed?: PendingCreate) {
    setSubmitting(true);
    setMessage("");
    setFieldErrors({});
    try {
      let attempt = fixed;
      if (!attempt) {
        const built = buildBody(pending?.scheduledAt);
        lockedKeyRef.current = built.key;
        attempt = {
          idempotencyKey: built.key,
          body: built.raw,
          scheduledAt: built.scheduledAt,
        };
        setPending(attempt);
      }
      const res = await apiFetch<{ id: string }>("/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: attempt.body,
      });
      setPending(null);
      lockedKeyRef.current = null;
      nav(`/jobs/${res.id}`);
    } catch (e) {
      if (e instanceof ApiError) {
        if (e.details?.length) {
          const map: Record<string, string> = {};
          for (const d of e.details) map[d.field] = d.code;
          setFieldErrors(map);
        } else {
          setMessage(e.message);
        }
      } else {
        setMessage("Network error. You can retry the same request or start a new one.");
      }
    } finally {
      setSubmitting(false);
    }
  }

  function onSubmit(ev: FormEvent) {
    ev.preventDefault();
    if (formFingerprint !== lastFp.current && pending) {
      setMessage("Form changed after an incomplete submit. Retry the pending request or discard it.");
      return;
    }
    lastFp.current = formFingerprint;
    void submit();
  }

  function discardPending() {
    if (
      pending &&
      !window.confirm(
        "A previous submit might have created a job. Starting fresh may create a duplicate if the first request succeeded.",
      )
    ) {
      return;
    }
    setPending(null);
    lockedKeyRef.current = null;
    lastFp.current = formFingerprint;
    setMessage("Pending request cleared.");
  }

  return (
    <section>
      <h2>Create single job</h2>
      <form onSubmit={onSubmit} className="form">
        <label>
          Recipient ID
          <input value={recipientId} onChange={(e) => setRecipientId(e.target.value)} required />
          {fieldErrors.recipient_id && <span className="field-error">{fieldErrors.recipient_id}</span>}
        </label>
        <label>
          To
          <input value={to} onChange={(e) => setTo(e.target.value)} required />
        </label>
        <label>
          Subject
          <input value={subject} onChange={(e) => setSubject(e.target.value)} required />
        </label>
        <label>
          Body
          <textarea value={body} onChange={(e) => setBody(e.target.value)} rows={4} required />
        </label>
        <fieldset>
          <legend>Schedule</legend>
          <label>
            <input type="radio" checked={mode === "delay"} onChange={() => setMode("delay")} /> Delay (seconds)
          </label>
          {mode === "delay" && (
            <input type="number" min={1} value={delaySec} onChange={(e) => setDelaySec(Number(e.target.value))} />
          )}
          <label>
            <input type="radio" checked={mode === "absolute"} onChange={() => setMode("absolute")} /> Absolute (local TZ)
          </label>
          {mode === "absolute" && (
            <input type="datetime-local" value={localTime} onChange={(e) => setLocalTime(e.target.value)} required={mode === "absolute"} />
          )}
        </fieldset>
        {message && <p className="error">{message}</p>}
        <div className="toolbar">
          <button className="btn primary" type="submit" disabled={submitting}>
            {submitting ? "Submitting…" : "Create job"}
          </button>
          {pending && (
            <>
              <button type="button" className="btn" disabled={submitting} onClick={() => void submit(pending)}>
                Retry same request
              </button>
              <button type="button" className="btn" onClick={discardPending}>
                Start new request
              </button>
            </>
          )}
          <Link to="/jobs">Back to list</Link>
        </div>
      </form>
    </section>
  );
}