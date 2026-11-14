import { FormEvent, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { ApiError, apiFetch } from "../api/client";
import {
  DEFAULT_RECIPIENTS,
  MAX_CAMPAIGN_BODY,
  MAX_RECIPIENTS,
  VarRule,
  buildCampaignBody,
  generateRecipients,
  utf8BodySize,
} from "../lib/campaignGenerator";
import { renderTemplate } from "../lib/templateRender";
import { delayToApiTime, localInputToApiTime } from "../lib/time";

type ScheduleMode = "absolute" | "delay";

export function CampaignCreatePage() {
  const nav = useNavigate();
  const [subject, setSubject] = useState("[name], we have an offer");
  const [bodyTpl, setBodyTpl] = useState("Your discount is [sum] [currency]");
  const [count, setCount] = useState(DEFAULT_RECIPIENTS);
  const [mode, setMode] = useState<ScheduleMode>("delay");
  const [delaySec, setDelaySec] = useState(45);
  const [localTime, setLocalTime] = useState("");
  const [seed, setSeed] = useState(() => Math.floor(Math.random() * 1e9));
  const [previewStale, setPreviewStale] = useState(true);
  const [frozenRecipients, setFrozenRecipients] = useState<ReturnType<typeof generateRecipients> | null>(null);
  const [frozenScheduledAt, setFrozenScheduledAt] = useState<string | null>(null);
  const [pending, setPending] = useState<{ key: string; body: string; scheduledAt: string } | null>(null);
  const [message, setMessage] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const rules: Record<string, VarRule> = useMemo(
    () => ({
      name: { kind: "random_name" },
      sum: { kind: "random_int", min: 10, max: 100 },
      currency: { kind: "pick", options: ["USD", "EUR", "UAH"] },
    }),
    [],
  );

  const configVersion = useMemo(
    () => JSON.stringify({ subject, bodyTpl, count, mode, delaySec, localTime, seed }),
    [subject, bodyTpl, count, mode, delaySec, localTime, seed],
  );
  const [lockedConfig, setLockedConfig] = useState<string | null>(null);

  function markStale() {
    setPreviewStale(true);
    if (pending && lockedConfig && configVersion !== lockedConfig) {
      setMessage("Form changed after an incomplete submit. Retry the pending request or discard it.");
    }
  }

  function regenPreview() {
    setSeed(Math.floor(Math.random() * 1e9));
    setFrozenRecipients(null);
    setPreviewStale(true);
    markStale();
  }

  function updatePreview() {
    const recipients = generateRecipients(count, seed, rules, subject, bodyTpl);
    setFrozenRecipients(recipients);
    setPreviewStale(false);
  }

  function resolveScheduledAt(): string {
    return mode === "delay" ? delayToApiTime(delaySec) : localInputToApiTime(localTime);
  }

  async function submit(reuse?: typeof pending) {
    setSubmitting(true);
    setMessage("");
    try {
      if (!reuse) {
        if (previewStale || !frozenRecipients) {
          setMessage("Update preview before submit.");
          setSubmitting(false);
          return;
        }
        const scheduledAt = frozenScheduledAt ?? resolveScheduledAt();
        if (!frozenScheduledAt) setFrozenScheduledAt(scheduledAt);
        const key = `ui-campaign-${crypto.randomUUID()}`;
        const bodyObj = buildCampaignBody(key, scheduledAt, subject, bodyTpl, frozenRecipients);
        const size = utf8BodySize(bodyObj);
        if (size > MAX_CAMPAIGN_BODY) {
          setMessage(`Request would be ${size} bytes (limit ${MAX_CAMPAIGN_BODY}). Reduce recipients or template size.`);
          setSubmitting(false);
          return;
        }
        const raw = JSON.stringify(bodyObj);
        setPending({ key, body: raw, scheduledAt });
        setLockedConfig(configVersion);
        reuse = { key, body: raw, scheduledAt };
      }
      const res = await apiFetch<{ campaign_id: string }>("/campaigns", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: reuse.body,
      });
      setPending(null);
      setLockedConfig(null);
      nav(`/campaigns/${res.campaign_id}`);
    } catch (e) {
      if (e instanceof ApiError) setMessage(e.message);
      else setMessage("Network error. Use Retry same request to continue.");
    } finally {
      setSubmitting(false);
    }
  }

  function onSubmit(ev: FormEvent) {
    ev.preventDefault();
    void submit();
  }

  const previewRows = frozenRecipients?.slice(0, 5) ?? [];

  return (
    <section>
      <h2>Create campaign</h2>
      <form onSubmit={onSubmit} className="form">
        <label>
          Template subject
          <input
            value={subject}
            onChange={(e) => {
              setSubject(e.target.value);
              markStale();
            }}
          />
        </label>
        <label>
          Template body
          <textarea
            value={bodyTpl}
            onChange={(e) => {
              setBodyTpl(e.target.value);
              markStale();
            }}
            rows={3}
          />
        </label>
        <label>
          Recipients ({DEFAULT_RECIPIENTS} default, max {MAX_RECIPIENTS})
          <input
            type="number"
            min={1}
            max={MAX_RECIPIENTS}
            value={count}
            onChange={(e) => {
              setCount(Number(e.target.value));
              markStale();
            }}
          />
        </label>
        <fieldset>
          <legend>Schedule</legend>
          <label>
            <input type="radio" checked={mode === "delay"} onChange={() => setMode("delay")} /> Delay (seconds)
          </label>
          {mode === "delay" && (
            <input
              type="number"
              min={1}
              value={delaySec}
              onChange={(e) => {
                setDelaySec(Number(e.target.value));
                markStale();
              }}
            />
          )}
          <label>
            <input type="radio" checked={mode === "absolute"} onChange={() => setMode("absolute")} /> Absolute (local TZ)
          </label>
          {mode === "absolute" && (
            <input
              type="datetime-local"
              value={localTime}
              onChange={(e) => {
                setLocalTime(e.target.value);
                markStale();
              }}
            />
          )}
          <p className="hint">Scheduled time is fixed on first submit and kept for Retry.</p>
        </fieldset>
        <div className="toolbar">
          <button type="button" className="btn" onClick={regenPreview}>
            Regenerate random seed
          </button>
          <button type="button" className="btn" onClick={updatePreview}>
            Update preview
          </button>
        </div>
        {previewStale && <p className="warn">Preview is stale — update preview before submit.</p>}
        {previewRows.length > 0 && !previewStale && (
          <div className="preview">
            <h3>Preview (first {previewRows.length})</h3>
            <ul>
              {previewRows.map((r) => (
                <li key={r.recipient_id}>
                  <strong>{r.recipient_id}</strong> — {renderTemplate(subject, r.variables)} /{" "}
                  {renderTemplate(bodyTpl, r.variables)}
                </li>
              ))}
            </ul>
          </div>
        )}
        {message && <p className="error">{message}</p>}
        <div className="toolbar">
          <button className="btn primary" type="submit" disabled={submitting}>
            {submitting ? "Submitting…" : "Create campaign"}
          </button>
          {pending && (
            <>
              <button type="button" className="btn" disabled={submitting} onClick={() => void submit(pending)}>
                Retry same request
              </button>
              <button
                type="button"
                className="btn"
                onClick={() => {
                  if (
                    window.confirm(
                      "Previous submit might have created a campaign. Continue only if you know it failed.",
                    )
                  ) {
                    setPending(null);
                    setLockedConfig(null);
                  }
                }}
              >
                Start new request
              </button>
            </>
          )}
          <Link to="/campaigns">Back</Link>
        </div>
      </form>
    </section>
  );
}