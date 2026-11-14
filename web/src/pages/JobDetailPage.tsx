import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { ApiError, apiFetch } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { useSequentialPoll } from "../hooks/useSequentialPoll";
import { formatApiTime } from "../lib/time";

type Job = {
  id: string;
  recipient_id: string;
  status: string;
  scheduled_at: string;
  payload: { to?: string; subject?: string; body?: string };
  last_error?: string | null;
  created_at: string;
  updated_at: string;
  canceled_at?: string | null;
  campaign_id?: string;
};

export function JobDetailPage() {
  const { id } = useParams();
  const [job, setJob] = useState<Job | null>(null);
  const [message, setMessage] = useState("");

  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!id) return;
      const data = await apiFetch<Job>(`/jobs/${id}`, undefined, signal);
      setJob(data);
    },
    [id],
  );

  useSequentialPoll(load, Boolean(id));

  async function cancel() {
    if (!id) return;
    setMessage("");
    try {
      await apiFetch(`/jobs/${id}/cancel`, { method: "POST" });
      await load(new AbortController().signal);
    } catch (e) {
      if (e instanceof ApiError && e.code === "too_late") {
        setMessage("Cancel is too late for the current job state.");
        await load(new AbortController().signal);
      } else if (e instanceof ApiError) {
        setMessage(e.message);
      }
    }
  }

  if (!job) return <p>Loading…</p>;
  if (job.campaign_id) {
    return (
      <p>
        This job belongs to a campaign.{" "}
        <Link to={`/campaigns/${job.campaign_id}`}>Open campaign</Link>
      </p>
    );
  }

  const canCancel = job.status === "scheduled" || job.status === "queued";

  return (
    <section>
      <h2>Job {job.id}</h2>
      <StatusBadge status={job.status} />
      <dl className="detail">
        <dt>Recipient</dt>
        <dd>{job.recipient_id}</dd>
        <dt>Scheduled</dt>
        <dd>{formatApiTime(job.scheduled_at)}</dd>
        <dt>To</dt>
        <dd>{job.payload?.to}</dd>
        <dt>Subject</dt>
        <dd>{job.payload?.subject}</dd>
        <dt>Body</dt>
        <dd>
          <pre>{job.payload?.body}</pre>
        </dd>
        {job.last_error && (
          <>
            <dt>Error</dt>
            <dd>{job.last_error}</dd>
          </>
        )}
        <dt>Updated</dt>
        <dd>{formatApiTime(job.updated_at)}</dd>
      </dl>
      {message && <p className="error">{message}</p>}
      <div className="toolbar">
        {canCancel && (
          <button type="button" className="btn" onClick={() => void cancel()}>
            Cancel job
          </button>
        )}
        <Link to="/jobs">Back</Link>
      </div>
    </section>
  );
}