import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { apiFetch } from "../api/client";
import { StatusBadge, campaignPhase } from "../components/StatusBadge";
import { useSequentialPoll } from "../hooks/useSequentialPoll";
import { formatApiTime } from "../lib/time";
import { Pager } from "./JobListPage";

type Campaign = {
  campaign_id: string;
  scheduled_at: string;
  total_jobs: number;
  counts: Record<string, number>;
  template?: { subject: string; body: string };
};

type JobRow = {
  job_id: string;
  recipient_id: string;
  status: string;
  scheduled_at: string;
  last_error?: string | null;
};

type JobPage = { items: JobRow[]; total: number; limit: number; offset: number };

type CancelResult = { recipient_id: string; result: string };

export function CampaignDetailPage() {
  const { id } = useParams();
  const [summary, setSummary] = useState<Campaign | null>(null);
  const [jobs, setJobs] = useState<JobPage | null>(null);
  const [statusFilter, setStatusFilter] = useState("");
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [cancelOut, setCancelOut] = useState<CancelResult[] | null>(null);
  const [jobDetail, setJobDetail] = useState<{ subject?: string; body?: string; to?: string } | null>(null);
  const limit = 20;

  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!id) return;
      const q = new URLSearchParams({ limit: String(limit), offset: String(offset) });
      if (statusFilter) q.set("status", statusFilter);
      const [camp, jobPage] = await Promise.all([
        apiFetch<Campaign>(`/campaigns/${id}`, undefined, signal),
        apiFetch<JobPage>(`/campaigns/${id}/jobs?${q}`, undefined, signal),
      ]);
      setSummary(camp);
      setJobs(jobPage);
    },
    [id, offset, statusFilter],
  );

  useSequentialPoll(load, Boolean(id));

  function toggle(id: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  async function cancelSelected() {
    if (!id || selected.size === 0) return;
    const res = await apiFetch<{ results: CancelResult[] }>(`/campaigns/${id}/cancel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ recipient_ids: [...selected] }),
    });
    setCancelOut(res.results);
    setSelected(new Set());
    await load(new AbortController().signal);
  }

  async function openMessage(jobId: string) {
    const job = await apiFetch<{ payload: { to?: string; subject?: string; body?: string } }>(`/jobs/${jobId}`);
    setJobDetail(job.payload);
  }

  if (!summary || !jobs) return <p>Loading…</p>;

  return (
    <section>
      <h2>Campaign {summary.campaign_id}</h2>
      <p>
        <strong>{campaignPhase(summary.counts)}</strong> — scheduled {formatApiTime(summary.scheduled_at)}
      </p>
      <p className="counts">
        succeeded {summary.counts.succeeded ?? 0}, failed {summary.counts.failed ?? 0}, canceled{" "}
        {summary.counts.canceled ?? 0}, total {summary.total_jobs}
      </p>
      {summary.template && (
        <div className="preview">
          <h3>Template</h3>
          <p>
            <strong>Subject:</strong> {summary.template.subject}
          </p>
          <pre>{summary.template.body}</pre>
        </div>
      )}
      <label>
        Filter status{" "}
        <select
          value={statusFilter}
          onChange={(e) => {
            setOffset(0);
            setSelected(new Set());
            setStatusFilter(e.target.value);
          }}
        >
          <option value="">All</option>
          {["scheduled", "queued", "processing", "succeeded", "failed", "canceled"].map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </label>
      <table>
        <thead>
          <tr>
            <th />
            <th>Recipient</th>
            <th>Status</th>
            <th>Scheduled</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {jobs.items.map((j) => (
            <tr key={j.job_id}>
              <td>
                <input type="checkbox" checked={selected.has(j.recipient_id)} onChange={() => toggle(j.recipient_id)} />
              </td>
              <td>{j.recipient_id}</td>
              <td>
                <StatusBadge status={j.status} />
              </td>
              <td>{formatApiTime(j.scheduled_at)}</td>
              <td>
                <button type="button" className="linkish" onClick={() => void openMessage(j.job_id)}>
                  Message
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <Pager
        total={jobs.total}
        limit={limit}
        offset={offset}
        setOffset={(n) => {
          setSelected(new Set());
          setOffset(n);
        }}
      />
      <div className="toolbar">
        <button type="button" className="btn" disabled={selected.size === 0} onClick={() => void cancelSelected()}>
          Cancel selected ({selected.size})
        </button>
        <Link to="/campaigns">Back</Link>
      </div>
      {cancelOut && (
        <div className="preview">
          <h3>Cancel results</h3>
          <ul>
            {cancelOut.map((r) => (
              <li key={r.recipient_id}>
                {r.recipient_id}: {r.result}
              </li>
            ))}
          </ul>
        </div>
      )}
      {jobDetail && (
        <div className="modal" onClick={() => setJobDetail(null)}>
          <div className="modal-body" onClick={(e) => e.stopPropagation()}>
            <h3>Rendered message</h3>
            <p>To: {jobDetail.to}</p>
            <p>Subject: {jobDetail.subject}</p>
            <pre>{jobDetail.body}</pre>
            <button type="button" onClick={() => setJobDetail(null)}>
              Close
            </button>
          </div>
        </div>
      )}
    </section>
  );
}