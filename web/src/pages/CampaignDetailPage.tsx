import { useCallback, useRef, useState } from "react";
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

type CancelAllResult = {
  campaign_id: string;
  canceled_now: number;
  already_canceled: number;
  too_late: number;
  total_jobs: number;
};

export function CampaignDetailPage() {
  const { id } = useParams();
  const [summary, setSummary] = useState<Campaign | null>(null);
  const [jobs, setJobs] = useState<JobPage | null>(null);
  const [statusFilter, setStatusFilter] = useState("");
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [cancelOut, setCancelOut] = useState<CancelResult[] | null>(null);
  const [jobDetail, setJobDetail] = useState<{ subject?: string; body?: string; to?: string } | null>(null);
  const [cancelAllResult, setCancelAllResult] = useState<CancelAllResult | null>(null);
  const [cancelAllBusy, setCancelAllBusy] = useState(false);
  const dataEpoch = useRef(0);
  const limit = 20;

  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!id) return;
      const epoch = dataEpoch.current;
      const q = new URLSearchParams({ limit: String(limit), offset: String(offset) });
      if (statusFilter) q.set("status", statusFilter);
      const [camp, jobPage] = await Promise.all([
        apiFetch<Campaign>(`/campaigns/${id}`, undefined, signal),
        apiFetch<JobPage>(`/campaigns/${id}/jobs?${q}`, undefined, signal),
      ]);
      if (epoch !== dataEpoch.current) return;
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

  async function reloadFresh() {
    dataEpoch.current += 1;
    await load(new AbortController().signal);
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
    await reloadFresh();
  }

  async function cancelAllCampaign() {
    if (!id) return;
    if (
      !window.confirm(
        "Cancel all pending jobs in this campaign? Jobs already processing or completed will not be affected.",
      )
    ) {
      return;
    }
    setCancelAllBusy(true);
    try {
      const res = await apiFetch<CancelAllResult>(`/campaigns/${id}/cancel-all`, { method: "POST" });
      setCancelAllResult(res);
      setCancelOut(null);
      setSelected(new Set());
      await reloadFresh();
    } catch (e) {
      console.error(e);
      setCancelAllResult(null);
    } finally {
      setCancelAllBusy(false);
    }
  }

  const pendingCount = (summary?.counts.scheduled ?? 0) + (summary?.counts.queued ?? 0);

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
        <button
          type="button"
          className="btn"
          disabled={pendingCount === 0 || cancelAllBusy}
          onClick={() => void cancelAllCampaign()}
        >
          {cancelAllBusy ? "Canceling…" : "Cancel campaign"}
        </button>
        <button type="button" className="btn" disabled={selected.size === 0} onClick={() => void cancelSelected()}>
          Cancel selected ({selected.size})
        </button>
        <Link to="/campaigns">Back</Link>
      </div>
      {cancelAllResult && (
        <div className="preview">
          <h3>Cancel campaign result</h3>
          <p>
            Newly canceled: {cancelAllResult.canceled_now}, already canceled: {cancelAllResult.already_canceled}, too
            late: {cancelAllResult.too_late} (total jobs: {cancelAllResult.total_jobs})
          </p>
        </div>
      )}
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