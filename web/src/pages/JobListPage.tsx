import { useCallback, useState } from "react";
import { Link } from "react-router-dom";
import { apiFetch } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { useSequentialPoll } from "../hooks/useSequentialPoll";
import { formatApiTime } from "../lib/time";

type JobRow = {
  id: string;
  recipient_id: string;
  status: string;
  scheduled_at: string;
  updated_at: string;
};

type Page = { items: JobRow[]; total: number; limit: number; offset: number };

const STATUSES = ["", "scheduled", "queued", "processing", "succeeded", "failed", "canceled"];

export function JobListPage() {
  const [status, setStatus] = useState("");
  const [offset, setOffset] = useState(0);
  const [page, setPage] = useState<Page | null>(null);
  const [apiDown, setApiDown] = useState(false);
  const limit = 20;

  const load = useCallback(
    async (signal: AbortSignal) => {
      const q = new URLSearchParams({ limit: String(limit), offset: String(offset) });
      if (status) q.set("status", status);
      try {
        const data = await apiFetch<Page>(`/jobs?${q}`, undefined, signal);
        setPage(data);
        setApiDown(false);
      } catch (e) {
        if ((e as Error).name !== "AbortError") setApiDown(true);
      }
    },
    [status, offset],
  );

  useSequentialPoll(load, true);

  return (
    <section>
      <ServiceWarning on={apiDown} />
      <div className="toolbar">
        <Link className="btn primary" to="/jobs/new">
          Create job
        </Link>
        <label>
          Status{" "}
          <select
            value={status}
            onChange={(e) => {
              setOffset(0);
              setStatus(e.target.value);
            }}
          >
            <option value="">All</option>
            {STATUSES.filter(Boolean).map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
      </div>
      {!page ? (
        <p>Loading…</p>
      ) : page.items.length === 0 ? (
        <p className="empty">No standalone jobs match this filter.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Recipient</th>
              <th>Status</th>
              <th>Scheduled</th>
              <th>Updated</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {page.items.map((j) => (
              <tr key={j.id}>
                <td>{j.recipient_id}</td>
                <td>
                  <StatusBadge status={j.status} />
                </td>
                <td>{formatApiTime(j.scheduled_at)}</td>
                <td>{formatApiTime(j.updated_at)}</td>
                <td>
                  <Link to={`/jobs/${j.id}`}>Open</Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <Pager total={page?.total ?? 0} limit={limit} offset={offset} setOffset={setOffset} />
    </section>
  );
}

export function Pager({
  total,
  limit,
  offset,
  setOffset,
}: {
  total: number;
  limit: number;
  offset: number;
  setOffset: (n: number) => void;
}) {
  const canPrev = offset > 0;
  const canNext = offset + limit < total;
  return (
    <div className="pager">
      <button type="button" disabled={!canPrev} onClick={() => setOffset(Math.max(0, offset - limit))}>
        Previous
      </button>
      <span>
        {offset + 1}–{Math.min(offset + limit, total)} of {total}
      </span>
      <button type="button" disabled={!canNext} onClick={() => setOffset(offset + limit)}>
        Next
      </button>
    </div>
  );
}

function ServiceWarning({ on }: { on: boolean }) {
  if (!on) return null;
  return <p className="error">API is unavailable. Check that the controller is running.</p>;
}