import { useCallback, useState } from "react";
import { Link } from "react-router-dom";
import { apiFetch } from "../api/client";
import { StatusBadge, campaignPhase } from "../components/StatusBadge";
import { useSequentialPoll } from "../hooks/useSequentialPoll";
import { formatApiTime } from "../lib/time";
import { Pager } from "./JobListPage";

type CampaignRow = {
  campaign_id: string;
  scheduled_at: string;
  total_jobs: number;
  counts: Record<string, number>;
};

type Page = { items: CampaignRow[]; total: number; limit: number; offset: number };

export function CampaignListPage() {
  const [offset, setOffset] = useState(0);
  const [page, setPage] = useState<Page | null>(null);
  const limit = 20;

  const load = useCallback(
    async (signal: AbortSignal) => {
      const data = await apiFetch<Page>(`/campaigns?limit=${limit}&offset=${offset}`, undefined, signal);
      setPage(data);
    },
    [offset],
  );

  useSequentialPoll(load, true);

  return (
    <section>
      <div className="toolbar">
        <Link className="btn primary" to="/campaigns/new">
          Create campaign
        </Link>
      </div>
      {!page ? (
        <p>Loading…</p>
      ) : page.items.length === 0 ? (
        <p className="empty">No campaigns yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Phase</th>
              <th>Scheduled</th>
              <th>Jobs</th>
              <th>Counts</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {page.items.map((c) => (
              <tr key={c.campaign_id}>
                <td>{campaignPhase(c.counts)}</td>
                <td>{formatApiTime(c.scheduled_at)}</td>
                <td>{c.total_jobs}</td>
                <td className="counts">
                  <StatusBadge status="succeeded" /> {c.counts.succeeded ?? 0}{" "}
                  <StatusBadge status="failed" /> {c.counts.failed ?? 0}{" "}
                  <StatusBadge status="canceled" /> {c.counts.canceled ?? 0}{" "}
                  <StatusBadge status="unknown" /> {c.counts.unknown ?? 0}
                </td>
                <td>
                  <Link to={`/campaigns/${c.campaign_id}`}>Open</Link>
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