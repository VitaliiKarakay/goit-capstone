const CLASS: Record<string, string> = {
  scheduled: "st-scheduled",
  queued: "st-queued",
  processing: "st-processing",
  succeeded: "st-succeeded",
  failed: "st-failed",
  canceled: "st-canceled",
  unknown: "st-unknown",
};

export function StatusBadge({ status }: { status: string }) {
  return <span className={`badge ${CLASS[status] ?? ""}`}>{status}</span>;
}

export function campaignPhase(counts: Record<string, number>): string {
  if (counts.processing > 0) return "Processing";
  if (counts.queued > 0) return "Queued";
  if (counts.scheduled > 0) return "Scheduled";
  if ((counts.unknown ?? 0) > 0) return "Uncertain";
  return "Finished";
}