import { renderTemplate } from "./templateRender";

export type VarRule =
  | { kind: "random_name" }
  | { kind: "random_int"; min: number; max: number }
  | { kind: "pick"; options: string[] }
  | { kind: "fixed"; value: string };

export type RecipientDraft = {
  recipient_id: string;
  to: string;
  variables: Record<string, string>;
};

const NAMES = ["Alex", "Jordan", "Sam", "Taylor", "Morgan", "Riley", "Casey", "Quinn"];

export function mulberry32(seed: number) {
  return () => {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function generateRecipients(
  count: number,
  seed: number,
  rules: Record<string, VarRule>,
  subjectTpl: string,
  bodyTpl: string,
): RecipientDraft[] {
  const rnd = mulberry32(seed);
  const out: RecipientDraft[] = [];
  for (let n = 0; n < count; n++) {
    const variables: Record<string, string> = {};
    for (const [key, rule] of Object.entries(rules)) {
      switch (rule.kind) {
        case "random_name":
          variables[key] = NAMES[Math.floor(rnd() * NAMES.length)];
          break;
        case "random_int": {
          const span = rule.max - rule.min + 1;
          variables[key] = String(rule.min + Math.floor(rnd() * span));
          break;
        }
        case "pick":
          variables[key] = rule.options[Math.floor(rnd() * rule.options.length)] ?? "";
          break;
        case "fixed":
          variables[key] = rule.value;
          break;
      }
    }
    const recipient_id = `demo-user-${n + 1}`;
    const local = recipient_id.replace(/[^a-z0-9-]/gi, "");
    out.push({
      recipient_id,
      to: `${local}@example.com`,
      variables,
    });
    renderTemplate(subjectTpl, variables);
    renderTemplate(bodyTpl, variables);
  }
  return out;
}

export const MAX_CAMPAIGN_BODY = 1 << 20;
export const MAX_RECIPIENTS = 1000;
export const DEFAULT_RECIPIENTS = 50;

export function buildCampaignBody(
  idempotencyKey: string,
  scheduledAt: string,
  subject: string,
  body: string,
  recipients: RecipientDraft[],
): unknown {
  return {
    idempotency_key: idempotencyKey,
    scheduled_at: scheduledAt,
    sender: "mock",
    template: { subject, body },
    recipients,
  };
}

export function utf8BodySize(body: unknown): number {
  return new TextEncoder().encode(JSON.stringify(body)).length;
}