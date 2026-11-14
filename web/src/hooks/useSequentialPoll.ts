import { useCallback, useEffect, useRef } from "react";

export function useSequentialPoll(
  tick: (signal: AbortSignal) => Promise<void>,
  enabled: boolean,
  intervalMs = 2500,
) {
  const tickRef = useRef(tick);
  tickRef.current = tick;
  const abortRef = useRef<AbortController | null>(null);

  const run = useCallback(async () => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    const ac = new AbortController();
    abortRef.current = ac;
    try {
      await tickRef.current(ac.signal);
    } catch (e) {
      if ((e as Error).name !== "AbortError") {
        console.error(e);
      }
    }
  }, []);

  useEffect(() => {
    if (!enabled) {
      abortRef.current?.abort();
      return;
    }
    let cancelled = false;
    const loop = async () => {
      while (!cancelled) {
        await run();
        await new Promise((r) => setTimeout(r, intervalMs));
      }
    };
    void loop();
    return () => {
      cancelled = true;
      abortRef.current?.abort();
    };
  }, [enabled, intervalMs, run]);
}