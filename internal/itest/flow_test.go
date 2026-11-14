//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	pgmod "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/VitaliiKarakay/goit-capstone/internal/controller"
	"github.com/VitaliiKarakay/goit-capstone/internal/domain"
	natspub "github.com/VitaliiKarakay/goit-capstone/internal/messaging/nats"
	"github.com/VitaliiKarakay/goit-capstone/internal/sender"
	"github.com/VitaliiKarakay/goit-capstone/internal/storage/postgres"
	"github.com/VitaliiKarakay/goit-capstone/internal/worker"
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env, err := startEnv(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	env.close()
	os.Exit(code)
}

func TestConcurrentIdempotency(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC()
	body := jobBody("idem-"+uuid.NewString(), "user-1", at, "same")
	const n = 16
	codes := make([]int, n)
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i], ids[i], errs[i] = env.post(body)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
		if ids[i] == "" {
			t.Fatalf("empty id for status %d", codes[i])
		}
	}
	created := 0
	for i, code := range codes {
		if ids[i] == "" || ids[i] != ids[0] {
			t.Fatalf("id[%d]=%s code=%d", i, ids[i], code)
		}
		switch code {
		case http.StatusCreated:
			created++
		case http.StatusOK:
		default:
			t.Fatalf("status %d", code)
		}
	}
	if created != 1 {
		t.Fatalf("created responses = %d", created)
	}
	var rows int
	if err := env.db.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE idempotency_key = $1 AND campaign_id IS NULL`, keyFrom(t, body)).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d", rows)
	}
	status, _, err := env.post(jobBody(keyFrom(t, body), "user-1", at, "different"))
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusConflict {
		t.Fatalf("conflict status = %d", status)
	}
}

func TestSendStartsNoEarlierThanScheduledAt(t *testing.T) {
	scheduled := time.Now().Add(6 * time.Second).UTC().Truncate(time.Microsecond)
	body := jobBody("early-"+uuid.NewString(), "user-early", scheduled, "later")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)
	if env.rec.count(jobID) != 0 {
		t.Fatal("sender was called during create")
	}
	earlyDeadline := scheduled.Add(-2 * time.Second)
	for time.Now().Before(earlyDeadline) {
		if env.rec.count(jobID) != 0 {
			t.Fatal("sender was called before scheduled_at")
		}
		job := env.get(t, id)
		if job["send_started_at"] != nil {
			t.Fatalf("send_started_at=%v before scheduled_at", job["send_started_at"])
		}
		time.Sleep(150 * time.Millisecond)
	}
	job := env.waitStatus(t, id, "succeeded", 20*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
	started := mustTime(t, job["send_started_at"])
	if started.Before(scheduled) {
		t.Fatalf("send_started_at %s is before scheduled_at %s", started, scheduled)
	}
}

func TestDuplicateDeliveryDoesNotSendAgain(t *testing.T) {
	body := jobBody("dup-"+uuid.NewString(), "user-dup", time.Now().Add(-time.Second), "now")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)
	env.waitStatus(t, id, "succeeded", 20*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("calls before duplicate = %d", env.rec.count(jobID))
	}
	before := env.consumerSeq(t)
	_, err = env.js.Publish(natspub.Subject, []byte(id), nats.MsgId("redelivery-"+id))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for env.consumerSeq(t) <= before && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if env.consumerSeq(t) <= before {
		t.Fatal("duplicate message was not delivered")
	}
	time.Sleep(300 * time.Millisecond)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls after duplicate = %d", env.rec.count(jobID))
	}
	if env.get(t, id)["status"] != "succeeded" {
		t.Fatal("status changed after duplicate")
	}
}

func TestRecoversAfterDatabaseOutage(t *testing.T) {
	env.dbProxy.reject(true)
	t.Cleanup(func() { env.dbProxy.reject(false) })
	before := env.naks()
	body := jobBody("db-"+uuid.NewString(), "user-db", time.Now().Add(-time.Second), "outage")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && env.naks() == before {
		if env.rec.count(jobID) != 0 {
			t.Fatal("sender was called while the database was unavailable")
		}
		time.Sleep(100 * time.Millisecond)
	}
	got := env.naks() - before
	if got < 1 || got >= worker.MaxDeliver {
		t.Fatalf("delayed naks during outage = %d", got)
	}
	job := env.get(t, id)
	if job["status"] == "succeeded" || job["send_started_at"] != nil {
		t.Fatalf("job advanced during outage: %+v", job)
	}

	env.dbProxy.reject(false)
	env.waitStatus(t, id, "succeeded", 20*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
}

func TestRecoversWhenDatabaseFailsBeforeSend(t *testing.T) {
	env.fault.failMark.Store(true)
	t.Cleanup(func() { env.fault.failMark.Store(false) })
	before := env.naks()
	body := jobBody("mark-"+uuid.NewString(), "user-mark", time.Now().Add(-time.Second), "before-send")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)

	deadline := time.Now().Add(20 * time.Second)
	var job map[string]any
	for time.Now().Before(deadline) {
		job = env.get(t, id)
		if env.rec.count(jobID) != 0 {
			t.Fatal("sender was called before send_started_at was saved")
		}
		if job["status"] == "processing" && job["send_started_at"] == nil && env.naks() > before {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if job["status"] != "processing" || job["send_started_at"] != nil || env.naks() == before {
		t.Fatalf("job did not stay claimed before send: %+v naks=%d", job, env.naks()-before)
	}

	env.fault.failMark.Store(false)
	env.waitStatus(t, id, "succeeded", 30*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
}

func TestRecoversWhenDatabaseFailsWhileSavingResult(t *testing.T) {
	env.fault.failFinish.Store(true)
	t.Cleanup(func() { env.fault.failFinish.Store(false) })
	beforeNaks := env.naks()
	body := jobBody("finish-"+uuid.NewString(), "user-finish", time.Now().Add(-time.Second), "save-result")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)

	deadline := time.Now().Add(30 * time.Second)
	var job map[string]any
	for time.Now().Before(deadline) {
		job = env.get(t, id)
		if job["status"] == "processing" && job["send_started_at"] != nil && env.rec.count(jobID) == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if env.rec.count(jobID) != 1 || job["status"] != "processing" || job["send_started_at"] == nil {
		t.Fatalf("send was not recorded before finish: %+v calls=%d", job, env.rec.count(jobID))
	}
	if env.naks() != beforeNaks {
		t.Fatalf("finish retry returned the message to the broker: naks=%d", env.naks()-beforeNaks)
	}

	env.fault.failFinish.Store(false)
	env.waitStatus(t, id, "succeeded", 40*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
}

func TestMarkSendStartedSurvivesLostResponse(t *testing.T) {
	env.fault.loseMarkResponse.Store(true)
	t.Cleanup(func() { env.fault.loseMarkResponse.Store(false) })
	body := jobBody("marklost-"+uuid.NewString(), "user-marklost", time.Now().Add(-time.Second), "lost-response")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)
	env.waitStatus(t, id, "succeeded", 30*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
}

func TestOutboxSurvivesNATSOutage(t *testing.T) {
	env.natsProxy.reject(true)
	t.Cleanup(func() { env.natsProxy.reject(false) })
	body := jobBody("nats-"+uuid.NewString(), "user-nats", time.Now().Add(-time.Second), "broker")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	queued := false
	for time.Now().Before(deadline) {
		job := env.get(t, id)
		published, found := env.outboxPublished(t, id)
		if published {
			t.Fatal("outbox was marked published while NATS was unavailable")
		}
		if job["status"] == "queued" && found {
			queued = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !queued {
		t.Fatalf("job was not left queued, status=%v", env.get(t, id)["status"])
	}
	time.Sleep(700 * time.Millisecond)
	if published, _ := env.outboxPublished(t, id); published {
		t.Fatal("outbox was marked published while NATS was still unavailable")
	}

	env.natsProxy.reject(false)
	env.waitStatus(t, id, "succeeded", 30*time.Second)
	if env.rec.count(uuid.MustParse(id)) != 1 {
		t.Fatalf("sender calls = %d", env.rec.count(uuid.MustParse(id)))
	}
	published, found := env.outboxPublished(t, id)
	if !found || !published {
		t.Fatal("outbox was not marked published after NATS recovered")
	}
}

const itestAckWait = 2 * time.Second

func TestTwoWorkersRaceBeforeSendStarted(t *testing.T) {
	env.fault.markSleep.Store(int64(3 * time.Second))
	t.Cleanup(func() { env.fault.markSleep.Store(0) })
	body := jobBody("race-"+uuid.NewString(), "user-race", time.Now().Add(-time.Second), "race")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)

	var owner string
	published := false
	beforeA := env.runnerBy["worker-a"].Stats.NotOwnerRejects.Load()
	beforeB := env.runnerBy["worker-b"].Stats.NotOwnerRejects.Load()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		job := env.get(t, id)
		if owner == "" {
			owner, _ = job["worker_id"].(string)
		}
		if owner != "" && job["status"] == "processing" && job["send_started_at"] == nil && !published {
			if _, err := env.js.Publish(natspub.Subject, []byte(id), nats.MsgId("race-force-"+id)); err != nil {
				t.Fatal(err)
			}
			published = true
		}
		other := "worker-b"
		beforeOther := beforeB
		if owner == "worker-b" {
			other = "worker-a"
			beforeOther = beforeA
		}
		if published && owner != "" && env.runnerBy[other].Stats.NotOwnerRejects.Load()-beforeOther >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !published {
		t.Fatal("job never entered processing before send_started_at was recorded")
	}
	other := "worker-b"
	beforeOther := beforeB
	if owner == "worker-b" {
		other = "worker-a"
		beforeOther = beforeA
	}
	if env.runnerBy[other].Stats.NotOwnerRejects.Load()-beforeOther < 1 {
		t.Fatalf("%s did not reject a foreign job before send_started_at was recorded", other)
	}

	env.waitStatus(t, id, "succeeded", 45*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
}

func TestFinishOutlastsAckWaitWithoutResending(t *testing.T) {
	env.fault.failFinish.Store(true)
	t.Cleanup(func() { env.fault.failFinish.Store(false) })
	body := jobBody("ackwait-"+uuid.NewString(), "user-ackwait", time.Now().Add(-time.Second), "finish")
	_, id, err := env.post(body)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse(id)

	slowStart := time.Now()
	deadline := time.Now().Add(20 * time.Second)
	var job map[string]any
	for time.Now().Before(deadline) {
		job = env.get(t, id)
		if job["send_started_at"] != nil && env.rec.count(jobID) == 1 && job["status"] == "processing" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if env.rec.count(jobID) != 1 || job["status"] != "processing" || job["send_started_at"] == nil {
		t.Fatalf("expected one send and processing before finish: %+v calls=%d", job, env.rec.count(jobID))
	}
	redeliveredBefore := env.consumerRedelivered()
	if redeliveredBefore < 0 {
		t.Fatal("could not read consumer redelivery count from the broker")
	}

	for time.Since(slowStart) < itestAckWait+time.Second {
		time.Sleep(100 * time.Millisecond)
	}
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls after ack wait = %d", env.rec.count(jobID))
	}
	if extra := env.consumerRedelivered() - redeliveredBefore; extra < 0 {
		t.Fatal("could not read consumer redelivery count from the broker")
	} else if extra != 0 {
		t.Fatalf("broker redelivered during in-progress finish: %d", extra)
	}
	job = env.get(t, id)
	if job["status"] != "processing" {
		t.Fatalf("result was saved before finish recovered: %+v", job)
	}

	env.fault.failFinish.Store(false)
	env.waitStatus(t, id, "succeeded", 30*time.Second)
	if env.rec.count(jobID) != 1 {
		t.Fatalf("sender calls = %d", env.rec.count(jobID))
	}
	if extra := env.consumerRedelivered() - redeliveredBefore; extra < 0 {
		t.Fatal("could not read consumer redelivery count from the broker")
	} else if extra != 0 {
		t.Fatalf("broker redelivered while saving result: %d", extra)
	}
}

func TestTwoWorkersShareTheConsumer(t *testing.T) {
	ids := make([]string, 0, 24)
	for range 24 {
		body := jobBody("batch-"+uuid.NewString(), "user-batch", time.Now().Add(-time.Second), "batch")
		_, id, err := env.post(body)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	seen := map[string]int{}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		seen = map[string]int{}
		done := 0
		for _, id := range ids {
			job := env.get(t, id)
			if job["status"] == "succeeded" {
				done++
				if workerID, _ := job["worker_id"].(string); workerID != "" {
					seen[workerID]++
				}
			}
		}
		if done == len(ids) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(ids) != countSucceeded(t, ids) {
		t.Fatalf("succeeded %d of %d", countSucceeded(t, ids), len(ids))
	}
	if seen["worker-a"] == 0 || seen["worker-b"] == 0 {
		t.Fatalf("worker distribution = %v", seen)
	}
}

var env *environment

type environment struct {
	api       string
	db        *pgxpool.Pool
	direct    *postgres.Store
	workers   *postgres.Store
	fault     *faultStore
	dbProxy   *proxy
	natsProxy *proxy
	js        nats.JetStreamContext
	nc        *nats.Conn
	rec       *recorder
	runners   []*worker.Runner
	runnerBy  map[string]*worker.Runner
	cancel    context.CancelFunc
	pg        *pgmod.PostgresContainer
	nats      *tcnats.NATSContainer
}

func startEnv(ctx context.Context) (*environment, error) {
	pg, err := pgmod.Run(ctx, "postgres:17.11-alpine",
		pgmod.WithDatabase("scheduler"),
		pgmod.WithUsername("scheduler"),
		pgmod.WithPassword("scheduler"),
	)
	if err != nil {
		return nil, err
	}
	natsContainer, err := tcnats.Run(ctx, "nats:2.15.2-alpine")
	if err != nil {
		_ = pg.Terminate(context.Background())
		return nil, err
	}
	e := &environment{pg: pg, nats: natsContainer}
	ok := false
	defer func() {
		if !ok {
			e.close()
		}
	}()

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, err
	}
	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		return nil, err
	}
	dbProxy, err := newProxy(mustHost(dsn))
	if err != nil {
		return nil, err
	}
	natsProxy, err := newProxy(mustHost(natsURL))
	if err != nil {
		return nil, err
	}
	e.dbProxy = dbProxy
	e.natsProxy = natsProxy

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := postgres.Migrate(ctx, dsn); err != nil {
		return nil, err
	}
	direct, err := postgres.Connect(ctx, dsn, log)
	if err != nil {
		return nil, err
	}
	workerDB, err := postgres.Connect(ctx, replaceHost(dsn, dbProxy.addr()), log)
	if err != nil {
		return nil, err
	}
	fault := &faultStore{Store: workerDB}
	e.fault = fault
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	brokerURL := replaceHost(natsURL, natsProxy.addr())
	bus, err := natspub.Connect(ctx, brokerURL, "itest-controller", log)
	if err != nil {
		return nil, err
	}
	if err := bus.Ensure(ctx, itestAckWait, worker.MaxDeliver); err != nil {
		return nil, err
	}
	nc, err := nats.Connect(brokerURL, nats.MaxReconnects(-1), nats.ReconnectWait(200*time.Millisecond))
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	appCtx, cancel := context.WithCancel(context.Background())
	rec := &recorder{}
	runnerBy := map[string]*worker.Runner{}
	var runners []*worker.Runner
	for _, id := range []string{"worker-a", "worker-b"} {
		wbus, err := natspub.Connect(ctx, brokerURL, id, log)
		if err != nil {
			cancel()
			return nil, err
		}
		runner := worker.New(log, fault, recordingSender{workerID: id, rec: rec}, id)
		runner.SetDelays(300*time.Millisecond, nil)
		runner.SetProgressInterval(itestAckWait / 3)
		runnerBy[id] = runner
		go func() { _ = runner.Run(appCtx, appCtx, wbus.Pull) }()
		runners = append(runners, runner)
	}
	go controller.RunScheduler(appCtx, log, direct, 200*time.Millisecond, 50)
	go controller.RunOutbox(appCtx, log, direct, bus, 100*time.Millisecond, 50)
	server := httptest.NewServer(controller.NewAPI(log, direct).Handler())

	e.api = server.URL
	e.db = pool
	e.direct = direct
	e.workers = workerDB
	e.js = js
	e.nc = nc
	e.rec = rec
	e.runners = runners
	e.runnerBy = runnerBy
	e.cancel = func() {
		cancel()
		server.Close()
		bus.Close()
		nc.Close()
	}
	env = e
	ok = true
	return e, nil
}

func (e *environment) close() {
	if e == nil {
		return
	}
	if e.cancel != nil {
		e.cancel()
	}
	if e.direct != nil {
		e.direct.Close()
	}
	if e.workers != nil {
		e.workers.Close()
	}
	if e.db != nil {
		e.db.Close()
	}
	if e.dbProxy != nil {
		e.dbProxy.close()
	}
	if e.natsProxy != nil {
		e.natsProxy.close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if e.pg != nil {
		_ = e.pg.Terminate(ctx)
	}
	if e.nats != nil {
		_ = e.nats.Terminate(ctx)
	}
}

func (e *environment) naks() int64 {
	var n int64
	for _, runner := range e.runners {
		n += runner.Stats.DelayedNak.Load()
	}
	return n
}

func (e *environment) post(body string) (int, string, error) {
	res, err := http.Post(e.api+"/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, "", err
	}
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	id, _ := payload["id"].(string)
	if res.StatusCode != http.StatusCreated && res.StatusCode != http.StatusOK && res.StatusCode != http.StatusConflict {
		return res.StatusCode, id, fmt.Errorf("status %d body %s", res.StatusCode, raw)
	}
	return res.StatusCode, id, nil
}

func (e *environment) get(t *testing.T, id string) map[string]any {
	t.Helper()
	res, err := http.Get(e.api + "/jobs/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("get %s: %d %v", id, res.StatusCode, payload)
	}
	return payload
}

func (e *environment) waitStatus(t *testing.T, id, status string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]any
	for time.Now().Before(deadline) {
		last = e.get(t, id)
		if last["status"] == status {
			return last
		}
		if last["status"] == "failed" && status != "failed" {
			t.Fatalf("job %s failed: %v", id, last["last_error"])
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s status=%v want %s", id, last["status"], status)
	return nil
}

func (e *environment) outboxPublished(t *testing.T, id string) (published bool, found bool) {
	t.Helper()
	var at *time.Time
	err := e.db.QueryRow(context.Background(), `SELECT published_at FROM outbox WHERE job_id = $1::uuid`, id).Scan(&at)
	if err != nil {
		return false, false
	}
	return at != nil, true
}

func (e *environment) consumerRedelivered() int {
	for range 50 {
		if e.nc != nil {
			if js, err := e.nc.JetStream(); err == nil {
				e.js = js
			}
		}
		info, err := e.js.ConsumerInfo(natspub.Stream, natspub.Durable)
		if err == nil {
			return info.NumRedelivered
		}
		time.Sleep(200 * time.Millisecond)
	}
	return -1
}

func (e *environment) canceledTerminalAcks() int64 {
	var n int64
	for _, runner := range e.runners {
		n += runner.Stats.CanceledTerminalAcks.Load()
	}
	return n
}

func (e *environment) consumerSeq(t *testing.T) uint64 {
	t.Helper()
	info, err := e.js.ConsumerInfo(natspub.Stream, natspub.Durable)
	if err != nil {
		t.Fatal(err)
	}
	return info.Delivered.Consumer
}

func countSucceeded(t *testing.T, ids []string) int {
	t.Helper()
	n := 0
	for _, id := range ids {
		if env.get(t, id)["status"] == "succeeded" {
			n++
		}
	}
	return n
}

func jobBody(key, recipient string, at time.Time, subject string) string {
	raw, _ := json.Marshal(map[string]any{
		"idempotency_key": key,
		"recipient_id":    recipient,
		"scheduled_at":    domain.FormatTime(at),
		"sender":          "mock",
		"payload":         map[string]string{"to": "user@example.com", "subject": subject, "body": "hello"},
	})
	return string(raw)
}

func keyFrom(t *testing.T, body string) string {
	t.Helper()
	var payload struct {
		Key string `json:"idempotency_key"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	return payload.Key
}

func mustTime(t *testing.T, v any) time.Time {
	t.Helper()
	s, _ := v.(string)
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

type faultStore struct {
	*postgres.Store
	failMark         atomic.Bool
	failFinish       atomic.Bool
	loseMarkResponse atomic.Bool
	markSleep        atomic.Int64
	pauseClaim       atomic.Bool
}

func (s *faultStore) Claim(ctx context.Context, id uuid.UUID, workerID string) (domain.Job, bool, bool, error) {
	for s.pauseClaim.Load() {
		select {
		case <-ctx.Done():
			return domain.Job{}, false, false, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return s.Store.Claim(ctx, id, workerID)
}

func (s *faultStore) MarkSendStarted(ctx context.Context, id, attempt uuid.UUID) (time.Time, error) {
	if delay := time.Duration(s.markSleep.Load()); delay > 0 {
		time.Sleep(delay)
	}
	if s.failMark.Load() {
		return time.Time{}, dbDown()
	}
	started, err := s.Store.MarkSendStarted(ctx, id, attempt)
	if err != nil {
		return started, err
	}
	if s.loseMarkResponse.CompareAndSwap(true, false) {
		return started, dbDown()
	}
	return started, nil
}

func (s *faultStore) Finish(ctx context.Context, id uuid.UUID, status domain.Status, lastError *string) error {
	if s.failFinish.Load() {
		return dbDown()
	}
	return s.Store.Finish(ctx, id, status, lastError)
}

func dbDown() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

type recordingSender struct {
	workerID string
	rec      *recorder
}

func (s recordingSender) Send(_ context.Context, job sender.Job) error {
	s.rec.add(job.ID, s.workerID)
	time.Sleep(30 * time.Millisecond)
	return nil
}

type recorder struct {
	mu    sync.Mutex
	calls []uuid.UUID
}

func (r *recorder) add(id uuid.UUID, _ string) {
	r.mu.Lock()
	r.calls = append(r.calls, id)
	r.mu.Unlock()
}

func (r *recorder) count(id uuid.UUID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, call := range r.calls {
		if call == id {
			n++
		}
	}
	return n
}

type proxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	closed bool
	alive  map[net.Conn]struct{}
}

func newProxy(target string) (*proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{ln: ln, target: target, alive: map[net.Conn]struct{}{}}
	go p.serve()
	return p, nil
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

func (p *proxy) serve() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		closed := p.closed
		if !closed {
			p.alive[conn] = struct{}{}
		}
		p.mu.Unlock()
		if closed {
			conn.Close()
			continue
		}
		go p.forward(conn)
	}
}

func (p *proxy) forward(client net.Conn) {
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		p.forget(client)
		client.Close()
		return
	}
	p.mu.Lock()
	if !p.closed {
		p.alive[upstream] = struct{}{}
	}
	reject := p.closed
	p.mu.Unlock()
	if reject {
		client.Close()
		upstream.Close()
		return
	}
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, client)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, upstream)
		done <- struct{}{}
	}()
	<-done
	p.forget(client)
	p.forget(upstream)
	client.Close()
	upstream.Close()
}

func (p *proxy) reject(on bool) {
	p.mu.Lock()
	p.closed = on
	var conns []net.Conn
	if on {
		for conn := range p.alive {
			conns = append(conns, conn)
		}
		p.alive = map[net.Conn]struct{}{}
	}
	p.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}

func (p *proxy) forget(conn net.Conn) {
	p.mu.Lock()
	delete(p.alive, conn)
	p.mu.Unlock()
}

func (p *proxy) close() {
	_ = p.ln.Close()
	p.reject(true)
}

func mustHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return parsed.Host
}

func replaceHost(rawURL, host string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	parsed.Host = host
	return parsed.String()
}
