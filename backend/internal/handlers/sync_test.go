package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"snorlx/backend/internal/models"

	gh "github.com/google/go-github/v93/github"
)

func TestSyncTracker_OneSyncPerUserAndDueInterval(t *testing.T) {
	tr := newSyncTracker()

	if !tr.begin(1, true) {
		t.Fatal("first sync must start")
	}
	if tr.begin(1, false) {
		t.Fatal("a second sync of the same user must be refused while one runs")
	}
	if !tr.begin(2, false) {
		t.Fatal("another user is independent")
	}

	tr.progress(1, 10, 3, "acme/app")
	st := tr.get(1)
	if !st.Running || !st.Manual || st.Total != 10 || st.Synced != 3 || st.Current != "acme/app" {
		t.Fatalf("unexpected status %+v", st)
	}

	tr.finish(1, "boom")
	st = tr.get(1)
	if st.Running || st.Error != "boom" {
		t.Fatalf("expected a finished status carrying the error, got %+v", st)
	}

	if tr.beginIfDue(1, time.Hour) {
		t.Fatal("a sync that started moments ago must not be due again")
	}
	if !tr.beginIfDue(3, time.Hour) {
		t.Fatal("a user that never synced is due")
	}
	tr.finish(3, "")
	if !tr.beginIfDue(3, 0) {
		t.Fatal("a zero interval is always due once the previous sync finished")
	}
	if tr.beginIfDue(3, 0) {
		t.Fatal("a running sync is never due")
	}
	if got := tr.get(99); got.Running || got.Total != 0 {
		t.Fatalf("unknown user must report an idle status, got %+v", got)
	}
}

func TestSyncRepositories_ConflictWhileRunning(t *testing.T) {
	h := newTestHandler(&mockStorage{})
	if !h.syncs.begin(1, false) {
		t.Fatal("setup: begin")
	}

	req := withUser(httptest.NewRequest(http.MethodPost, "/api/repositories/sync", nil), &models.User{ID: 1, AccessToken: "tok"})
	rec := httptest.NewRecorder()
	h.SyncRepositories(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 while a sync runs, got %d", rec.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["status"] != "already_running" {
		t.Fatalf("unexpected body %q (%v)", rec.Body.String(), err)
	}
}

func TestSyncStatus_ReportsProgressOfTheCaller(t *testing.T) {
	h := newTestHandler(&mockStorage{})
	h.syncs.begin(1, true)
	h.syncs.progress(1, 4, 2, "acme/api")
	h.syncs.begin(2, true)
	h.syncs.progress(2, 99, 98, "other/repo")

	req := withUser(httptest.NewRequest(http.MethodGet, "/api/repositories/sync/status", nil), &models.User{ID: 1})
	rec := httptest.NewRecorder()
	h.SyncStatus(rec, req)

	var st syncStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !st.Running || !st.Manual || st.Total != 4 || st.Synced != 2 || st.Current != "acme/api" {
		t.Fatalf("expected the caller's own progress, got %+v", st)
	}

	rec = httptest.NewRecorder()
	h.SyncStatus(rec, httptest.NewRequest(http.MethodGet, "/api/repositories/sync/status", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a user, got %d", rec.Code)
	}
}

func TestScoreIsStale(t *testing.T) {
	pushed := func(ago time.Duration) *gh.Repository {
		ts := gh.Timestamp{Time: time.Now().Add(-ago)}
		return &gh.Repository{PushedAt: &ts}
	}
	scoreAged := func(age time.Duration) func(context.Context, int) (*models.RepositoryScore, error) {
		return func(context.Context, int) (*models.RepositoryScore, error) {
			return &models.RepositoryScore{ScannedAt: time.Now().Add(-age)}, nil
		}
	}

	cases := []struct {
		name  string
		score func(context.Context, int) (*models.RepositoryScore, error)
		repo  *gh.Repository
		want  bool
	}{
		{name: "never scored", score: nil, repo: pushed(48 * time.Hour), want: true},
		{name: "fresh and untouched", score: scoreAged(time.Hour), repo: pushed(48 * time.Hour), want: false},
		{name: "fresh score, pushed since, but too young to redo", score: scoreAged(time.Hour), repo: pushed(time.Minute), want: false},
		{name: "older score and pushed since", score: scoreAged(12 * time.Hour), repo: pushed(time.Hour), want: true},
		{name: "older score, not pushed since", score: scoreAged(12 * time.Hour), repo: pushed(24 * time.Hour), want: false},
		{name: "past max age", score: scoreAged(8 * 24 * time.Hour), repo: pushed(30 * 24 * time.Hour), want: true},
		{name: "unknown push time", score: scoreAged(12 * time.Hour), repo: &gh.Repository{}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(&mockStorage{getLatestScoreFunc: tc.score})
			if got := h.scoreIsStale(context.Background(), 7, tc.repo); got != tc.want {
				t.Errorf("scoreIsStale = %v, want %v", got, tc.want)
			}
		})
	}
}

// syncFixture serves one repository (acme/app) with one workflow and one run and records what the
// sync stores.
type syncFixture struct {
	api            *fakeGitHubAPI
	h              *Handler
	repoUpserts    atomic.Int32
	workflowSaves  atomic.Int32
	runSaves       atomic.Int32
	storedAlready  atomic.Bool
	workflowsError atomic.Bool
}

const userReposPath = "user/repos?affiliation=owner%2Ccollaborator%2Corganization_member&per_page=100&visibility=all"

func newSyncFixture(t *testing.T) *syncFixture {
	t.Helper()
	f := &syncFixture{api: newFakeGitHubAPI(t)}
	f.api.serve(userReposPath, `W/"repos-1"`, []map[string]interface{}{
		{"id": 1, "name": "app", "full_name": "acme/app", "default_branch": "main", "owner": map[string]interface{}{"login": "acme"}},
	})
	f.api.serve("repos/acme/app/actions/workflows?per_page=100", `W/"wf-1"`, map[string]interface{}{
		"total_count": 1,
		"workflows":   []map[string]interface{}{{"id": 30, "name": "CI", "path": ".github/workflows/ci.yml"}},
	})
	f.api.serve("repos/acme/app/actions/runs?per_page=50", `W/"runs-1"`, map[string]interface{}{
		"total_count":   1,
		"workflow_runs": []map[string]interface{}{{"id": 600, "workflow_id": 30, "status": "completed", "name": "CI", "run_started_at": "2026-10-05T11:00:00Z"}},
	})

	store := &mockStorage{
		getUserByIDFunc: tokenUser(1),
		getRepoByGitHubIDFunc: func(context.Context, int64) (*models.Repository, error) {
			if f.storedAlready.Load() {
				return &models.Repository{ID: 7, GitHubID: 1, FullName: "acme/app"}, nil
			}
			return nil, nil
		},
		upsertRepositoryFunc: func(_ context.Context, repo *models.Repository) (*models.Repository, error) {
			f.repoUpserts.Add(1)
			saved := *repo
			saved.ID = 7
			return &saved, nil
		},
		upsertWorkflowFunc: func(_ context.Context, w *models.Workflow) (*models.Workflow, error) {
			f.workflowSaves.Add(1)
			saved := *w
			saved.ID = 3
			return &saved, nil
		},
		listWorkflowsFunc: func(context.Context, int, *int) ([]models.Workflow, error) {
			return []models.Workflow{{ID: 3, GitHubID: 30, RepoID: 7, Name: "CI"}}, nil
		},
		upsertRunFunc: func(_ context.Context, run *models.WorkflowRun) (*models.WorkflowRun, error) {
			f.runSaves.Add(1)
			return run, nil
		},
		listRepositoriesFunc: func(context.Context, int, int, int, string) ([]models.Repository, int, error) {
			return []models.Repository{{ID: 7, FullName: "acme/app"}}, 1, nil
		},
	}
	f.h = newPollerHandler(t, store, f.api)
	return f
}

func (f *syncFixture) requestsTo(substr string) int {
	n := 0
	for _, p := range f.api.requestedPaths() {
		if strings.Contains(p, substr) {
			n++
		}
	}
	return n
}

func TestRunSync_IncrementalSkipsUnchangedRepositories(t *testing.T) {
	f := newSyncFixture(t)
	f.storedAlready.Store(true)
	ctx := context.Background()

	// First pass: nothing is cached yet, so workflows and runs are stored.
	f.h.syncs.begin(1, false)
	f.h.runSync(ctx, 1, "tok", true)
	if f.workflowSaves.Load() != 1 || f.runSaves.Load() != 1 {
		t.Fatalf("first pass should store the workflow and the run, stored %d and %d (requests %v)",
			f.workflowSaves.Load(), f.runSaves.Load(), f.api.requestedPaths())
	}
	if f.h.syncs.get(1).Running {
		t.Fatal("the sync must mark itself finished")
	}

	// Second pass: GitHub answers 304 for both lists, so nothing is written.
	f.h.syncs.begin(1, false)
	f.h.runSync(ctx, 1, "tok", true)
	if f.workflowSaves.Load() != 1 || f.runSaves.Load() != 1 {
		t.Fatalf("unchanged repository must not be written again, stored %d workflows and %d runs",
			f.workflowSaves.Load(), f.runSaves.Load())
	}

	// A new run on GitHub changes the runs ETag only: runs are stored, workflows are not.
	f.api.serve("repos/acme/app/actions/runs?per_page=50", `W/"runs-2"`, map[string]interface{}{
		"total_count": 2,
		"workflow_runs": []map[string]interface{}{
			{"id": 601, "workflow_id": 30, "status": "queued", "name": "CI", "run_started_at": "2026-10-05T12:00:00Z"},
			{"id": 600, "workflow_id": 30, "status": "completed", "name": "CI", "run_started_at": "2026-10-05T11:00:00Z"},
		},
	})
	f.h.syncs.begin(1, false)
	f.h.runSync(ctx, 1, "tok", true)
	if f.runSaves.Load() != 3 {
		t.Fatalf("the changed run list should be stored (1 + 2 runs), got %d", f.runSaves.Load())
	}
	if f.workflowSaves.Load() != 1 {
		t.Fatalf("unchanged workflows must stay untouched, got %d saves", f.workflowSaves.Load())
	}
}

func TestRunSync_NewRepositoryIsSyncedInFull(t *testing.T) {
	f := newSyncFixture(t) // storedAlready stays false: the repository is new to the instance

	f.h.syncs.begin(1, false)
	f.h.runSync(context.Background(), 1, "tok", true)

	if f.workflowSaves.Load() != 1 || f.runSaves.Load() != 1 {
		t.Fatalf("a new repository must get its workflows and runs, stored %d and %d (requests %v)",
			f.workflowSaves.Load(), f.runSaves.Load(), f.api.requestedPaths())
	}
	if f.h.etags.get(workflowsETagKey(7)) != "" || f.h.etags.get(syncRunsETagKey(7)) != "" {
		t.Error("the unconditional path must not record ETags for the conditional one")
	}
}

func TestRunSync_FullSyncAlwaysRefetchesAndReportsStatus(t *testing.T) {
	f := newSyncFixture(t)
	f.storedAlready.Store(true)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		f.h.syncs.begin(1, true)
		f.h.runSync(ctx, 1, "tok", false)
	}
	if f.workflowSaves.Load() != 2 || f.runSaves.Load() != 2 {
		t.Fatalf("a manual sync is a full pass every time, stored %d workflows and %d runs",
			f.workflowSaves.Load(), f.runSaves.Load())
	}
	st := f.h.syncs.get(1)
	if st.Running || st.Total != 1 || st.Error != "" {
		t.Fatalf("unexpected final status %+v", st)
	}
}

func TestRunSync_FailureIsReportedInStatus(t *testing.T) {
	f := newSyncFixture(t)
	f.api.fail(userReposPath, http.StatusInternalServerError)

	f.h.syncs.begin(1, true)
	f.h.runSync(context.Background(), 1, "tok", false)

	st := f.h.syncs.get(1)
	if st.Running || st.Error == "" {
		t.Fatalf("a failed sync must end with an error status, got %+v", st)
	}
	if f.h.syncs.begin(1, true) == false {
		t.Fatal("a failed sync must release the user's slot")
	}
}

func TestAutoSync_RunsOnlyForWatchingUsersWithSyncedRepositories(t *testing.T) {
	f := newSyncFixture(t)
	f.storedAlready.Store(true)
	ctx := context.Background()

	// Nobody is watching: nothing happens.
	f.h.autoSyncDueUsers(ctx, time.Hour)
	if f.api.requests.Load() != 0 {
		t.Fatal("no GitHub traffic is allowed without a watching user")
	}

	// A watching user is synced once, then left alone until the interval passes.
	f.h.watchers.touch(1)
	f.h.autoSyncDueUsers(ctx, time.Hour)
	first := f.api.requests.Load()
	if first == 0 {
		t.Fatal("a watching user with synced repositories must be synced")
	}
	f.h.autoSyncDueUsers(ctx, time.Hour)
	if f.api.requests.Load() != first {
		t.Fatal("a user synced moments ago must not be synced again")
	}
	if f.requestsTo("/user/repos") != 1 {
		t.Fatalf("expected one repository listing, got paths %v", f.api.requestedPaths())
	}
}

func TestAutoSync_SkipsUsersWhoNeverSynced(t *testing.T) {
	f := newSyncFixture(t)
	f.h.storage.(*mockStorage).listRepositoriesFunc = func(context.Context, int, int, int, string) ([]models.Repository, int, error) {
		return nil, 0, nil
	}
	f.h.watchers.touch(1)

	f.h.autoSyncDueUsers(context.Background(), time.Hour)

	if f.api.requests.Load() != 0 {
		t.Fatalf("an account is not imported until the user pressed Sync once, saw %v", f.api.requestedPaths())
	}
	if f.h.syncs.get(1).Running {
		t.Fatal("the skipped sync must release its slot")
	}
}
