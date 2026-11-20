package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"snorlx/backend/internal/models"
	"snorlx/backend/internal/scorer"

	gh "github.com/google/go-github/v93/github"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
)

// Repository sync comes in two kinds.
//
// A manual sync (the Sync button) is a full pass: every repository, its workflows, its newest
// runs and its score are fetched again, and its progress is pushed to the browser.
//
// An automatic sync runs in the background for users who are watching, every AUTO_SYNC_INTERVAL.
// It is incremental and silent: repositories and workflows are listed with conditional requests
// (a 304 costs no rate-limit budget), runs are only stored when the newest page changed, and a
// score is recomputed only when it is old or the repository was pushed to since. A repository
// that is new to the instance gets the full treatment. When it stored something, the user's open
// views are told to refetch.

const (
	// autoSyncCheckEvery is how often the background loop looks for users whose sync is due.
	autoSyncCheckEvery = time.Minute
	// syncRunsPerRepo is how many of the newest runs a sync stores per repository.
	syncRunsPerRepo = 50
	// scoreMaxAge is how old a score may get before an incremental sync recomputes it.
	scoreMaxAge = 7 * 24 * time.Hour
	// scoreMinAge keeps a busy repository from being rescored on every incremental sync: a push
	// makes a score stale only once it is at least this old.
	scoreMinAge = 6 * time.Hour
)

func workflowsETagKey(repoID int) string { return fmt.Sprintf("workflows:%d", repoID) }
func syncRunsETagKey(repoID int) string  { return fmt.Sprintf("sync-runs:%d", repoID) }

// ===== Tracker =====

// syncStatus is the state of a user's current or most recent sync. It backs
// GET /api/repositories/sync/status, which the browser falls back to when WebSocket events are
// lost, and lets two syncs of one user never run at once.
type syncStatus struct {
	Running bool `json:"running"`
	// Manual is true for a sync the user started with the Sync button; only those show progress.
	Manual  bool   `json:"manual"`
	Total   int    `json:"total"`
	Synced  int    `json:"synced"`
	Current string `json:"current"`
	// Error is set when the most recent sync failed before it could store anything.
	Error string `json:"error,omitempty"`
}

type syncTracker struct {
	mu      sync.Mutex
	status  map[int]*syncStatus
	started map[int]time.Time
}

func newSyncTracker() *syncTracker {
	return &syncTracker{status: make(map[int]*syncStatus), started: make(map[int]time.Time)}
}

// begin marks a sync of userID as running. It returns false when one already runs.
func (t *syncTracker) begin(userID int, manual bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.beginLocked(userID, manual)
}

func (t *syncTracker) beginLocked(userID int, manual bool) bool {
	if st := t.status[userID]; st != nil && st.Running {
		return false
	}
	t.status[userID] = &syncStatus{Running: true, Manual: manual}
	t.started[userID] = time.Now()
	return true
}

// beginIfDue begins an automatic sync unless one runs or the last sync, of any kind, started less
// than interval ago.
func (t *syncTracker) beginIfDue(userID int, interval time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if at, ok := t.started[userID]; ok && time.Since(at) < interval {
		return false
	}
	return t.beginLocked(userID, false)
}

func (t *syncTracker) progress(userID, total, synced int, current string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st := t.status[userID]; st != nil {
		st.Total, st.Synced, st.Current = total, synced, current
	}
}

func (t *syncTracker) finish(userID int, errMsg string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st := t.status[userID]; st != nil {
		st.Running = false
		st.Error = errMsg
	}
}

func (t *syncTracker) get(userID int) syncStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st := t.status[userID]; st != nil {
		return *st
	}
	return syncStatus{}
}

// ===== Manual sync =====

// SyncRepositories starts a background full sync of repositories from GitHub.
// Returns immediately with 202 Accepted, progress is sent via WebSocket and is also readable from
// SyncStatus. A sync that is already running (manual or automatic) answers 409.
func (h *Handler) SyncRepositories(w http.ResponseWriter, r *http.Request) {
	user := h.getUserFromContext(r.Context())
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Copy what the background goroutine needs; the request context ends when the handler returns.
	userID := user.ID
	accessToken := user.AccessToken

	w.Header().Set("Content-Type", "application/json")
	if !h.syncs.begin(userID, true) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "already_running",
			"message": "A sync is already running",
		})
		return
	}

	// Return immediately - sync runs in background
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "started",
		"message": "Sync started, progress will be sent via WebSocket",
	})

	// Run sync in background; do not use r.Context(): it is cancelled when the handler returns (after 202),
	// which would immediately cancel the sync and trigger sync:error. Use Background so sync runs to completion.
	go h.runSync(context.Background(), userID, accessToken, false) // #nosec G118 -- intentional: sync must outlive the HTTP request
}

// SyncStatus reports the state of the caller's current or most recent sync.
func (h *Handler) SyncStatus(w http.ResponseWriter, r *http.Request) {
	user := h.getUserFromContext(r.Context())
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h.syncs.get(user.ID))
}

// ===== Automatic sync =====

// RunAutoSync keeps repositories, workflows and scores current for watching users until ctx is
// cancelled: a user whose last sync started more than interval ago gets an incremental one.
// Only users who already synced repositories qualify, so nobody's account is imported without
// them pressing Sync once.
func (h *Handler) RunAutoSync(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(autoSyncCheckEvery)
	defer ticker.Stop()
	for {
		h.autoSyncDueUsers(ctx, interval)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-h.wakeAutoSync:
		}
	}
}

func (h *Handler) autoSyncDueUsers(ctx context.Context, interval time.Duration) {
	for _, userID := range h.watchingUsers() {
		if ctx.Err() != nil {
			return
		}
		if !h.syncs.beginIfDue(userID, interval) {
			continue
		}
		user, err := h.storage.GetUserByID(ctx, userID)
		if err != nil || user == nil || user.AccessToken == "" {
			h.syncs.finish(userID, "no usable GitHub token")
			continue
		}
		if _, total, err := h.storage.ListRepositories(ctx, userID, 1, 1, ""); err != nil || total == 0 {
			h.syncs.finish(userID, "")
			continue
		}
		h.runSync(ctx, userID, user.AccessToken, true)
	}
}

// ===== Sync =====

// runSync performs a sync of userID's repositories in the background; the caller has already
// registered it with h.syncs. Every repository it stores is granted to userID, which is what makes
// it visible to that user afterwards. A manual (not incremental) sync reports its progress over the
// WebSocket; an incremental one only announces that it stored something.
func (h *Handler) runSync(ctx context.Context, userID int, accessToken string, incremental bool) {
	errMsg := ""
	defer func() { h.syncs.finish(userID, errMsg) }()

	// Create GitHub client with user's token
	token := &oauth2.Token{AccessToken: accessToken}
	client := h.ghClient.GetUserClient(ctx, token)

	// Fetch user's repositories from GitHub
	ghRepos, err := h.ghClient.ListUserRepositories(ctx, client)
	if err != nil {
		log.Error().Err(err).Bool("incremental", incremental).Msg("Failed to fetch repositories from GitHub")
		errMsg = "Failed to fetch repositories from GitHub"
		if !incremental {
			h.wsHub.SendSyncError(userID, errMsg)
		}
		return
	}

	// Apply filters from config
	ghRepos = h.filterRepositories(ghRepos)

	total := len(ghRepos)
	var syncedRepos, syncedWorkflows, syncedRuns int
	changed := false

	h.syncs.progress(userID, total, 0, "")
	if !incremental {
		h.wsHub.SendSyncStart(userID, total)
	}

	for i, ghRepo := range ghRepos {
		if ctx.Err() != nil {
			break
		}
		h.syncs.progress(userID, total, i, ghRepo.GetFullName())
		if !incremental {
			h.wsHub.SendSyncProgress(userID, i, total, ghRepo.GetFullName())
		}

		res := h.syncRepository(ctx, client, userID, ghRepo, incremental)
		if res.saved {
			syncedRepos++
		}
		syncedWorkflows += res.workflows
		syncedRuns += res.runs
		changed = changed || res.changed
	}

	log.Info().
		Int("user_id", userID).
		Bool("incremental", incremental).
		Int("repositories", syncedRepos).
		Int("workflows", syncedWorkflows).
		Int("runs", syncedRuns).
		Msg("Sync completed")

	if incremental {
		if changed {
			h.wsHub.SendSyncRefresh(userID)
		}
		return
	}
	h.wsHub.SendSyncComplete(userID, syncedRepos, syncedWorkflows, syncedRuns)
}

// repoSyncResult counts what syncRepository stored.
type repoSyncResult struct {
	saved     bool // the repository itself was stored and granted to the user
	workflows int
	runs      int
	// changed is true when anything visible to the user may differ from before.
	changed bool
}

// syncRepository stores one repository with its workflows, newest runs and score. An incremental
// sync of a repository that is already stored asks GitHub conditionally and writes only what
// changed.
func (h *Handler) syncRepository(ctx context.Context, client *gh.Client, userID int, ghRepo *gh.Repository, incremental bool) repoSyncResult {
	var res repoSyncResult
	stored, lookupErr := h.storage.GetRepositoryByGitHubID(ctx, ghRepo.GetID())
	conditional := incremental && lookupErr == nil && stored != nil

	// Convert and save repository
	repo := &models.Repository{
		GitHubID:      ghRepo.GetID(),
		Name:          ghRepo.GetName(),
		FullName:      ghRepo.GetFullName(),
		DefaultBranch: ghRepo.GetDefaultBranch(),
		HTMLURL:       ghRepo.GetHTMLURL(),
		IsPrivate:     ghRepo.GetPrivate(),
		IsActive:      true,
	}
	if ghRepo.Description != nil {
		repo.Description = ghRepo.Description
	}

	savedRepo, err := h.storage.UpsertRepository(ctx, repo)
	if err != nil {
		log.Error().Err(err).Str("repo", ghRepo.GetFullName()).Msg("Failed to save repository")
		return res
	}
	if err := h.storage.GrantRepositoryAccess(ctx, userID, savedRepo.ID); err != nil {
		log.Error().Err(err).Str("repo", ghRepo.GetFullName()).Msg("Failed to grant repository access")
		return res
	}
	res.saved = true
	res.changed = !conditional

	owner := ghRepo.GetOwner().GetLogin()
	repoName := ghRepo.GetName()

	// Fetch and save workflows for this repository
	var ghWorkflows []*gh.Workflow
	workflowsKey := workflowsETagKey(savedRepo.ID)
	workflowsETag := ""
	workflowsChanged := true
	if conditional {
		ghWorkflows, workflowsETag, workflowsChanged, err = h.ghClient.ListWorkflowsIfChanged(ctx, client, owner, repoName, h.etags.get(workflowsKey))
	} else {
		ghWorkflows, err = h.ghClient.ListWorkflows(ctx, client, owner, repoName)
	}
	if err != nil {
		log.Warn().Err(err).Str("repo", ghRepo.GetFullName()).Msg("Failed to fetch workflows")
		return res
	}

	var savedWorkflows []models.Workflow
	allWorkflowsSaved := true
	if workflowsChanged {
		res.changed = true
		savedWorkflows = make([]models.Workflow, 0, len(ghWorkflows))
		for _, ghWorkflow := range ghWorkflows {
			savedWorkflow, err := h.storage.UpsertWorkflow(ctx, workflowFromGitHub(ghWorkflow, savedRepo.ID))
			if err != nil {
				log.Error().Err(err).Str("workflow", ghWorkflow.GetName()).Msg("Failed to save workflow")
				allWorkflowsSaved = false
				continue
			}
			res.workflows++
			savedWorkflows = append(savedWorkflows, *savedWorkflow)
		}
		// The ETag is remembered only once every workflow is stored, so a failed write makes the
		// next sync list the repository again.
		if conditional && allWorkflowsSaved {
			h.etags.set(workflowsKey, workflowsETag)
		}
	} else {
		// Nothing to write, but the runs below need the stored workflows to find theirs.
		savedWorkflows, err = h.storage.ListWorkflows(ctx, userID, &savedRepo.ID)
		if err != nil {
			log.Warn().Err(err).Str("repo", ghRepo.GetFullName()).Msg("Failed to list stored workflows")
			return res
		}
	}

	// Fetch and save workflow runs for this repository (limit to 50 recent runs for faster sync)
	var ghRuns []*gh.WorkflowRun
	runsKey := syncRunsETagKey(savedRepo.ID)
	runsETag := ""
	runsChanged := true
	if conditional {
		ghRuns, runsETag, runsChanged, err = h.ghClient.ListRecentWorkflowRunsIfChanged(ctx, client, owner, repoName, syncRunsPerRepo, h.etags.get(runsKey))
	} else {
		ghRuns, err = h.ghClient.ListWorkflowRuns(ctx, client, owner, repoName, nil, syncRunsPerRepo)
	}
	if err != nil {
		log.Warn().Err(err).Str("repo", ghRepo.GetFullName()).Msg("Failed to fetch workflow runs")
		return res
	}

	if runsChanged {
		workflows := indexWorkflows(savedWorkflows)
		allRunsSaved := true
		for _, ghRun := range ghRuns {
			run, ok := h.runFromGitHub(ghRun, savedRepo.ID, workflows)
			if !ok {
				continue
			}
			if _, err := h.storage.UpsertRun(ctx, run); err != nil {
				log.Error().Err(err).Int64("run_id", ghRun.GetID()).Msg("Failed to save workflow run")
				allRunsSaved = false
				continue
			}
			res.runs++
		}
		res.changed = true
		if conditional && allRunsSaved {
			h.etags.set(runsKey, runsETag)
		}
	}

	// Score repository (documentation, security, CI/CD, etc.)
	if h.scorer != nil && (!incremental || h.scoreIsStale(ctx, savedRepo.ID, ghRepo)) {
		meta := &scorer.RepoMeta{
			HasWorkflows:  len(savedWorkflows) > 0,
			WorkflowNames: make([]string, 0, len(savedWorkflows)),
			Topics:        ghRepo.Topics,
		}
		if ghRepo.PushedAt != nil {
			t := ghRepo.PushedAt.Time
			meta.PushedAt = &t
		}
		meta.Archived = ghRepo.GetArchived()
		for _, w := range savedWorkflows {
			meta.WorkflowNames = append(meta.WorkflowNames, w.Name)
		}
		score, err := h.scorer.ScoreRepository(ctx, client, owner, repoName, savedRepo, meta)
		if err != nil {
			log.Warn().Err(err).Str("repo", ghRepo.GetFullName()).Msg("Failed to score repository")
		} else if _, err := h.storage.UpsertRepositoryScore(ctx, score); err != nil {
			log.Error().Err(err).Str("repo", ghRepo.GetFullName()).Msg("Failed to save repository score")
		} else {
			res.changed = true
		}
	}
	return res
}

// scoreIsStale reports whether an incremental sync should recompute the score of a repository:
// it has none, it is older than scoreMaxAge, or the repository was pushed to since it was taken
// and it is at least scoreMinAge old.
func (h *Handler) scoreIsStale(ctx context.Context, repoID int, ghRepo *gh.Repository) bool {
	score, err := h.storage.GetLatestRepositoryScore(ctx, repoID)
	if err != nil || score == nil {
		return true
	}
	age := time.Since(score.ScannedAt)
	if age > scoreMaxAge {
		return true
	}
	return age > scoreMinAge && ghRepo.PushedAt != nil && ghRepo.PushedAt.After(score.ScannedAt)
}
