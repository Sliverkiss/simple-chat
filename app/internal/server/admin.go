// Admin account management API: upload, list, and remove accounts over the
// running service (belmo.io deployment model — container filesystems are
// ephemeral, the owner manages accounts via HTTP after deploy).
//
// All three endpoints sit behind the same optional DS_API_KEY as /v1
// (open access when unset — the owner's explicit decision). Responses are
// deliberately unredacted (full mobile, password, device_id): the surface
// is owner-only.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"simple-chat/internal/accountstore"
	"simple-chat/internal/upstream"
)

// adminBodyMaxBytes bounds the upload body: account records are tiny; 4 MiB
// accommodates a very large batch with headroom without inviting abuse.
const adminBodyMaxBytes = 4 << 20

// adminAccountRequest is one account in an upload body.
type adminAccountRequest struct {
	Mobile   string `json:"mobile"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Region   string `json:"region"`
	Channel  string `json:"channel"`
	DeviceID string `json:"device_id"`
}

// adminUploadRequest accepts {"accounts":[...]} or a single object (its
// fields parsed when no "accounts" key is present).
type adminUploadRequest struct {
	Accounts []adminAccountRequest `json:"accounts"`
	// Single-object convenience fields (used when "accounts" is absent).
	adminAccountRequest
}

// UnmarshalJSON implements the single-object convenience: a body without an
// "accounts" array is treated as one account.
func (r *adminUploadRequest) UnmarshalJSON(b []byte) error {
	type alias adminUploadRequest // avoid recursion
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*r = adminUploadRequest(a)
	if r.Accounts == nil {
		var single adminAccountRequest
		if err := json.Unmarshal(b, &single); err != nil {
			return err
		}
		r.Accounts = []adminAccountRequest{single}
	}
	return nil
}

// adminAccountResponse is the unredacted record echo: the full stored
// account plus the identity key.
type adminAccountResponse struct {
	upstream.Account
	// Identity is mobile, else email — the dedup/delete key.
	Identity string `json:"identity"`
}

// writeAdminError writes a plain JSON error (admin surface, not the OpenAI
// error envelope).
func writeAdminError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// writeAdminJSON writes a JSON payload.
func writeAdminJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// handleAdminUpload persists and hot-adds one or many accounts.
// Store-first ordering: every record is persisted via the active Store
// before any pool mutation; a store failure answers 500 with the pool
// untouched. A duplicate identity answers 409 with the existing record and
// persists/hot-adds nothing.
func (s *Server) handleAdminUpload(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "no account store configured (set DS_ACCOUNTS or DS_REDIS_HOST)")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyMaxBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "failed to read body")
		return
	}
	var req adminUploadRequest
	if len(body) >= adminBodyMaxBytes {
		// The LimitReader already truncated anything bigger — a parse of
		// truncated JSON would only produce a confusing error.
		writeAdminError(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeAdminError(w, http.StatusBadRequest, `invalid JSON body: expected {"accounts":[...]} or a single account object`)
		return
	}
	if len(req.Accounts) == 0 {
		writeAdminError(w, http.StatusBadRequest, "at least one account is required")
		return
	}

	// Validate every record first; one bad record rejects the batch (the
	// owner uploads a coherent set or gets a clean 400 naming the reason).
	accounts := make([]upstream.Account, 0, len(req.Accounts))
	for i, ra := range req.Accounts {
		a := upstream.Account{
			Mobile:   strings.TrimSpace(ra.Mobile),
			Email:    strings.TrimSpace(ra.Email),
			Password: ra.Password,
			Region:   strings.TrimSpace(ra.Region),
			Channel:  strings.TrimSpace(ra.Channel),
			DeviceID: strings.TrimSpace(ra.DeviceID),
		}
		if msg := validateAdminAccount(a); msg != "" {
			writeAdminError(w, http.StatusBadRequest, fmt.Sprintf("account %d: %s", i+1, msg))
			return
		}
		accounts = append(accounts, a)
	}

	// Duplicate check against the persisted records AND the batch itself:
	// same identity (mobile, else email) → 409 with the existing record.
	existing, err := s.store.Load(r.Context())
	if err != nil {
		// The store is the source of truth for the duplicate check; an
		// unreadable store must not degrade into a blind write.
		s.logger.Printf("admin upload: store load failed: %v", err)
		writeAdminError(w, http.StatusInternalServerError, "account store unavailable")
		return
	}
	known := make(map[string]upstream.Account, len(existing))
	for _, a := range existing {
		known[a.Identity()] = a
	}
	seen := map[string]bool{}
	for _, a := range accounts {
		id := a.Identity()
		if prev, ok := known[id]; ok {
			writeAdminJSON(w, http.StatusConflict, map[string]any{"error": "account already exists", "accounts": []adminAccountResponse{toAdminResponse(prev)}})
			return
		}
		if seen[id] {
			writeAdminError(w, http.StatusBadRequest, fmt.Sprintf("duplicate identity %q in batch", id))
			return
		}
		seen[id] = true
	}

	// Store-first: persist every record; on failure, roll back the ones
	// already written so the store and pool stay consistent.
	stored := make([]upstream.Account, 0, len(accounts))
	for _, a := range accounts {
		// Mint the device id now so the persisted record and the echo both
		// carry it (identical to the startup EnsureDeviceIDs behavior).
		if a.DeviceID == "" && a.Channel != "web" {
			a.DeviceID = upstream.ResolveDeviceID(a)
		}
		if err := s.store.SaveAccount(r.Context(), a); err != nil {
			s.logger.Printf("admin upload: store save %s failed: %v", a.Identity(), err)
			for _, prev := range stored {
				s.store.DeleteAccount(r.Context(), prev.Identity())
			}
			writeAdminError(w, http.StatusInternalServerError, "account store write failed")
			return
		}
		stored = append(stored, a)
	}
	// Pool mutation only after successful persistence.
	for _, a := range stored {
		if err := s.pool.AddAccount(a); err != nil {
			// Should be impossible (validated above), but never 500 the
			// whole batch over an already-persisted account — log and
			// continue; a restart reloads it from the store.
			s.logger.Printf("admin upload: pool add %s failed (persisted; picked up on restart): %v", a.Identity(), err)
		}
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{"accounts": toAdminResponses(stored)})
}

// validateAdminAccount checks one upload record and returns a rejection
// message ("" = valid). Mirrors upstream.Account.Validate but with
// owner-facing messages that name the rule and the valid values.
func validateAdminAccount(a upstream.Account) string {
	mobile := a.Mobile
	email := a.Email
	switch {
	case mobile != "" && email != "":
		return `exactly one of mobile/email must be set (got both)`
	case mobile == "" && email == "":
		return `exactly one of mobile/email must be set (got neither)`
	}
	if a.Password == "" {
		return "password must be a non-empty string"
	}
	if a.Region != "" && a.Region != "cn" {
		return fmt.Sprintf("unknown region %q (valid values: \"cn\")", a.Region)
	}
	switch a.Channel {
	case "", "android", "web":
	default:
		return fmt.Sprintf("unknown channel %q (valid values: \"\", \"android\", \"web\")", a.Channel)
	}
	if a.Channel == "web" && a.DeviceID == "" {
		return `channel "web" requires an explicit device_id (browser-harvested Shumei SMSdk id — see web-reverse-research.md §5)`
	}
	return ""
}

// handleAdminList reports every account with its live state and a summary.
// Reads come from the pool snapshot — the live truth — so the endpoint
// works even during a store outage (read-only, no persistence needed).
func (s *Server) handleAdminList(w http.ResponseWriter, r *http.Request) {
	snaps := s.pool.Snapshot()
	out := make([]map[string]any, 0, len(snaps))
	ready, parked := 0, 0
	for _, snap := range snaps {
		rec := map[string]any{
			"mobile":      snap.Account.Mobile,
			"email":       snap.Account.Email,
			"password":    snap.Account.Password,
			"region":      snap.Account.Region,
			"device_id":   snap.Account.DeviceID,
			"channel":     snap.Account.Channel,
			"state":       snap.State,
			"inflight":    snap.Inflight,
			"max_inflight": snap.MaxInflight,
			"token_warm":  snap.TokenWarm,
		}
		if snap.State == "ready" {
			ready++
		} else {
			parked++
			rec["park_kind"] = snap.ParkKind
			rec["park_until"] = snap.ParkUntil.UTC().Format(time.RFC3339)
			rec["park_reason"] = snap.ParkReason
		}
		out = append(out, rec)
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{
		"accounts": out,
		"summary": map[string]int{
			"ready":   ready,
			"parked":  parked,
			"total":   len(snaps),
		},
	})
}

// handleAdminDelete removes one account (id = mobile or email from the URL
// path). Store-first: the record is removed from the Store, then the pool
// slot is retired — in-flight leases finish naturally, no new acquisitions,
// no upstream logout. Unknown id → 404.
func (s *Server) handleAdminDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeAdminError(w, http.StatusBadRequest, "account id required")
		return
	}
	if s.store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "no account store configured (set DS_ACCOUNTS or DS_REDIS_HOST)")
		return
	}
	// Echo needs the full record; read it before deleting.
	snaps := s.pool.Snapshot()
	var removed *upstream.Account
	for _, snap := range snaps {
		if snap.Account.MatchesIdentity(id) {
			a := snap.Account
			removed = &a
			break
		}
	}
	if removed == nil {
		writeAdminError(w, http.StatusNotFound, "account not found")
		return
	}
	// Store-first: persisted removal before the pool stops selecting it.
	if err := s.store.DeleteAccount(r.Context(), id); err != nil {
		if errors.Is(err, accountstore.ErrAccountNotFound) {
			// In the pool but not the store (memory-only ParkStore config):
			// still retire it from rotation.
			s.logger.Printf("admin delete: %s not in store (pool-only); retiring from pool", id)
		} else {
			s.logger.Printf("admin delete: store delete %s failed: %v", id, err)
			writeAdminError(w, http.StatusInternalServerError, "account store delete failed")
			return
		}
	}
	s.pool.RemoveAccount(id)
	writeAdminJSON(w, http.StatusOK, map[string]any{"account": toAdminResponse(*removed)})
}

// toAdminResponse wraps a stored record for the unredacted echo.
func toAdminResponse(a upstream.Account) adminAccountResponse {
	return adminAccountResponse{Account: a, Identity: a.Identity()}
}

func toAdminResponses(accounts []upstream.Account) []adminAccountResponse {
	out := make([]adminAccountResponse, 0, len(accounts))
	for _, a := range accounts {
		out = append(out, toAdminResponse(a))
	}
	return out
}
