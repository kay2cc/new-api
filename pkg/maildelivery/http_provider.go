package maildelivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/mail"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
)

// Provider hosts are fixed by adapters; configuration cannot redirect credentials.
var providerHTTPClient = &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

type HTTPProvider struct{ Kind string }

var errNotSubmitted = errors.New("mail connection failed before submission")

func (p HTTPProvider) request(ctx context.Context, c Credentials, method, path string, body any) (int, []byte, error) {
	hosts := map[string]string{"resend": "https://api.resend.com", "brevo": "https://api.brevo.com/v3", "smtp2go": "https://api.smtp2go.com/v3"}
	host := hosts[p.Kind]
	if host == "" || c.APIKey == "" {
		return 0, nil, fmt.Errorf("mail provider credentials missing")
	}
	attemptID := ""
	if envelope, ok := body.(sendEnvelope); ok {
		body = envelope.Body
		attemptID = envelope.AttemptID
	}
	var data []byte
	var err error
	if body != nil {
		data, err = common.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, host+path, bytes.NewReader(data))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	switch p.Kind {
	case "resend":
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	case "brevo":
		req.Header.Set("api-key", c.APIKey)
	case "smtp2go":
		req.Header.Set("X-Smtp2go-Api-Key", c.APIKey)
	}
	if p.Kind == "resend" && attemptID != "" {
		req.Header.Set("Idempotency-Key", attemptID)
	}
	// A connection failure before any connection was acquired cannot have
	// submitted the message. Errors after a connection existed remain ambiguous,
	// including errors on a later automatic transport retry.
	var connected atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.Store(true) }}))
	resp, err := providerHTTPClient.Do(req)
	if err != nil {
		if !connected.Load() {
			return 0, nil, errNotSubmitted
		}
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	return resp.StatusCode, raw, err
}

type sendEnvelope struct {
	Body      any
	AttemptID string
}

func (p HTTPProvider) Send(ctx context.Context, c Credentials, ch model.MailChannel, m Message, attemptID string) Result {
	from := (&mail.Address{Name: ch.FromName, Address: ch.FromAddress}).String()
	var payload any
	var path string
	switch p.Kind {
	case "resend":
		path = "/emails"
		payload = map[string]any{"from": from, "to": m.To, "subject": m.Subject, "html": m.HTML}
	case "brevo":
		to := make([]map[string]string, 0, len(m.To))
		for _, a := range m.To {
			to = append(to, map[string]string{"email": a})
		}
		path = "/smtp/email"
		payload = map[string]any{"sender": map[string]string{"name": ch.FromName, "email": ch.FromAddress}, "to": to, "subject": m.Subject, "htmlContent": m.HTML, "tags": []string{attemptID}}
	case "smtp2go":
		path = "/email/send"
		payload = map[string]any{"sender": from, "to": m.To, "subject": m.Subject, "html_body": m.HTML, "fastaccept": false}
	default:
		return Result{State: "rejected", Code: "unsupported_provider"}
	}
	status, raw, err := p.request(ctx, c, http.MethodPost, path, sendEnvelope{Body: payload, AttemptID: attemptID})
	if err != nil {
		if errors.Is(err, errNotSubmitted) {
			return Result{State: "rejected", Code: "connection_failed"}
		}
		return Result{State: "unknown", Code: "transport_unknown"}
	}
	if status < 200 || status >= 300 {
		state := "rejected"
		if status >= 500 || status == 408 || p.Kind == "resend" && status == 409 {
			state = "unknown"
		}
		return Result{State: state, Code: "http_" + strconv.Itoa(status)}
	}
	var response struct {
		ID        string `json:"id"`
		MessageID string `json:"messageId"`
		Data      struct {
			Succeeded *int   `json:"succeeded"`
			Failed    *int   `json:"failed"`
			Failures  []any  `json:"failures"`
			EmailID   string `json:"email_id"`
		} `json:"data"`
	}
	if common.Unmarshal(raw, &response) != nil {
		return Result{State: "unknown", Code: "invalid_response"}
	}
	id := response.ID
	if p.Kind == "brevo" {
		id = response.MessageID
	}
	if p.Kind == "smtp2go" {
		id = response.Data.EmailID
		if response.Data.Succeeded == nil || response.Data.Failed == nil {
			return Result{State: "unknown", ID: id, Code: "missing_send_result"}
		}
		if *response.Data.Succeeded == 0 && *response.Data.Failed > 0 {
			return Result{State: "rejected", Code: "provider_rejected"}
		}
		if *response.Data.Succeeded != 1 || *response.Data.Failed != 0 || len(response.Data.Failures) > 0 {
			return Result{State: "unknown", ID: id, Code: "partial_result"}
		}
	}
	if id == "" {
		return Result{State: "unknown", Code: "missing_message_id"}
	}
	return Result{State: "accepted", ID: id}
}

func (p HTTPProvider) Lookup(ctx context.Context, c Credentials, a model.MailAttempt) Result {
	if p.Kind == "resend" && a.ProviderID != "" {
		status, raw, err := p.request(ctx, c, http.MethodGet, "/emails/"+url.PathEscape(a.ProviderID), nil)
		if err != nil || status != 200 {
			return Result{State: "pending", Code: "lookup_unavailable"}
		}
		var data struct {
			LastEvent string `json:"last_event"`
		}
		if common.Unmarshal(raw, &data) != nil {
			return Result{State: "pending", Code: "invalid_lookup"}
		}
		switch data.LastEvent {
		case "delivered", "opened", "clicked":
			return Result{State: "delivered"}
		case "bounced", "complained", "suppressed":
			return Result{State: "permanent", Code: "recipient_rejected"}
		case "failed", "canceled":
			return Result{State: "failed", Code: "delivery_failed"}
		}
	}
	if p.Kind == "brevo" {
		query := url.Values{"limit": {"50"}, "days": {"1"}, "sort": {"desc"}}
		if a.ProviderID != "" {
			query.Set("messageId", a.ProviderID)
		} else {
			tags, _ := common.Marshal([]string{a.ID})
			query.Set("tags", string(tags))
		}
		status, raw, err := p.request(ctx, c, http.MethodGet, "/smtp/statistics/events?"+query.Encode(), nil)
		if err != nil || status != 200 {
			return Result{State: "pending", Code: "lookup_unavailable"}
		}
		var data struct {
			Events []struct {
				Event     string `json:"event"`
				MessageID string `json:"messageId"`
				Tag       string `json:"tag"`
			} `json:"events"`
		}
		if common.Unmarshal(raw, &data) != nil {
			return Result{State: "pending", Code: "invalid_lookup"}
		}
		for _, event := range data.Events {
			// Do not treat unfiltered results as proof of this ambiguous submission.
			if a.ProviderID == "" && event.Tag != a.ID {
				continue
			}
			if a.ProviderID != "" && event.MessageID != a.ProviderID {
				continue
			}
			switch event.Event {
			case "delivered", "opened", "clicks":
				return Result{State: "delivered", ID: event.MessageID}
			case "hardBounces", "invalid", "spam", "unsubscribed":
				return Result{State: "permanent", ID: event.MessageID, Code: "recipient_rejected"}
			case "softBounces", "blocked", "error":
				return Result{State: "failed", ID: event.MessageID, Code: "delivery_failed"}
			case "requests":
				return Result{State: "accepted", ID: event.MessageID}
			}
		}
	}
	if p.Kind == "smtp2go" && a.ProviderID != "" {
		status, raw, err := p.request(ctx, c, http.MethodPost, "/activity/search", map[string]any{"search_email_id": a.ProviderID, "start_date": time.Unix(a.CreatedAt, 0).UTC().Add(-time.Minute).Format(time.RFC3339), "only_latest": true, "limit": 20})
		if err != nil || status != 200 {
			return Result{State: "pending", Code: "lookup_unavailable"}
		}
		var data struct {
			Data struct {
				Events []struct {
					EmailID string `json:"email_id"`
					Event   string `json:"event"`
				} `json:"events"`
			} `json:"data"`
		}
		if common.Unmarshal(raw, &data) == nil {
			for _, e := range data.Data.Events {
				if e.EmailID != a.ProviderID {
					continue
				}
				switch e.Event {
				case "delivered", "opened", "clicked":
					return Result{State: "delivered"}
				case "hard-bounced", "soft-bounced", "spam", "unsubscribed":
					return Result{State: "permanent", Code: "recipient_rejected"}
				case "rejected":
					return Result{State: "failed", Code: "delivery_failed"}
				case "processed":
					return Result{State: "accepted", ID: a.ProviderID}
				}
			}
		}
	}
	return Result{State: "pending"}
}

func (p HTTPProvider) Quotas(ctx context.Context, c Credentials) ([]QuotaObservation, error) {
	if p.Kind == "brevo" {
		status, raw, err := p.request(ctx, c, http.MethodGet, "/account", nil)
		if err != nil || status != 200 {
			return nil, quotaError("brevo")
		}
		var data struct {
			Plan []struct {
				Type        string  `json:"type"`
				CreditsType string  `json:"creditsType"`
				Credits     float64 `json:"credits"`
			} `json:"plan"`
			Relay struct {
				Enabled bool `json:"enabled"`
			} `json:"relay"`
		}
		if common.Unmarshal(raw, &data) != nil || !data.Relay.Enabled {
			return nil, quotaError("brevo")
		}
		for _, plan := range data.Plan {
			if plan.Type == "free" && plan.CreditsType == "sendLimit" && plan.Credits == float64(int64(plan.Credits)) {
				return []QuotaObservation{{Name: "daily", Capacity: 300, Remaining: int64(plan.Credits)}}, nil
			}
		}
		return nil, quotaError("brevo_plan") // Paid credits require an explicit adapter rule.
	}
	if p.Kind == "smtp2go" {
		status, raw, err := p.request(ctx, c, http.MethodPost, "/stats/email_cycle", map[string]any{})
		if err != nil || status != 200 {
			return nil, quotaError("smtp2go")
		}
		var data struct {
			Data struct {
				Start     string `json:"cycle_start"`
				End       string `json:"cycle_end"`
				Remaining *int64 `json:"cycle_remaining"`
				Max       *int64 `json:"cycle_max"`
			} `json:"data"`
		}
		if common.Unmarshal(raw, &data) != nil || data.Data.Remaining == nil || data.Data.Max == nil {
			return nil, quotaError("smtp2go")
		}
		var end time.Time
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05.999999Z07:00"} {
			end, err = time.Parse(layout, strings.TrimSpace(data.Data.End))
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, quotaError("smtp2go_cycle_timezone")
		}
		return []QuotaObservation{{Name: "monthly", Capacity: *data.Data.Max, Remaining: *data.Data.Remaining, Period: data.Data.Start + "/" + data.Data.End, PeriodEnd: end.Unix()}}, nil
	}
	return nil, quotaError("unsupported")
}
