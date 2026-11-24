// Package maildelivery provides database-backed mail routing without a settings UI.
package maildelivery

import (
	"context"
	"errors"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Message struct {
	Subject string
	To      []string
	HTML    string
}
type Credentials struct {
	APIKey   string `json:"api_key,omitempty"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	TLS      string `json:"tls,omitempty"`
}
type Result struct {
	State string // accepted, delivered, rejected, permanent, unknown, pending, failed
	ID    string
	Code  string // safe, bounded machine code; never raw provider messages
}

type QuotaObservation struct {
	Name      string
	Remaining int64
	Capacity  int64
	Period    string
	PeriodEnd int64
}

// Provider can be registered by future integrations without changing the scheduler.
type Provider interface {
	Send(context.Context, Credentials, model.MailChannel, Message, string) Result
	Lookup(context.Context, Credentials, model.MailAttempt) Result
	Quotas(context.Context, Credentials) ([]QuotaObservation, error)
}

type Dispatcher struct {
	DB        *gorm.DB
	Providers map[string]Provider
	ProcessID string
	Now       func() time.Time
	Valid     func(string) (bool, error)
}

func New(db *gorm.DB) *Dispatcher {
	d := &Dispatcher{DB: db, ProcessID: uuid.NewString(), Now: time.Now,
		Providers: map[string]Provider{"resend": HTTPProvider{Kind: "resend"}, "brevo": HTTPProvider{Kind: "brevo"}, "smtp2go": HTTPProvider{Kind: "smtp2go"}, "smtp": SMTPProvider{}}}
	d.Valid = d.validChallenge
	return d
}

var errUnavailable = errors.New("mail quota or channel unavailable")

func parseMessage(r common.EmailDeliveryRequest) (Message, error) {
	m := Message{Subject: r.Subject, HTML: r.Content}
	if strings.ContainsAny(r.Subject, "\r\n\x00") || len(r.Content) > 1024*1024 {
		return m, errors.New("invalid mail content")
	}
	for address := range strings.SplitSeq(r.Receiver, ";") {
		a, err := mail.ParseAddress(strings.TrimSpace(address))
		if err != nil || len(m.To) >= 50 {
			return m, errors.New("invalid mail recipient")
		}
		m.To = append(m.To, a.Address)
	}
	if len(m.To) == 0 {
		return m, errors.New("mail recipient required")
	}
	return m, nil
}

func (d *Dispatcher) validChallenge(tag string) (bool, error) {
	if !strings.HasPrefix(tag, "binding:") {
		return common.IsEmailVerificationCurrent(tag), nil
	}
	parts := strings.Split(tag, ":")
	if len(parts) != 3 {
		return false, nil
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return false, nil
	}
	var flow model.AuthFlow
	err = d.DB.First(&flow, "id = ? AND purpose = ?", id, model.AuthFlowPurposeEmailBinding).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if flow.ConsumedAt != nil || !flow.ExpiresAt.After(d.Now()) {
		return false, nil
	}
	var state model.EmailBindingState
	if common.UnmarshalJsonStr(flow.Payload, &state) != nil {
		return false, nil
	}
	return state.FailedAttempts < model.EmailBindingMaxAttempts && model.EmailBindingDeliveryTag(flow.Id, state) == tag, nil
}
