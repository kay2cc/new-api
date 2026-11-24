package maildelivery

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type fakeProvider struct {
	lookupFn     func(model.MailAttempt) Result
	send         func(Message, string) Result
	lookup       Result
	observations []QuotaObservation
	calls        int
	mu           sync.Mutex
}

func (p *fakeProvider) Send(_ context.Context, _ Credentials, _ model.MailChannel, m Message, id string) Result {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.send != nil {
		return p.send(m, id)
	}
	return Result{State: "delivered", ID: id}
}
func (p *fakeProvider) Lookup(_ context.Context, _ Credentials, a model.MailAttempt) Result {
	if p.lookupFn != nil {
		return p.lookupFn(a)
	}
	if p.lookup.State == "" {
		return Result{State: "pending"}
	}
	return p.lookup
}
func (p *fakeProvider) Quotas(context.Context, Credentials) ([]QuotaObservation, error) {
	return p.observations, nil
}

func mailFixture(t *testing.T) *Dispatcher {
	t.Helper()
	t.Setenv("MAIL_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))))
	dsn := os.Getenv("MAIL_TEST_DSN")
	if dsn == "" {
		t.Skip("MAIL_TEST_DSN is required for PostgreSQL integration tests")
	}
	target, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"postgres", "postgresql"}, target.Scheme, "mail integration tests use PostgreSQL only")
	// Create a private schema before any migration or fixture cleanup. The normal
	// application schema must never be dropped or truncated by these tests.
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	adminSQL, err := admin.DB()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, adminSQL.Close()) })
	schemaName := "mail_delivery_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, admin.Exec("CREATE SCHEMA \""+schemaName+"\"").Error)
	t.Cleanup(func() { assert.NoError(t, admin.Exec("DROP SCHEMA \""+schemaName+"\" CASCADE").Error) })
	query := target.Query()
	query.Set("search_path", schemaName)
	target.RawQuery = query.Encode()
	db, err := gorm.Open(postgres.Open(target.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sqlDB.Close()) })
	var currentSchema string
	require.NoError(t, db.Raw("SELECT current_schema()").Scan(&currentSchema).Error)
	require.Equal(t, schemaName, currentSchema)
	require.NoError(t, model.MigrateMailDelivery(db))
	require.NoError(t, db.AutoMigrate(&model.AuthFlow{}))
	d := New(db)
	d.Now = func() time.Time { return time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC) }
	return d
}
func addMailAccount(t *testing.T, d *Dispatcher, name string, priority int, limit int64, p *fakeProvider) model.MailAccount {
	t.Helper()
	d.Providers[name] = p
	require.NoError(t, d.Import([]AccountInput{{Name: name, Provider: name, Enabled: true, Credentials: Credentials{APIKey: "dummy-test-secret"},
		Quotas:   []QuotaInput{{Name: "daily", Mode: "manual", Period: "confirmed-test-period", Limit: limit}},
		Channels: []ChannelInput{{Name: name, FromAddress: "noreply@example.com", FromName: "Test", Priority: priority, Enabled: true}}}}))
	var a model.MailAccount
	require.NoError(t, d.DB.First(&a, "name = ?", name).Error)
	assert.NotContains(t, a.Secret, "dummy-test-secret")
	return a
}
func queueMail(t *testing.T, d *Dispatcher, receiver string) []model.MailJob {
	t.Helper()
	var prior []model.MailJob
	require.NoError(t, d.DB.Find(&prior).Error)
	seen := map[string]bool{}
	for _, j := range prior {
		seen[j.ID] = true
	}
	require.NoError(t, d.enqueue(common.EmailDeliveryRequest{Subject: "Test", Receiver: receiver, Content: "sensitive-code", ExpiresAt: d.Now().Add(10 * time.Minute)}))
	var jobs []model.MailJob
	require.NoError(t, d.DB.Order("created_at,id").Find(&jobs).Error)

	fresh := make([]model.MailJob, 0, len(jobs))
	for _, j := range jobs {
		if !seen[j.ID] {
			fresh = append(fresh, j)
		}
	}
	return fresh
}
func TestRoutingThresholdAndFailure(t *testing.T) {
	d := mailFixture(t)
	first := &fakeProvider{send: func(Message, string) Result { return Result{State: "rejected", Code: "http_429"} }}
	second := &fakeProvider{}
	third := &fakeProvider{}
	a := addMailAccount(t, d, "first", 1, 10, first)
	b := addMailAccount(t, d, "second", 2, 1, second)
	addMailAccount(t, d, "third", 3, 10, third)
	// A second From address under one account cannot bypass the shared threshold.
	require.NoError(t, d.DB.Create(&model.MailChannel{AccountID: b.ID, Name: "second-alias", FromAddress: "other@example.com", Enabled: true, Priority: 3}).Error)
	jobs := queueMail(t, d, "a@example.com;b@example.com")
	require.Len(t, jobs, 2)
	for _, j := range jobs {
		assert.NotContains(t, j.Payload, "sensitive-code")
		require.NoError(t, d.Process(context.Background(), j.ID))
	}
	assert.Equal(t, 1, first.calls)
	assert.Equal(t, 1, second.calls)
	assert.Equal(t, 1, third.calls)
	var q model.MailQuota
	require.NoError(t, d.DB.First(&q, "account_id = ?", a.ID).Error)
	assert.Zero(t, q.Used)
	assert.Zero(t, q.Reserved)
	q = model.MailQuota{}
	require.NoError(t, d.DB.First(&q, "account_id = ?", b.ID).Error)
	assert.EqualValues(t, 1, q.Used)
	assert.Zero(t, q.Reserved)
	var stored []model.MailJob
	require.NoError(t, d.DB.Find(&stored).Error)
	for _, j := range stored {
		assert.Equal(t, "delivered", j.State)
		assert.Empty(t, j.Payload)
	}
	require.NoError(t, model.MigrateMailDelivery(d.DB))
	var count int64
	require.NoError(t, d.DB.Model(&model.MailJob{}).Count(&count).Error)
	assert.EqualValues(t, 2, count)
	err := d.DB.Create(&model.MailChannel{Name: "second", FromAddress: "duplicate@example.com"}).Error
	require.Error(t, err, "channel name uniqueness survives repeated migration")
}
func TestAmbiguousOutcomeAndExpiry(t *testing.T) {
	for _, resolve := range []string{"delivered", "rejected", "expired"} {
		t.Run(resolve, func(t *testing.T) {
			d := mailFixture(t)
			unknown := &fakeProvider{send: func(Message, string) Result { return Result{State: "unknown", Code: "transport_unknown"} }}
			backup := &fakeProvider{}
			a := addMailAccount(t, d, "primary", 1, 10, unknown)
			addMailAccount(t, d, "backup", 2, 10, backup)
			job := queueMail(t, d, "a@example.com")[0]
			require.NoError(t, d.Process(context.Background(), job.ID))
			require.NoError(t, d.Process(context.Background(), job.ID))
			assert.Equal(t, 1, unknown.calls)
			assert.Zero(t, backup.calls)
			var attempt model.MailAttempt
			require.NoError(t, d.DB.First(&attempt, "job_id = ?", job.ID).Error)
			if resolve == "expired" {
				oldNow := d.Now()
				d.Now = func() time.Time { return oldNow.Add(11 * time.Minute) }
				// Reconcile after expiry, without retaining or resending the code.
				unknown.lookup = Result{State: "delivered", ID: "provider-proof"}
				require.NoError(t, d.RunOnce(context.Background()))
				assert.Equal(t, 1, unknown.calls)
			} else {
				require.NoError(t, d.settle(attempt, Result{State: resolve, Code: "operator_confirmed"}))
				require.NoError(t, d.Process(context.Background(), job.ID))
			}
			require.NoError(t, d.DB.First(&job, "id = ?", job.ID).Error)
			assert.Equal(t, "delivered", job.State)
			assert.Empty(t, job.Payload)
			if resolve == "rejected" {
				assert.Equal(t, 1, backup.calls)
			} else {
				assert.Zero(t, backup.calls)
			}
			var q model.MailQuota
			require.NoError(t, d.DB.First(&q, "account_id = ?", a.ID).Error)
			assert.Zero(t, q.Reserved)
		})
	}
}
func TestConcurrentWorkersAndQuotaReservation(t *testing.T) {
	d := mailFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	primary := &fakeProvider{send: func(Message, string) Result { close(entered); <-release; return Result{State: "delivered"} }}
	backup := &fakeProvider{}
	a := addMailAccount(t, d, "primary", 1, 1, primary)
	addMailAccount(t, d, "backup", 2, 10, backup)
	jobs := queueMail(t, d, "a@example.com;b@example.com")
	done := make(chan error, 1)
	go func() { done <- d.Process(context.Background(), jobs[0].ID) }()
	<-entered
	// Another worker cannot reclaim a leased job or exceed a reserved account budget.
	require.NoError(t, d.Process(context.Background(), jobs[0].ID))
	require.NoError(t, d.Process(context.Background(), jobs[1].ID))
	close(release)
	require.NoError(t, <-done)
	assert.Equal(t, 1, primary.calls)
	assert.Equal(t, 1, backup.calls)
	var q model.MailQuota
	require.NoError(t, d.DB.First(&q, "account_id = ?", a.ID).Error)
	assert.EqualValues(t, 1, q.Used)
	assert.Zero(t, q.Reserved)
}
func TestQuotaPeriodsAndProviderBalance(t *testing.T) {
	t.Run("UTC and late outcomes", func(t *testing.T) {
		d := mailFixture(t)
		p := &fakeProvider{send: func(Message, string) Result { return Result{State: "unknown"} }}
		a := addMailAccount(t, d, "primary", 1, 1, p)
		before := time.Date(2026, 11, 24, 15, 59, 0, 0, time.UTC)
		d.Now = func() time.Time { return before }
		require.NoError(t, d.DB.Model(&model.MailQuota{}).Where("account_id = ?", a.ID).Updates(map[string]any{"mode": "calendar_day", "timezone": "UTC", "period": "2026-11-24", "reserved": 0}).Error)
		job := queueMail(t, d, "a@example.com")[0]
		require.NoError(t, d.Process(context.Background(), job.ID))
		var oldAttempt model.MailAttempt
		require.NoError(t, d.DB.First(&oldAttempt, "job_id = ?", job.ID).Error)
		before = before.Add(2 * time.Minute) // Hong Kong midnight must not reset UTC daily quota.
		other := queueMail(t, d, "b@example.com")
		require.NoError(t, d.Process(context.Background(), other[0].ID))
		var q model.MailQuota
		require.NoError(t, d.DB.First(&q, "account_id = ?", a.ID).Error)
		assert.Equal(t, "2026-11-24", q.Period)
		assert.EqualValues(t, 1, q.Reserved)
		before = time.Date(2026, 11, 25, 0, 1, 0, 0, time.UTC)
		queueMail(t, d, "c@example.com")
		var newest model.MailJob
		require.NoError(t, d.DB.Where("created_at = ?", before.Unix()).First(&newest).Error)
		require.NoError(t, d.Process(context.Background(), newest.ID))
		require.NoError(t, d.settle(oldAttempt, Result{State: "delivered"}))
		require.NoError(t, d.DB.First(&q, q.ID).Error)
		assert.Equal(t, "2026-11-25", q.Period)
		assert.EqualValues(t, 1, q.Reserved)
		assert.Zero(t, q.Used)
	})
	t.Run("provider balance", func(t *testing.T) {
		d := mailFixture(t)
		p := &fakeProvider{observations: []QuotaObservation{{Name: "daily", Capacity: 300, Remaining: 0}}}
		a := addMailAccount(t, d, "primary", 1, 300, p)
		b := &fakeProvider{}
		addMailAccount(t, d, "backup", 2, 10, b)
		require.NoError(t, d.DB.Model(&model.MailQuota{}).Where("account_id = ?", a.ID).Updates(map[string]any{"mode": "provider_balance", "capacity": 300}).Error)
		job := queueMail(t, d, "a@example.com")[0]
		require.NoError(t, d.Process(context.Background(), job.ID))
		assert.Zero(t, p.calls)
		assert.Equal(t, 1, b.calls)
		p.observations[0].Remaining = 300
		jobs := queueMail(t, d, "b@example.com")
		for _, j := range jobs {
			require.NoError(t, d.Process(context.Background(), j.ID))
		}
		assert.Equal(t, 1, p.calls)
	})
	t.Run("manual never guesses midnight", func(t *testing.T) {
		d := mailFixture(t)
		p := &fakeProvider{}
		a := addMailAccount(t, d, "primary", 1, 1, p)
		job := queueMail(t, d, "a@example.com")[0]
		require.NoError(t, d.Process(context.Background(), job.ID))
		now := d.Now()
		d.Now = func() time.Time { return now.Add(24 * time.Hour) }
		jobs := queueMail(t, d, "b@example.com")
		for _, j := range jobs {
			require.NoError(t, d.Process(context.Background(), j.ID))
		}
		assert.Equal(t, 1, p.calls)
		var q model.MailQuota
		require.NoError(t, d.DB.First(&q, "account_id = ?", a.ID).Error)
		require.NoError(t, d.resetPeriod([]string{fmt.Sprint(q.ID), "next-confirmed-period", fmt.Sprint(d.Now().Add(24 * time.Hour).Unix())}))
		require.NoError(t, d.Process(context.Background(), jobs[len(jobs)-1].ID))
		assert.Equal(t, 2, p.calls)
	})
}
func TestVerificationInvalidationAndEncryption(t *testing.T) {
	d := mailFixture(t)
	p := &fakeProvider{}
	addMailAccount(t, d, "primary", 1, 10, p)
	oldHandler := common.EmailDeliveryHandler
	common.EmailDeliveryHandler = d.enqueue
	t.Cleanup(func() { common.EmailDeliveryHandler = oldHandler })
	// Use real wall time because the existing OTP store owns its own clock.
	d.Now = time.Now
	common.RegisterVerificationCodeWithKey("otp@example.com", "123456", common.EmailVerificationPurpose)
	require.NoError(t, common.SendVerificationEmail("OTP", "otp@example.com", "123456", common.EmailVerificationPurpose, "123456"))
	common.RegisterVerificationCodeWithKey("otp@example.com", "654321", common.EmailVerificationPurpose)
	require.NoError(t, d.RunOnce(context.Background()))
	assert.Zero(t, p.calls)
	common.DeleteKey("otp@example.com", common.EmailVerificationPurpose)
	state := model.EmailBindingState{NewCodeHash: "salted-test-hash"}
	payload, err := common.Marshal(state)
	require.NoError(t, err)
	flow := model.AuthFlow{TokenHash: strings.Repeat("h", 64), Purpose: model.AuthFlowPurposeEmailBinding, Payload: string(payload), ExpiresAt: d.Now().Add(10 * time.Minute)}
	require.NoError(t, d.DB.Create(&flow).Error)
	tag := model.EmailBindingDeliveryTag(flow.Id, state)
	valid, err := d.Valid(tag)
	require.NoError(t, err)
	assert.True(t, valid)
	require.NoError(t, d.enqueue(common.EmailDeliveryRequest{Subject: "Bind", Receiver: "bind@example.com", Content: "123456", ExpiresAt: flow.ExpiresAt, ValidityTag: tag}))
	now := d.Now()
	require.NoError(t, d.DB.Model(&flow).Update("consumed_at", now).Error)
	require.NoError(t, d.RunOnce(context.Background()))
	assert.Zero(t, p.calls)
	encrypted, err := seal([]byte("secret"), "job:a")
	require.NoError(t, err)
	_, err = unseal(encrypted, "job:b")
	require.Error(t, err)
	t.Setenv("MAIL_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("z", 32))))
	_, err = unseal(encrypted, "job:a")
	require.Error(t, err)
}

type mailRoundTripper func(*http.Request) (*http.Response, error)

func (f mailRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if trace := httptrace.ContextClientTrace(r.Context()); trace != nil && trace.GotConn != nil {
		trace.GotConn(httptrace.GotConnInfo{})
	}
	return f(r)
}
func TestHTTPProviderContracts(t *testing.T) {
	cases := []struct {
		kind        string
		status      int
		body, state string
	}{
		{"resend", 201, `{"id":"r1"}`, "accepted"}, {"resend", 409, `{}`, "unknown"}, {"resend", 429, `{}`, "rejected"},
		{"brevo", 201, `{"messageId":"b1"}`, "accepted"}, {"brevo", 503, `{}`, "unknown"},
		{"smtp2go", 200, `{"data":{"succeeded":0,"failed":1,"failures":["private info"]}}`, "rejected"},
		{"smtp2go", 200, `{"data":{"succeeded":1,"failed":0,"email_id":"s1"}}`, "accepted"},
		{"smtp2go", 200, `{"data":{"email_id":"s1"}}`, "unknown"},
	}
	old := providerHTTPClient
	t.Cleanup(func() { providerHTTPClient = old })
	for _, tc := range cases {
		t.Run(tc.kind+tc.body+fmt.Sprint(tc.status), func(t *testing.T) {
			providerHTTPClient = &http.Client{Transport: mailRoundTripper(func(req *http.Request) (*http.Response, error) {
				raw, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				assert.NotContains(t, string(raw), "dummy-key")
				if tc.kind == "resend" {
					assert.Equal(t, "attempt-stable", req.Header.Get("Idempotency-Key"))
					assert.Equal(t, "Bearer dummy-key", req.Header.Get("Authorization"))
				}
				if tc.kind == "brevo" {
					assert.Equal(t, "dummy-key", req.Header.Get("api-key"))
				}
				if tc.kind == "smtp2go" {
					assert.Contains(t, string(raw), `"fastaccept":false`)
					assert.Equal(t, "dummy-key", req.Header.Get("X-Smtp2go-Api-Key"))
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
			})}
			r := (HTTPProvider{Kind: tc.kind}).Send(context.Background(), Credentials{APIKey: "dummy-key"}, model.MailChannel{FromAddress: "from@example.com"}, Message{To: []string{"to@example.com"}, Subject: "Test", HTML: "HTML"}, "attempt-stable")
			assert.Equal(t, tc.state, r.State)
			assert.NotContains(t, r.Code, "private info")
		})
	}
	providerHTTPClient = &http.Client{Transport: mailRoundTripper(func(*http.Request) (*http.Response, error) { return nil, errors.New("timeout") })}
	assert.Equal(t, "unknown", (HTTPProvider{Kind: "brevo"}).Send(context.Background(), Credentials{APIKey: "key"}, model.MailChannel{}, Message{}, "a").State)
}
func TestLookupAndQuotaContracts(t *testing.T) {
	old := providerHTTPClient
	t.Cleanup(func() { providerHTTPClient = old })
	cases := []struct{ kind, body, state string }{
		{"resend", `{"last_event":"delivered"}`, "delivered"},
		{"brevo", `{"events":[{"event":"delivered","messageId":"unrelated","tag":"other"}]}`, "pending"},
		{"brevo", `{"events":[{"event":"delivered","messageId":"proof","tag":"attempt"}]}`, "delivered"},
		{"smtp2go", `{"data":{"events":[{"event":"delivered","email_id":"proof"}]}}`, "delivered"},
	}
	for _, tc := range cases {
		providerHTTPClient = &http.Client{Transport: mailRoundTripper(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		})}
		a := model.MailAttempt{ID: "attempt", ProviderID: "proof", CreatedAt: time.Now().Unix()}
		if tc.kind == "brevo" {
			a.ProviderID = ""
		}
		assert.Equal(t, tc.state, (HTTPProvider{Kind: tc.kind}).Lookup(context.Background(), Credentials{APIKey: "key"}, a).State)
	}
	for _, stamp := range []string{"2026-12-01 00:00:00", "2026-12-01T00:00:00Z"} {
		providerHTTPClient = &http.Client{Transport: mailRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data":{"cycle_start":"start","cycle_end":"` + stamp + `","cycle_max":1000,"cycle_remaining":999}}`))}, nil
		})}
		observations, err := (HTTPProvider{Kind: "smtp2go"}).Quotas(context.Background(), Credentials{APIKey: "key"})
		if strings.HasSuffix(stamp, "Z") {
			require.NoError(t, err)
			assert.EqualValues(t, 999, observations[0].Remaining)
		} else {
			require.Error(t, err, "never guess a provider timezone")
		}
	}
}
func TestSMTPRequiresTLS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, "220 localhost SMTP\r\n")
		buf := make([]byte, 512)
		_, _ = conn.Read(buf)
		_, _ = io.WriteString(conn, "250 localhost\r\n")
		_, _ = conn.Read(buf)
	}()
	address := listener.Addr().(*net.TCPAddr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := (SMTPProvider{}).Send(ctx, Credentials{Host: "127.0.0.1", Port: address.Port, TLS: "starttls", Username: "private-user", Password: "private-password"}, model.MailChannel{}, Message{}, "id")
	assert.Equal(t, "rejected", result.State)
	assert.Equal(t, "smtp_tls_required", result.Code)
	<-done
}

func TestResendReplaysOnlyIdenticalLiveAttempt(t *testing.T) {
	d := mailFixture(t)
	p := &fakeProvider{}
	d.Providers["resend"] = p
	require.NoError(t, d.Import([]AccountInput{{Name: "resend", Provider: "resend", Enabled: true, Credentials: Credentials{APIKey: "dummy"}, Quotas: []QuotaInput{{Name: "daily", Mode: "calendar_day", Timezone: "UTC", Limit: 100}}, Channels: []ChannelInput{{Name: "original", FromAddress: "original@example.com", Enabled: true}}}}))
	d.Providers["resend"] = HTTPProvider{Kind: "resend"}
	old := providerHTTPClient
	t.Cleanup(func() { providerHTTPClient = old })
	var bodies, keys []string
	providerHTTPClient = &http.Client{Transport: mailRoundTripper(func(r *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		bodies = append(bodies, string(raw))
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if len(bodies) == 1 {
			return nil, errors.New("lost acknowledgement")
		}
		return &http.Response{StatusCode: 201, Body: io.NopCloser(strings.NewReader(`{"id":"proof"}`))}, nil
	})}
	job := queueMail(t, d, "a@example.com")[0]
	require.NoError(t, d.Process(context.Background(), job.ID))
	require.NoError(t, d.DB.Model(&model.MailChannel{}).Where("name = ?", "original").Update("from_address", "edited@example.com").Error)
	require.NoError(t, d.Process(context.Background(), job.ID))
	require.Len(t, bodies, 2)
	assert.Equal(t, bodies[0], bodies[1])
	assert.Equal(t, keys[0], keys[1])
	assert.Contains(t, bodies[1], "original@example.com")
	// After the message deadline the worker may query state but must never replay POST.
	current := d.Now()
	d.Now = func() time.Time { return current.Add(11 * time.Minute) }
	providerHTTPClient = &http.Client{Transport: mailRoundTripper(func(r *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodGet, r.Method)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"last_event":"delivered"}`))}, nil
	})}
	require.NoError(t, d.RunOnce(context.Background()))
	require.NoError(t, d.DB.First(&job, "id = ?", job.ID).Error)
	assert.Equal(t, "delivered", job.State)
	assert.Empty(t, job.Payload)
}

func TestChallengeInvalidatedWhileReservingQuota(t *testing.T) {
	d := mailFixture(t)
	p := &fakeProvider{}
	a := addMailAccount(t, d, "primary", 1, 10, p)
	checks := 0
	d.Valid = func(string) (bool, error) { checks++; return checks < 3, nil }
	require.NoError(t, d.enqueue(common.EmailDeliveryRequest{Subject: "OTP", Receiver: "otp@example.com", Content: "123456", ExpiresAt: d.Now().Add(time.Minute), ValidityTag: "test-challenge"}))
	var job model.MailJob
	require.NoError(t, d.DB.First(&job).Error)
	require.NoError(t, d.Process(context.Background(), job.ID))
	assert.Zero(t, p.calls)
	require.NoError(t, d.DB.First(&job, "id = ?", job.ID).Error)
	assert.Equal(t, "expired", job.State)
	assert.Empty(t, job.Payload)
	var quota model.MailQuota
	require.NoError(t, d.DB.First(&quota, "account_id = ?", a.ID).Error)
	assert.Zero(t, quota.Reserved)
	assert.Zero(t, quota.Used)
}
func TestQueuedMailSurvivesRestartAndTemporaryFailure(t *testing.T) {
	d := mailFixture(t)
	p := &fakeProvider{send: func(Message, string) Result { return Result{State: "rejected", Code: "temporarily_unavailable"} }}
	addMailAccount(t, d, "primary", 1, 10, p)
	job := queueMail(t, d, "a@example.com")[0]
	require.NoError(t, d.Process(context.Background(), job.ID))
	require.NoError(t, d.DB.First(&job, "id = ?", job.ID).Error)
	assert.Equal(t, "queued", job.State)
	assert.NotEmpty(t, job.Payload)
	restarted := New(d.DB)
	restarted.Providers = d.Providers
	restarted.Now = func() time.Time { return d.Now().Add(2 * time.Minute) }
	p.send = nil
	require.NoError(t, restarted.RunOnce(context.Background()))
	require.NoError(t, d.DB.First(&job, "id = ?", job.ID).Error)
	assert.Equal(t, "delivered", job.State)
	assert.Equal(t, 2, p.calls)
}

func TestConnectionFailureTriggersSafeFailover(t *testing.T) {
	old := providerHTTPClient
	t.Cleanup(func() { providerHTTPClient = old })
	providerHTTPClient = &http.Client{Transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}}}
	r := (HTTPProvider{Kind: "brevo"}).Send(context.Background(), Credentials{APIKey: "dummy"}, model.MailChannel{}, Message{}, "test")
	assert.Equal(t, "rejected", r.State)
	assert.Equal(t, "connection_failed", r.Code)
}

func TestImportPreservesConsumedQuota(t *testing.T) {
	d := mailFixture(t)
	p := &fakeProvider{}
	d.Providers["resend"] = p
	input := []AccountInput{{Name: "resend", Provider: "resend", Enabled: true, Credentials: Credentials{APIKey: "dummy"}, Quotas: []QuotaInput{{Name: "daily", Mode: "calendar_day", Timezone: "UTC", Limit: 10, InitialUsed: 9}}, Channels: []ChannelInput{{Name: "resend", FromAddress: "from@example.com", Enabled: true}}}}
	require.NoError(t, d.Import(input))
	jobs := queueMail(t, d, "a@example.com;b@example.com")
	for _, j := range jobs {
		require.NoError(t, d.Process(context.Background(), j.ID))
	}
	assert.Equal(t, 1, p.calls)
	input[0].Quotas[0].InitialUsed = 0
	require.NoError(t, d.Import(input))
	var quota model.MailQuota
	require.NoError(t, d.DB.First(&quota).Error)
	assert.EqualValues(t, 10, quota.Used)
}

func TestLargeMailPayloadAcrossDatabases(t *testing.T) {
	d := mailFixture(t)
	p := &fakeProvider{send: func(m Message, _ string) Result { assert.Len(t, m.HTML, 96*1024); return Result{State: "delivered"} }}
	addMailAccount(t, d, "primary", 1, 10, p)
	require.NoError(t, d.enqueue(common.EmailDeliveryRequest{Subject: "Long HTML", Receiver: "a@example.com", Content: strings.Repeat("x", 96*1024), ExpiresAt: d.Now().Add(time.Hour)}))
	var job model.MailJob
	require.NoError(t, d.DB.First(&job).Error)
	require.NoError(t, d.Process(context.Background(), job.ID))
	assert.Equal(t, 1, p.calls)
	require.NoError(t, model.MigrateMailDelivery(d.DB))
}

func TestStaleLookupCannotOverrideConfirmedDelivery(t *testing.T) {
	d := mailFixture(t)
	primary := &fakeProvider{send: func(Message, string) Result { return Result{State: "unknown"} }}
	backup := &fakeProvider{}
	addMailAccount(t, d, "primary", 1, 10, primary)
	addMailAccount(t, d, "backup", 2, 10, backup)
	job := queueMail(t, d, "a@example.com")[0]
	require.NoError(t, d.Process(context.Background(), job.ID))
	primary.lookupFn = func(a model.MailAttempt) Result {
		require.NoError(t, d.settle(a, Result{State: "delivered", Code: "operator_confirmed"}))
		return Result{State: "failed", Code: "stale_failure"}
	}
	require.NoError(t, d.Process(context.Background(), job.ID))
	assert.Zero(t, backup.calls)
	require.NoError(t, d.DB.First(&job, "id = ?", job.ID).Error)
	assert.Equal(t, "delivered", job.State)
}

func TestPendingSubmissionDoesNotFailOver(t *testing.T) {
	d := mailFixture(t)
	p := &fakeProvider{send: func(Message, string) Result { return Result{State: "pending"} }}
	backup := &fakeProvider{}
	a := addMailAccount(t, d, "primary", 1, 10, p)
	addMailAccount(t, d, "backup", 2, 10, backup)
	job := queueMail(t, d, "a@example.com")[0]
	require.NoError(t, d.Process(context.Background(), job.ID))
	assert.Zero(t, backup.calls)
	require.NoError(t, d.DB.First(&job, "id = ?", job.ID).Error)
	assert.Equal(t, "unknown", job.State)
	var q model.MailQuota
	require.NoError(t, d.DB.First(&q, "account_id = ?", a.ID).Error)
	assert.EqualValues(t, 1, q.Reserved)
}
