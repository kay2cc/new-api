package maildelivery

import (
	"context"
	"crypto/tls"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
)

type SMTPProvider struct{}

func (SMTPProvider) Quotas(context.Context, Credentials) ([]QuotaObservation, error) {
	return nil, quotaError("smtp")
}
func (SMTPProvider) Lookup(context.Context, Credentials, model.MailAttempt) Result {
	return Result{State: "pending"}
}

func (SMTPProvider) Send(ctx context.Context, c Credentials, ch model.MailChannel, m Message, id string) Result {
	if c.Host == "" || c.Port <= 0 || c.Port > 65535 || (c.TLS != "starttls" && c.TLS != "implicit") {
		return Result{State: "rejected", Code: "smtp_configuration"}
	}
	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port)))
	if err != nil {
		return Result{State: "rejected", Code: "smtp_connect"}
	}
	defer conn.Close()
	deadline := time.Now().Add(20 * time.Second)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	_ = conn.SetDeadline(deadline)
	config := &tls.Config{ServerName: c.Host, MinVersion: tls.VersionTLS12}
	if c.TLS == "implicit" {
		secure := tls.Client(conn, config)
		if err = secure.HandshakeContext(ctx); err != nil {
			return Result{State: "rejected", Code: "smtp_tls"}
		}
		conn = secure
	}
	client, err := smtp.NewClient(conn, c.Host)
	if err != nil {
		return Result{State: "rejected", Code: "smtp_greeting"}
	}
	defer client.Close()
	if c.TLS == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return Result{State: "rejected", Code: "smtp_tls_required"}
		}
		if err = client.StartTLS(config); err != nil {
			return Result{State: "rejected", Code: "smtp_tls"}
		}
	}
	if c.Username != "" {
		if err = client.Auth(common.AutoSMTPAuth(c.Username, c.Password)); err != nil {
			return Result{State: "rejected", Code: "smtp_auth"}
		}
	}
	if err = client.Mail(ch.FromAddress); err != nil {
		return Result{State: "rejected", Code: "smtp_sender"}
	}
	for _, recipient := range m.To {
		if err = client.Rcpt(recipient); err != nil {
			return Result{State: "rejected", Code: "smtp_recipient"}
		}
	}
	writer, err := client.Data()
	if err != nil {
		return Result{State: "rejected", Code: "smtp_data"}
	}
	from := (&mail.Address{Name: ch.FromName, Address: ch.FromAddress}).String()
	_, domain, _ := strings.Cut(ch.FromAddress, "@")
	content := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\nMessage-ID: <%s@%s>\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n%s\r\n", from, strings.Join(m.To, ", "), mime.QEncoding.Encode("UTF-8", m.Subject), time.Now().Format(time.RFC1123Z), id, domain, m.HTML)
	if _, err = writer.Write([]byte(content)); err != nil {
		return Result{State: "unknown", Code: "smtp_write_unknown"}
	}
	if err = writer.Close(); err != nil {
		// A definite SMTP rejection after DATA is safe to fail over; socket errors
		// after the terminator are ambiguous, so preserve the reservation.
		if reply, ok := err.(*textproto.Error); ok && reply.Code >= 400 {
			return Result{State: "rejected", Code: "smtp_" + strconv.Itoa(reply.Code)}
		}
		return Result{State: "unknown", Code: "smtp_result_unknown"}
	}
	_ = client.Quit() // QUIT failure cannot undo a successful DATA acknowledgement.
	return Result{State: "accepted", ID: id}
}
