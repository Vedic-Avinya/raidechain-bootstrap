package fcm

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// PushSender sends a push to an FCM token (e.g. to wake the app).
// Noop when Firebase is not configured.
type PushSender interface {
	Send(ctx context.Context, token string, data map[string]string) error
	// SendDataOnly sends a data-only push (no notification payload).
	// Use for messages where the app builds its own notification (e.g. track requests with action buttons).
	// Data-only messages always trigger onMessageReceived even when the app is in background.
	SendDataOnly(ctx context.Context, token string, data map[string]string) error
}

// noopSender does nothing (used when no credentials are set).
type noopSender struct{}

func (noopSender) Send(context.Context, string, map[string]string) error         { return nil }
func (noopSender) SendDataOnly(context.Context, string, map[string]string) error { return nil }

// NoopSender returns a PushSender that silently discards all pushes.
// Use as a graceful fallback when FCM credentials are unavailable.
func NoopSender() PushSender { return noopSender{} }

// NewPushSender returns a PushSender. Credentials, in order of use:
//  1. GOOGLE_APPLICATION_CREDENTIALS (path to service account JSON)
//  2. FIREBASE_SERVICE_ACCOUNT_JSON (path or inline JSON)
//  3. Application Default Credentials (ADC) — on GCP VM uses the default Compute Engine
//     service account. Grant that account "Firebase Cloud Messaging API Admin" (or Firebase Admin)
//     in GCP IAM so it can send FCM. No key file needed on the VM.
// If FCM_DISABLED=true, returns a noop sender and does not try ADC.
func NewPushSender(ctx context.Context) (PushSender, error) {
	if strings.ToLower(strings.TrimSpace(os.Getenv("FCM_DISABLED"))) == "true" {
		slog.Info("fcm", "msg", "disabled by FCM_DISABLED; offline peers will not receive push")
		return noopSender{}, nil
	}
	credsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	credsJSON := strings.TrimSpace(os.Getenv("FIREBASE_SERVICE_ACCOUNT_JSON"))
	useADC := credsPath == "" && credsJSON == ""

	sender, err := newFirebaseSenderImpl(ctx, credsPath, credsJSON)
	if err != nil {
		// If we were trying ADC (no explicit creds) and it failed, fall back to noop instead of failing process.
		if useADC {
			slog.Info("fcm", "msg", "ADC unavailable or missing IAM role; FCM disabled", "err", err)
			return noopSender{}, nil
		}
		return nil, err
	}
	return sender, nil
}
