package fcm

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/compute/metadata"
	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

// serviceAccountKeyMeta is the subset of a GCP service account JSON we log at startup (no secrets).
type serviceAccountKeyMeta struct {
	ProjectID   string `json:"project_id"`
	ClientEmail string `json:"client_email"`
}

func logFCMCredentialIdentity(credsPath, credsJSON string) {
	var raw []byte
	var err error
	switch {
	case strings.TrimSpace(credsPath) != "":
		raw, err = os.ReadFile(credsPath)
	case strings.TrimSpace(credsJSON) != "":
		s := strings.TrimSpace(credsJSON)
		if strings.HasPrefix(s, "{") {
			raw = []byte(s)
		} else {
			path := s
			if !filepath.IsAbs(path) {
				if cwd, _ := os.Getwd(); cwd != "" {
					path = filepath.Join(cwd, path)
				}
			}
			raw, err = os.ReadFile(path)
		}
	default:
		return
	}
	if err != nil || len(raw) == 0 {
		return
	}
	var meta serviceAccountKeyMeta
	if json.Unmarshal(raw, &meta) != nil || meta.ClientEmail == "" {
		return
	}
	slog.Info("fcm", "msg", "using JSON key credentials (GOOGLE_APPLICATION_CREDENTIALS or FIREBASE_SERVICE_ACCOUNT_JSON) — NOT the VM default service account",
		"credential_service_account", meta.ClientEmail,
		"credential_project_id", meta.ProjectID,
		"iam_hint", "Grant roles/firebasecloudmessaging.admin (or Firebase Admin) to credential_service_account on GCP project credential_project_id (must match Android Firebase project). IAM on *-compute@developer.gserviceaccount.com does not apply while a key file is set.")
}

func newFirebaseSenderImpl(ctx context.Context, credsPath, credsJSON string) (PushSender, error) {
	logFCMCredentialIdentity(credsPath, credsJSON)

	var opts []option.ClientOption
	if credsPath != "" {
		opts = append(opts, option.WithCredentialsFile(credsPath))
	} else if credsJSON != "" {
		jsonBytes := []byte(credsJSON)
		if strings.HasSuffix(credsJSON, ".json") || (len(credsJSON) < 100 && !strings.HasPrefix(strings.TrimSpace(credsJSON), "{")) {
			path := credsJSON
			if !filepath.IsAbs(path) {
				if cwd, _ := os.Getwd(); cwd != "" {
					path = filepath.Join(cwd, path)
				}
			}
			var err error
			jsonBytes, err = os.ReadFile(path)
			if err != nil {
				slog.Error("fcm", "msg", "failed to read credentials file", "path", path, "err", err)
				return nil, err
			}
		}
		opts = append(opts, option.WithCredentialsJSON(jsonBytes))
	}
	// Optional explicit project (must match the Firebase/Android app project in google-services.json).
	// Order: FIREBASE_PROJECT_ID, then standard GCP env vars; if unset, SDK infers from JSON or ADC metadata.
	fbCfg := firebaseConfigFromEnv()
	// When opts is empty, Firebase uses Application Default Credentials (ADC).
	// On GCP Compute Engine that is the VM's service account; grant it Firebase roles in IAM.
	app, err := firebase.NewApp(ctx, fbCfg, opts...)
	if err != nil {
		if len(opts) == 0 {
			slog.Debug("fcm", "msg", "ADC init failed (not on GCP or missing IAM role)", "err", err)
		}
		return nil, err
	}
	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, err
	}
	source := "credentials file/JSON"
	if len(opts) == 0 {
		source = "Application Default Credentials (e.g. GCP VM service account)"
	}
	logArgs := []any{"msg", "initialized; will send push when rider peer is offline", "auth", source}
	if fbCfg != nil && fbCfg.ProjectID != "" {
		logArgs = append(logArgs, "firebase_project_id", fbCfg.ProjectID)
	} else {
		logArgs = append(logArgs, "firebase_project_id", "(inferred from credentials or metadata — set FIREBASE_PROJECT_ID to log explicitly)")
	}
	if len(opts) == 0 && metadata.OnGCE() {
		if email, err := metadata.Email("default"); err == nil {
			logArgs = append(logArgs, "adc_service_account", email, "iam_hint", "grant roles/firebasecloudmessaging.admin to this email in GCP IAM")
		}
	}
	slog.Info("fcm", logArgs...)
	return &firebaseSender{client: client}, nil
}

func firebaseConfigFromEnv() *firebase.Config {
	for _, k := range []string{"FIREBASE_PROJECT_ID", "GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return &firebase.Config{ProjectID: v}
		}
	}
	return nil
}

type firebaseSender struct {
	client *messaging.Client
}

func (s *firebaseSender) Send(ctx context.Context, token string, data map[string]string) error {
	if token == "" {
		return nil
	}
	// High priority so FCM delivers when device is in Doze/locked; otherwise messages can be delayed or dropped.
	msg := &messaging.Message{
		Token: token,
		Data:  data,
		Android: &messaging.AndroidConfig{
			Priority: "high",
		},
		// Minimal notification so system can show heads-up when app is in background/Doze (helps wake device).
		// App still receives data in onMessageReceived.
		Notification: &messaging.Notification{
			Title: "RideChain",
			Body:  "New message",
		},
	}
	msgID, err := s.client.Send(ctx, msg)
	if err != nil {
		slog.Warn("fcm", "msg", "send failed", "token_prefix", tokenPrefix(token), "err", err)
		return err
	}
	slog.Info("fcm", "msg", "sent", "message_id", msgID, "token_prefix", tokenPrefix(token))
	return nil
}

func (s *firebaseSender) SendDataOnly(ctx context.Context, token string, data map[string]string) error {
	if token == "" {
		return nil
	}
	msg := &messaging.Message{
		Token: token,
		Data:  data,
		Android: &messaging.AndroidConfig{
			Priority: "high",
		},
		// No Notification payload — ensures onMessageReceived is always called,
		// even when the app is in background. The app builds its own notification
		// (e.g. with Approve/Decline action buttons for track requests).
	}
	msgID, err := s.client.Send(ctx, msg)
	if err != nil {
		slog.Warn("fcm", "msg", "sendDataOnly failed", "token_prefix", tokenPrefix(token), "err", err)
		return err
	}
	slog.Info("fcm", "msg", "sentDataOnly", "message_id", msgID, "token_prefix", tokenPrefix(token))
	return nil
}

func tokenPrefix(t string) string {
	if len(t) <= 12 {
		return t
	}
	return t[:12] + "..."
}
