package fcm

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

func newFirebaseSenderImpl(ctx context.Context, credsPath, credsJSON string) (PushSender, error) {
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
	// When opts is empty, Firebase uses Application Default Credentials (ADC).
	// On GCP Compute Engine that is the VM's service account; grant it Firebase roles in IAM.
	app, err := firebase.NewApp(ctx, nil, opts...)
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
	slog.Info("fcm", "msg", "initialized; will send push when rider peer is offline", "auth", source)
	return &firebaseSender{client: client}, nil
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

func tokenPrefix(t string) string {
	if len(t) <= 12 {
		return t
	}
	return t[:12] + "..."
}
