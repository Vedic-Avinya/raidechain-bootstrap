package integrity

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	playintegrity "google.golang.org/api/playintegrity/v1"
	"google.golang.org/api/option"
)

// Verifier checks Play Integrity tokens via Google's server-side API.
// See https://developer.android.com/google/play/integrity/verdict#decrypt-verify-google-servers
type Verifier struct {
	service     *playintegrity.Service
	packageName string // e.g. "com.ridechain.rider"
	mu          sync.Mutex
}

// Verdict is the parsed result of a Play Integrity token verification.
type Verdict struct {
	DeviceIntegrity []string // e.g. ["MEETS_DEVICE_INTEGRITY"]
	AppRecognized   bool     // true if appRecognitionVerdict == "PLAY_RECOGNIZED"
	PackageName     string   // package name from the token
	LicensingOk     bool     // true if licensingVerdict is "LICENSED"
	RequestHash     string   // nonce / request hash from the token
}

// NewVerifier creates a Play Integrity verifier.
// Reads GOOGLE_APPLICATION_CREDENTIALS (or uses default credentials) for auth.
// packageName should be "com.ridechain.rider" (or ".debug" for debug builds).
func NewVerifier(packageName string) (*Verifier, error) {
	ctx := context.Background()

	var svc *playintegrity.Service
	var err error

	// Use explicit credentials file if set, otherwise use Application Default Credentials.
	credsFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credsFile != "" {
		svc, err = playintegrity.NewService(ctx, option.WithCredentialsFile(credsFile))
	} else {
		svc, err = playintegrity.NewService(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("playintegrity.NewService: %w", err)
	}

	slog.Info("play_integrity_verifier", "package", packageName, "msg", "initialized")
	return &Verifier{service: svc, packageName: packageName}, nil
}

// Verify decodes and validates a Play Integrity token.
// Returns the verdict or an error if the token is invalid / API call fails.
func (v *Verifier) Verify(ctx context.Context, token string) (*Verdict, error) {
	if token == "" {
		return nil, fmt.Errorf("empty integrity token")
	}

	req := &playintegrity.DecodeIntegrityTokenRequest{
		IntegrityToken: token,
	}

	resp, err := v.service.V1.DecodeIntegrityToken(v.packageName, req).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("DecodeIntegrityToken: %w", err)
	}

	payload := resp.TokenPayloadExternal
	if payload == nil {
		return nil, fmt.Errorf("nil token payload")
	}

	verdict := &Verdict{}

	// Device integrity labels (e.g. MEETS_DEVICE_INTEGRITY, MEETS_BASIC_INTEGRITY)
	if payload.DeviceIntegrity != nil {
		verdict.DeviceIntegrity = payload.DeviceIntegrity.DeviceRecognitionVerdict
	}

	// App integrity
	if payload.AppIntegrity != nil {
		verdict.AppRecognized = payload.AppIntegrity.AppRecognitionVerdict == "PLAY_RECOGNIZED"
		verdict.PackageName = payload.AppIntegrity.PackageName
	}

	// Account details (licensing)
	if payload.AccountDetails != nil {
		verdict.LicensingOk = payload.AccountDetails.AppLicensingVerdict == "LICENSED"
	}

	// Request details (nonce)
	if payload.RequestDetails != nil {
		verdict.RequestHash = payload.RequestDetails.RequestHash
	}

	return verdict, nil
}

// MeetsBasicIntegrity checks if the device passes at least basic integrity.
func (vd *Verdict) MeetsBasicIntegrity() bool {
	for _, label := range vd.DeviceIntegrity {
		l := strings.ToUpper(label)
		if l == "MEETS_BASIC_INTEGRITY" || l == "MEETS_DEVICE_INTEGRITY" || l == "MEETS_STRONG_INTEGRITY" {
			return true
		}
	}
	return false
}

// MeetsDeviceIntegrity checks for full device integrity (not rooted, verified boot).
func (vd *Verdict) MeetsDeviceIntegrity() bool {
	for _, label := range vd.DeviceIntegrity {
		if strings.ToUpper(label) == "MEETS_DEVICE_INTEGRITY" {
			return true
		}
	}
	return false
}
