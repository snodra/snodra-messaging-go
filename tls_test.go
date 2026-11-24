package snodra

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
)

// testCA is a certificate authority made for one test; nothing it signs is written to the
// repository.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

var serials atomic.Int64

func newTestCA(t testing.TB, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serials.Add(1)),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// leaf is a certificate the CA signed and its key, PEM.
type leaf struct {
	certPEM, keyPEM []byte
	serial          *big.Int
}

func (ca *testCA) issue(t testing.TB, tmpl *x509.Certificate) leaf {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl.SerialNumber = big.NewInt(serials.Add(1))
	if tmpl.NotBefore.IsZero() {
		tmpl.NotBefore = time.Now().Add(-time.Hour)
	}
	if tmpl.NotAfter.IsZero() {
		tmpl.NotAfter = time.Now().Add(24 * time.Hour)
	}
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return leaf{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}),
		serial:  tmpl.SerialNumber,
	}
}

// server is a hub certificate for name.
func (ca *testCA) server(t testing.TB, name string) tls.Certificate {
	t.Helper()
	l := ca.issue(t, &x509.Certificate{DNSNames: []string{name}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	pair, err := tls.X509KeyPair(l.certPEM, l.keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

// client is a client certificate naming service, valid until notAfter (zero: a day).
func (ca *testCA) client(t testing.TB, service string, notAfter time.Time) leaf {
	t.Helper()
	u, _ := url.Parse("snodra://service/" + service)
	return ca.issue(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: service}, URIs: []*url.URL{u}, NotAfter: notAfter,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
}

func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

// caFile writes the CA's certificate to a file of the test.
func (ca *testCA) caFile(t testing.TB) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	writeFile(t, path, string(ca.pem))
	return path
}

// pairFiles writes l to cert.pem and key.pem in dir, the key first.
func pairFiles(t testing.TB, dir string, l leaf) (certFile, keyFile string) {
	t.Helper()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeFile(t, keyFile, string(l.keyPEM))
	writeFile(t, certFile, string(l.certPEM))
	return certFile, keyFile
}

func TestTLSWithTheOperatorsCA(t *testing.T) {
	ca := newTestCA(t, "operator CA")
	hub := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), nil))
	startApp(t, Hubs(hub.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"}))
	is(t, hub.Next(t).RecvRegister(t).GetServiceName(), "orders-service")
}

func TestTLSRefusesAnotherCAOrName(t *testing.T) {
	ca, other := newTestCA(t, "operator CA"), newTestCA(t, "another CA")
	strange := fakehub.Start(t, fakehub.WithTLS(other.server(t, "hub.test"), nil))
	startApp(t, Hubs(strange.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"}))
	strange.NoStreamWithin(t, 2*time.Second)

	named := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), nil))
	startApp(t, Hubs(named.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "other.test"}))
	named.NoStreamWithin(t, 2*time.Second)
}

func TestTheCAFileIsReadAgainAtEveryConnect(t *testing.T) {
	old, renewed := newTestCA(t, "old CA"), newTestCA(t, "renewed CA")
	caFile := old.caFile(t)
	hub := fakehub.Start(t, fakehub.WithTLS(renewed.server(t, "hub.test"), nil))
	startApp(t, Hubs(hub.Addr()), TLS(TLSConfig{CAFile: caFile, ServerName: "hub.test"}))
	hub.NoStreamWithin(t, 300*ms)
	writeFile(t, caFile, string(renewed.pem))
	hub.Next(t).RecvRegister(t)
}

func TestTheClientCertificateIsPresented(t *testing.T) {
	ca := newTestCA(t, "operator CA")
	hub := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), ca.pool()))
	certFile, keyFile := pairFiles(t, t.TempDir(), ca.client(t, "orders-service", time.Time{}))
	startApp(t, Hubs(hub.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"}),
		ClientCertificate(certFile, keyFile))
	s := hub.Next(t)
	s.RecvRegister(t)
	if len(s.PeerCertificates) == 0 || len(s.PeerCertificates[0].URIs) != 1 {
		t.Fatalf("presented %v", s.PeerCertificates)
	}
	is(t, s.PeerCertificates[0].URIs[0].String(), "snodra://service/orders-service")
}

func TestARenewedCertificateIsPresentedBeforeNotAfter(t *testing.T) {
	ca := newTestCA(t, "operator CA")
	hub := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), ca.pool()))
	dir := t.TempDir()
	first := ca.client(t, "orders-service", time.Now().Add(time.Hour))
	certFile, keyFile := pairFiles(t, dir, first)
	tm := scaled()
	tm.certCheckInterval, tm.rollDelayMax = 50*ms, 0
	app := newApp(t, Hubs(hub.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"}),
		ClientCertificate(certFile, keyFile), withTimings(tm))
	is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) { return Empty{}, nil }), nil)
	is(t, app.Start(context.Background()), nil)
	s := hub.Next(t)
	is(t, len(s.RecvRegister(t).GetCommandTypes()), 1)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, s.PeerCertificates[0].SerialNumber.Cmp(first.serial), 0)

	renewed := ca.client(t, "orders-service", time.Now().Add(2*time.Hour))
	pairFiles(t, dir, renewed)
	start := time.Now()
	is(t, len(s.RecvRegister(t).GetCommandTypes()), 0) // out of the routing first
	s.HalfClosed(t, time.Second)
	s.Close(codes.OK, "")
	s2 := hub.Next(t)
	s2.RecvRegister(t)
	is(t, s2.PeerCertificates[0].SerialNumber.Cmp(renewed.serial), 0)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the renewed certificate came after %v", d)
	}
}

func TestACertificateExpiredCloseIsAnOrdinaryReconnect(t *testing.T) { // SG-15's exception
	ca := newTestCA(t, "operator CA")
	hub := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), ca.pool()))
	certFile, keyFile := pairFiles(t, t.TempDir(), ca.client(t, "orders-service", time.Time{}))
	app := startApp(t, Hubs(hub.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"}),
		ClientCertificate(certFile, keyFile), ReconnectBackoff(50*ms, 3*s))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	closed := time.Now()
	s.Close(codes.Unauthenticated, "certificate expired")
	hub.Next(t).RecvRegister(t)
	if d := time.Since(closed); d > time.Second { // a refusal would wait 2.4 to 3 s
		t.Fatalf("reconnected after %v, want the backoff floor", d)
	}
	if rej, ok := app.Rejection(); ok {
		t.Fatalf("the close was kept as a refusal: %+v", rej)
	}
}

// Only the hub's "certificate expired" close of a certificate session is an ordinary reconnect;
// another UNAUTHENTICATED close of it is a refusal.
func TestAnotherUnauthenticatedCloseOfACertificateSessionIsARefusal(t *testing.T) { // SG-15
	ca := newTestCA(t, "operator CA")
	hub := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), ca.pool()))
	certFile, keyFile := pairFiles(t, t.TempDir(), ca.client(t, "orders-service", time.Time{}))
	app := startApp(t, Hubs(hub.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"}),
		ClientCertificate(certFile, keyFile), ReconnectBackoff(50*ms, 1*s))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	closed := time.Now()
	s.Close(codes.Unauthenticated, "client certificate revoked")
	hub.Next(t).RecvRegister(t)
	within(t, time.Since(closed), 750*ms, 2*time.Second) // 80-100 % of the cap
	rej, ok := app.Rejection()
	is(t, ok, true)
	is(t, rej.Message, "client certificate revoked")
}

func TestAnUnauthenticatedCloseWithoutACertificateIsARefusal(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()), Token(testToken))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	s.Close(codes.Unauthenticated, "token revoked")
	hub.Next(t).RecvRegister(t)
	rej, ok := app.Rejection()
	is(t, ok, true)
	is(t, rej.Message, "token revoked")
}

func TestRollDelayBound(t *testing.T) {
	is(t, rollDelayBound(30*time.Second, 100*time.Second), 10*time.Second)
	is(t, rollDelayBound(30*time.Second, time.Hour), 30*time.Second)
	is(t, rollDelayBound(30*time.Second, -time.Second), time.Duration(0))
}

func TestACertificateForAnotherServiceIsRefusedAtStart(t *testing.T) {
	ca := newTestCA(t, "operator CA")
	dir := t.TempDir()
	certFile, keyFile := pairFiles(t, dir, ca.client(t, "billing-service", time.Time{}))
	_, err := New(Service("orders-service"), Hubs("hub:7000"), ClientCertificate(certFile, keyFile))
	if err == nil || !strings.Contains(err.Error(), "billing-service") || !strings.Contains(err.Error(), "orders-service") {
		t.Fatalf("New: %v", err)
	}
}

func TestAClientCertificatePairIsChecked(t *testing.T) {
	ca := newTestCA(t, "operator CA")
	dir := t.TempDir()
	good := ca.client(t, "orders-service", time.Time{})
	certFile, keyFile := pairFiles(t, dir, good)
	otherKey := filepath.Join(dir, "other-key.pem")
	writeFile(t, otherKey, string(ca.client(t, "orders-service", time.Time{}).keyPEM))
	expiredDir := t.TempDir()
	service, _ := url.Parse("snodra://service/orders-service")
	expiredCert, expiredKey := pairFiles(t, expiredDir, ca.issue(t, &x509.Certificate{
		URIs: []*url.URL{service}, NotBefore: time.Now().Add(-2 * time.Hour), NotAfter: time.Now().Add(-time.Hour),
	}))
	noSANCert, noSANKey := pairFiles(t, t.TempDir(), ca.issue(t, &x509.Certificate{DNSNames: []string{"orders.example"}}))
	for _, c := range []struct {
		what, cert, key, want string
	}{
		{"a missing certificate", filepath.Join(dir, "absent.pem"), keyFile, "cannot be read"},
		{"only the certificate", certFile, "", "both"},
		{"the key of another certificate", certFile, otherKey, "does not match"},
		{"the key file as the certificate", keyFile, keyFile, "no PEM certificate"},
		{"an expired certificate", expiredCert, expiredKey, "expired"},
		{"no service name", noSANCert, noSANKey, "snodra://service/"},
	} {
		_, err := New(Service("orders-service"), Hubs("hub:7000"), ClientCertificate(c.cert, c.key))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v", c.what, err)
		}
		if body := keyBody(good.keyPEM); strings.Contains(err.Error(), body) {
			t.Fatalf("%s: the error shows key material", c.what)
		}
	}
}

// keyBody is a piece of a PEM key's base64 body.
func keyBody(keyPEM []byte) string {
	lines := strings.Split(string(keyPEM), "\n")
	return lines[1][10:30]
}

func TestACredentialOverPlainTextIsWarnedOnce(t *testing.T) {
	const warning = "the instance's credential goes to hubs reached without TLS: it travels unencrypted"
	var buf lockedBuffer
	rec := &logRecorder{}
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()), Token(testToken),
		Logger(slog.New(teeHandler{rec, slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})})))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetToken(), testToken) // the wire is unchanged
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	s.Close(codes.Unavailable, "restart") // a reconnect warns nothing more
	hub.Next(t).RecvRegister(t)
	is(t, rec.count(slog.LevelWarn, warning), 1)
	rec.mu.Lock()
	for _, r := range rec.records {
		if r.Message == warning {
			var attrs []string
			r.Attrs(func(a slog.Attr) bool { attrs = append(attrs, a.String()); return true })
			if !strings.Contains(strings.Join(attrs, " "), hub.Addr()) {
				t.Errorf("the warning does not name the hub: %v", attrs)
			}
		}
	}
	rec.mu.Unlock()
	if strings.Contains(buf.String(), testToken) {
		t.Fatalf("the log shows the token: %s", buf.String())
	}

	// Nothing to warn about: TLS, an https:// hub, or no credential.
	ca := newTestCA(t, "operator CA")
	tlsHub := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), nil))
	for name, opts := range map[string][]Option{
		"TLS":           {Hubs(tlsHub.Addr()), Token(testToken), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"})},
		"https://":      {Hubs("https://" + tlsHub.Addr()), JWT(staticSource{})},
		"no credential": {Hubs(fakehub.Start(t).Addr())},
	} {
		quiet := &logRecorder{}
		startApp(t, append(opts, Logger(slog.New(quiet)))...)
		if n := quiet.count(slog.LevelWarn, warning); n != 0 {
			t.Fatalf("%s: warned %d times", name, n)
		}
	}
}

// teeHandler hands every record to both handlers.
type teeHandler struct{ a, b slog.Handler }

func (h teeHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h teeHandler) Handle(ctx context.Context, r slog.Record) error {
	_ = h.a.Handle(ctx, r.Clone())
	return h.b.Handle(ctx, r)
}
func (h teeHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return teeHandler{h.a.WithAttrs(as), h.b.WithAttrs(as)}
}
func (h teeHandler) WithGroup(g string) slog.Handler {
	return teeHandler{h.a.WithGroup(g), h.b.WithGroup(g)}
}
