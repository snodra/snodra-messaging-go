package snodra

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TLSConfig is TLS towards the hubs (see [TLS]).
type TLSConfig struct {
	// CAFile is a PEM file of the CA certificates that sign the hubs' certificates, read again
	// at every connection (a rotated CA applies at the next one); "" trusts the system's roots
	// (SNODRA_SERVER_TLS_CA).
	CAFile string
	// ServerName is the name the hubs' certificates must carry, also sent as SNI; "" is the host
	// of each hub address (SNODRA_SERVER_TLS_SERVER_NAME).
	ServerName string
}

// TLS makes every hub connection TLS (1.2 or later), verifying the hub's certificate against
// cfg's CA and server name. A hub with TLS on accepts nothing else.
func TLS(cfg TLSConfig) Option {
	return func(c *config) error {
		c.tls = &TLSConfig{CAFile: strings.TrimSpace(cfg.CAFile), ServerName: strings.TrimSpace(cfg.ServerName)}
		return nil
	}
}

// ClientCertificate makes the instance present a client certificate to the hubs (mutual TLS):
// certFile holds the chain, leaf first, keyFile its private key (PEM). It implies [TLS] (with
// the system's roots unless TLS is set). The leaf must carry exactly one URI SAN
// snodra://service/<name> naming the service; [New] checks the pair. The files are read at
// every connection and every 3 s: a renewed valid pair is rolled over to before the presented
// certificate's notAfter (when the hub closes the session), one hub at a time, after a random
// delay of at most a tenth of the presented certificate's remaining validity (at most 30 s).
// A pair that cannot be used keeps the one presented.
func ClientCertificate(certFile, keyFile string) Option {
	return func(c *config) error {
		c.clientCert, c.clientKey = strings.TrimSpace(certFile), strings.TrimSpace(keyFile)
		return nil
	}
}

// readCAFile is the pool of the certificates in the PEM file path.
func readCAFile(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot be read: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("holds no PEM certificate")
	}
	return pool, nil
}

// systemRootsWith is the system's roots plus the certificates in the PEM file path.
func systemRootsWith(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot be read: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("holds no PEM certificate")
	}
	return pool, nil
}

// transport is the transport credentials of one connection attempt: plain text without TLS;
// else TLS with the CA file read now, presenting id when there is one.
func (c *hubConn) transport(id *clientIdentity) (credentials.TransportCredentials, error) {
	t := c.app.cfg.tls
	if t == nil {
		return insecure.NewCredentials(), nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.tlsServerName()}
	if t.CAFile != "" {
		pool, err := readCAFile(t.CAFile)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "the hub CA file '%s' %v", t.CAFile, err)
		}
		cfg.RootCAs = pool
	}
	if id != nil {
		cert := id.cert()
		// Presented whatever CAs the hub names: the hub decides.
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil }
	}
	return credentials.NewTLS(cfg), nil
}

// tlsServerName is the name the hub's certificate is checked against: the configured one; else,
// for a resolved address, its DNS name, not its IP; else "" (the dialled host).
func (c *hubConn) tlsServerName() string {
	if t := c.app.cfg.tls; t != nil && t.ServerName != "" {
		return t.ServerName
	}
	return c.serverName
}

// serviceSANPrefix is the URI SAN prefix that names a service.
const serviceSANPrefix = "snodra://service/"

// clientIdentity is a client certificate pair that was read and checked: both files parse, the
// key is the leaf's, the leaf names one service.
type clientIdentity struct {
	cert        func() tls.Certificate // behind a closure: no printing reaches the key
	service     string
	fingerprint string   // the first 8 hex characters of the SHA-256 of the leaf: what is logged
	digest      [32]byte // over both files: tells a renewed pair from the one in use
	notBefore   time.Time
	notAfter    time.Time
}

// String shows the service, the fingerprint and the validity, never the key.
func (id *clientIdentity) String() string {
	return fmt.Sprintf("clientIdentity{service: %s, fingerprint: %s, notAfter: %s}", id.service, id.fingerprint,
		id.notAfter.UTC().Format(time.RFC3339))
}

// Format prints String whatever the verb.
func (id *clientIdentity) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, id.String()) }

// LogValue is String, for log/slog.
func (id *clientIdentity) LogValue() slog.Value { return slog.StringValue(id.String()) }

// certProblem is why a client certificate pair cannot be used.
type certProblem int

const (
	certUnusable     certProblem = iota // a file cannot be read or parsed, or names no service
	certWrongService                    // the leaf names another service: the hub refuses it
	certExpired
	certNotYetValid
)

// certError is a pair that cannot be used; its text names the files, never their content.
type certError struct {
	problem certProblem
	msg     string
}

func (e *certError) Error() string { return e.msg }

func certErr(p certProblem, format string, args ...any) *certError {
	return &certError{problem: p, msg: fmt.Sprintf(format, args...)}
}

// validServiceName is the hub's rule: 1 to 128 characters of A-Z a-z 0-9 . _ -.
func validServiceName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// loadClientIdentity reads and checks the pair in certFile and keyFile for service. Its
// validity period is not judged here (validAt).
func loadClientIdentity(certFile, keyFile, service string) (*clientIdentity, error) {
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, certErr(certUnusable, "the client certificate file '%s' cannot be read: %v", certFile, err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, certErr(certUnusable, "the client key file '%s' cannot be read: %v", keyFile, err)
	}
	if !hasPEM(certPEM, func(t string) bool { return t == "CERTIFICATE" }) {
		return nil, certErr(certUnusable, "the client certificate file '%s' holds no PEM certificate", certFile)
	}
	if !hasPEM(keyPEM, func(t string) bool { return strings.HasSuffix(t, "PRIVATE KEY") }) {
		return nil, certErr(certUnusable, "the client key file '%s' holds no PEM private key", keyFile)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		// The parser's errors carry no key material; still, only their kind is said.
		if strings.Contains(err.Error(), "does not match") {
			return nil, certErr(certUnusable, "the client key '%s' does not match the first certificate in '%s' (the leaf goes first)", keyFile, certFile)
		}
		return nil, certErr(certUnusable, "the client certificate '%s' or its key '%s' cannot be used: it must be PEM, the key RSA, ECDSA or Ed25519 (PKCS#1, SEC 1 or PKCS#8)", certFile, keyFile)
	}
	leaf := pair.Leaf
	if leaf == nil {
		if leaf, err = x509.ParseCertificate(pair.Certificate[0]); err != nil {
			return nil, certErr(certUnusable, "the first certificate in '%s' cannot be parsed", certFile)
		}
	}
	var named []string
	for _, u := range leaf.URIs {
		if s := u.String(); strings.HasPrefix(s, serviceSANPrefix) {
			named = append(named, strings.TrimPrefix(s, serviceSANPrefix))
		}
	}
	switch {
	case len(named) == 0:
		return nil, certErr(certUnusable, "the client certificate in '%s' carries no %s<name> URI SAN, so the hub cannot tell which service it is", certFile, serviceSANPrefix)
	case len(named) > 1:
		return nil, certErr(certUnusable, "the client certificate in '%s' carries more than one %s<name> URI SAN; the hub refuses it", certFile, serviceSANPrefix)
	case !validServiceName(named[0]):
		return nil, certErr(certUnusable, "the client certificate in '%s' names a service the hub would refuse (1 to 128 characters of A-Z a-z 0-9 . _ -)", certFile)
	case named[0] != service:
		return nil, certErr(certWrongService, "the client certificate in '%s' names service '%s', but this service is '%s': the hub requires the same name", certFile, named[0], service)
	}
	sum := sha256.Sum256(leaf.Raw)
	h := sha256.New()
	_ = binary.Write(h, binary.BigEndian, uint64(len(certPEM)))
	h.Write(certPEM)
	h.Write(keyPEM)
	id := &clientIdentity{
		cert:        func() tls.Certificate { return pair },
		service:     named[0],
		fingerprint: hex.EncodeToString(sum[:4]),
		notBefore:   leaf.NotBefore,
		notAfter:    leaf.NotAfter,
	}
	copy(id.digest[:], h.Sum(nil))
	return id, nil
}

// hasPEM is true when data holds a PEM block whose type is wanted.
func hasPEM(data []byte, wanted func(string) bool) bool {
	for {
		var b *pem.Block
		b, data = pem.Decode(data)
		if b == nil {
			return false
		}
		if wanted(b.Type) {
			return true
		}
		if len(bytes.TrimSpace(data)) == 0 {
			return false
		}
	}
}

// validAt is an error when the leaf's validity has not begun or has ended at now.
func (id *clientIdentity) validAt(now time.Time) error {
	if now.Before(id.notBefore) {
		return certErr(certNotYetValid, "the client certificate %s (%s) is not valid before %s", id.fingerprint, id.service, id.notBefore.UTC().Format(time.RFC3339))
	}
	if !now.Before(id.notAfter) {
		return certErr(certExpired, "the client certificate %s (%s) expired at %s", id.fingerprint, id.service, id.notAfter.UTC().Format(time.RFC3339))
	}
	return nil
}

// loadIdentity is the pair a connection attempt presents: read from the files now and checked
// (valid now). A problem with the files (a rotation caught half way, another service's pair, an
// expired one) falls back to the last good pair while it has not expired, warned once per
// distinct problem; without one the attempt fails before a stream is opened: PERMISSION_DENIED
// for another service, UNAUTHENTICATED for an expired pair (refusals), else FAILED_PRECONDITION.
// Called from the connection's goroutine only.
func (c *hubConn) loadIdentity() (*clientIdentity, error) {
	cfg := c.app.cfg
	now := time.Now()
	id, err := loadClientIdentity(cfg.clientCert, cfg.clientKey, cfg.service)
	if err == nil {
		err = id.validAt(now)
	}
	if err == nil {
		c.identity, c.identityProblem = id, ""
		return id, nil
	}
	if last := c.identity; last != nil && now.Before(last.notAfter) {
		if err.Error() != c.identityProblem {
			c.identityProblem = err.Error()
			c.log.Warn("the client certificate files cannot be used; keeping the last good pair",
				"reason", err.Error(), "fingerprint", last.fingerprint)
		}
		return last, nil
	}
	code := codes.FailedPrecondition
	var ce *certError
	if errors.As(err, &ce) {
		switch ce.problem {
		case certWrongService:
			code = codes.PermissionDenied
		case certExpired:
			code = codes.Unauthenticated
		}
	}
	return nil, status.Error(code, err.Error())
}

// rollDelayBound is the bound of the random delay before a rollover: limit, or a tenth of what
// the presented certificate has left, whichever is smaller (zero once it has expired).
// TODO(spec-gap): SG-8 (the specification says "a random delay").
func rollDelayBound(limit, remaining time.Duration) time.Duration {
	return max(0, min(limit, remaining/10))
}

// expiryWarningDue is true when a certificate valid from notBefore to notAfter has no more than
// a tenth of its lifetime (at most a day) left at now.
func expiryWarningDue(notBefore, notAfter, now time.Time) bool {
	threshold := min(notAfter.Sub(notBefore)/10, 24*time.Hour)
	return notAfter.Sub(now) <= threshold
}

// certWatch reads the client certificate files every certCheckInterval and rolls the hub
// connections over to a renewed pair (see [ClientCertificate]).
// TODO(spec-gap): SG-8 (the poll, the delay's bound and the rollover's limits).
type certWatch struct {
	app *App
	p   *pool

	problem       string // the last problem with the files, and whether it was logged at error
	problemLogged bool
	waiting       [32]byte // the not yet valid pair logged about
	warned        string   // the fingerprint warned about as near its end
	delayed       [32]byte // the pair the random delay was waited for
	refused       [32]byte // the pair a hub did not take back in time
}

// watchCertificates runs the watch until the shutdown.
func (p *pool) watchCertificates(a *App) {
	w := &certWatch{app: a, p: p}
	ticker := time.NewTicker(a.cfg.timings.certCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-a.stopped:
			return
		case <-a.killed:
			return
		}
		w.check()
	}
}

func (w *certWatch) stopping() bool {
	select {
	case <-w.app.stopped:
		return true
	case <-w.app.killed:
		return true
	default:
		return false
	}
}

// check looks at the files once: a valid pair that a registered connection is not presenting
// is rolled over to, after the random delay (once per pair), one hub at a time in order.
func (w *certWatch) check() {
	cfg := w.app.cfg
	now := time.Now()
	disk, err := loadClientIdentity(cfg.clientCert, cfg.clientKey, cfg.service)
	valid := false
	if err == nil {
		switch verr := disk.validAt(now); {
		case verr == nil:
			valid = true
		case isProblem(verr, certNotYetValid):
			if w.waiting != disk.digest {
				w.waiting = disk.digest
				w.app.log.Info("the renewed client certificate is not valid yet; the connections keep the one they present until it is",
					"reason", verr.Error())
			}
		default:
			w.noteProblem(verr)
			disk = nil
		}
	} else {
		w.noteProblem(err)
	}
	if disk != nil {
		w.problem, w.problemLogged = "", false
	}
	w.warnNearExpiry(disk, now)
	if !valid || disk.digest == w.refused {
		return
	}
	w.refused = [32]byte{}
	if disk = w.waitBeforeRolling(disk); disk == nil {
		return
	}
	for _, c := range w.p.all() {
		if w.stopping() {
			return
		}
		if c.presentsOther(disk.digest) && !c.rollOver(disk.fingerprint) {
			w.refused = disk.digest
			w.app.log.Error("the hub did not register again with the renewed client certificate in time; the other hubs keep the certificate they have until the files change again",
				"hub", c.address, "fingerprint", disk.fingerprint)
			return
		}
	}
}

func isProblem(err error, p certProblem) bool {
	var ce *certError
	return errors.As(err, &ce) && ce.problem == p
}

// noteProblem logs a problem with the files at error once it is still there a period later (a
// pair replaced in two writes is caught half way for a moment), at debug otherwise.
func (w *certWatch) noteProblem(err error) {
	msg := err.Error()
	if msg == w.problem {
		if !w.problemLogged {
			w.problemLogged = true
			w.app.log.Error("the client certificate files cannot be used; the connections keep the certificate they present", "reason", msg)
		}
		return
	}
	w.problem, w.problemLogged = msg, false
	w.app.log.Debug("the client certificate files cannot be used right now", "reason", msg)
}

// warnNearExpiry warns once per certificate when the one presented is in the last tenth of its
// life (at most the last day) and the files hold no other one.
func (w *certWatch) warnNearExpiry(disk *clientIdentity, now time.Time) {
	var presented *clientIdentity
	for _, c := range w.p.all() {
		if presented = c.presented(); presented != nil {
			break
		}
	}
	if presented == nil || (disk != nil && disk.digest != presented.digest) {
		return
	}
	if !expiryWarningDue(presented.notBefore, presented.notAfter, now) || w.warned == presented.fingerprint {
		return
	}
	w.warned = presented.fingerprint
	w.app.log.Warn("the client certificate expires soon and the file holds no renewed one; the hub closes the session at notAfter",
		"fingerprint", presented.fingerprint, "not_after", presented.notAfter, "file", w.app.cfg.clientCert)
}

// waitBeforeRolling waits, once per pair and only while a registered connection presents
// another one, a random time uniform in [0, rollDelayBound], then reads the files again and
// returns the pair they hold then; nil when the roll is not to go on (shutdown, or no usable
// pair any more: the next tick decides).
func (w *certWatch) waitBeforeRolling(disk *clientIdentity) *clientIdentity {
	if w.delayed == disk.digest {
		return disk
	}
	remaining, stale := time.Duration(0), false
	for _, c := range w.p.all() {
		if p := c.presented(); p != nil && p.digest != disk.digest {
			if left := time.Until(p.notAfter); !stale || left < remaining {
				remaining, stale = left, true
			}
		}
	}
	if !stale {
		return disk
	}
	w.delayed = disk.digest
	bound := rollDelayBound(w.app.cfg.timings.rollDelayMax, remaining)
	delay := time.Duration(rand.Float64() * float64(bound))
	if delay <= 0 {
		return disk
	}
	w.app.log.Info("a renewed client certificate is in the files; rolling the hub connections over to it after a random delay",
		"fingerprint", disk.fingerprint, "delay", delay)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-w.app.stopped:
		return nil
	case <-w.app.killed:
		return nil
	}
	cfg := w.app.cfg
	current, err := loadClientIdentity(cfg.clientCert, cfg.clientKey, cfg.service)
	if err == nil {
		err = current.validAt(time.Now())
	}
	if err != nil {
		return nil
	}
	w.delayed = current.digest
	return current
}

// presented is the client certificate of the connection's registered stream; nil without one.
func (c *hubConn) presented() *clientIdentity {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.acked == nil {
		return nil
	}
	return c.presentedCert
}

// presentsOther is true when the connection's registered stream presents another pair than
// digest.
func (c *hubConn) presentsOther(digest [32]byte) bool {
	p := c.presented()
	return p != nil && p.digest != digest
}

// rollOver moves the connection to the pair now in the files: its stream gets a Register with
// no types (the hub routes elsewhere), its requests get at most rollIdleLimit to finish, it is
// half-closed, and the connection reconnects at once, presenting the files' pair. True once a
// new stream is registered (or there was nothing to roll), false when none is within
// rollReconnectLimit.
func (c *hubConn) rollOver(fingerprint string) bool {
	ref, _ := c.live()
	if ref == nil {
		return true
	}
	c.setRolling(true)
	defer c.setRolling(false)
	c.log.Info("rolling the hub connection over to the renewed client certificate", "fingerprint", fingerprint)
	select { // a request for an earlier stream that ended before taking it
	case <-c.rollReq:
	default:
	}
	select {
	case c.rollReq <- ref:
	default:
	}
	t := c.t
	ended := time.NewTimer(rollSettle + t.rollIdleLimit + t.closeWait + time.Second)
	defer ended.Stop()
	select {
	case <-ref.gone:
	case <-ended.C:
		return false
	case <-c.stopping:
		return true
	case <-c.app.killed:
		return true
	}
	deadline := time.Now().Add(t.rollReconnectLimit)
	for {
		if r, _ := c.live(); r != nil && r != ref {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-time.After(rollPoll):
		case <-c.stopping:
			return true
		case <-c.app.killed:
			return true
		}
	}
}

func (c *hubConn) setRolling(on bool) {
	c.mu.Lock()
	c.rolling = on
	c.mu.Unlock()
}

// isRolling is true while the connection is being rolled over to a renewed certificate.
func (c *hubConn) isRolling() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rolling
}

// stopCtx is a context that ends when the connection stops (shutdown or hard stop); cancel
// releases it.
func (c *hubConn) stopCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-c.stopping:
		case <-c.app.killed:
		case <-ctx.Done():
		}
		cancel()
	}()
	return ctx, cancel
}
