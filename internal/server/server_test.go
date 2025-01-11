/*
 * Copyright 2018-2024 Burak Sezer
 * Copyright 2025 Arsene Tochemey Gandote
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"

	"github.com/tochemey/olric/pkg/flog"
	"github.com/tochemey/olric/pkg/testkit"
)

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func newServerWithPreConditionFunc(t *testing.T, preCond func(conn redcon.Conn, cmd redcon.Command) bool) *Server {
	bindPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	l := log.New(os.Stdout, "server-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, fl)
	s.SetPreConditionFunc(preCond)

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	t.Cleanup(func() {
		err = s.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	return s
}

func newTLSServerWithPreConditionFunc(t *testing.T, preCond func(conn redcon.Conn, cmd redcon.Command) bool) (*Server, *redis.Client) {
	bindPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	srvConfig, clientConfig := testkit.GetServerAndClientTLSConfig(t)

	l := log.New(os.Stdout, "server-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
		TLSConfig:       srvConfig,
	}
	s := New(c, fl)
	s.SetPreConditionFunc(preCond)

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	t.Cleanup(func() {
		err = s.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	return s, redis.NewClient(&redis.Options{
		Addr:      net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort)),
		TLSConfig: clientConfig,
	})
}

func newServer(t *testing.T) *Server {
	srv := newServerWithPreConditionFunc(t, nil)
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})
	return srv
}

func TestServer_RESP(t *testing.T) {
	s := newServer(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort)),
	})
	respEcho(t, s, rdb)
}

func TestServer_RESP_Stats(t *testing.T) {
	s := newServer(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort)),
	})
	respEcho(t, s, rdb)

	require.NotEqual(t, int64(0), CommandsTotal.Read())
	require.NotEqual(t, int64(0), ConnectionsTotal.Read())
	require.NotEqual(t, int64(0), CurrentConnections.Read())
	require.NotEqual(t, int64(0), WrittenBytesTotal.Read())
	require.NotEqual(t, int64(0), ReadBytesTotal.Read())
}

func TestTSLServer_RESP(t *testing.T) {
	srv, rdb := newTLSServerWithPreConditionFunc(t, nil)
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})

	respEcho(t, srv, rdb)
}

func generateKeyAndCSR(t *testing.T) ([]byte, []byte) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	key := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaKey),
	})

	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			Country:      []string{"US"},
			Locality:     []string{"San Francisco"},
			Organization: []string{"Awesomeness, Inc."},
			Province:     []string{"California"},
		},
		SignatureAlgorithm: x509.SHA256WithRSA,
		IPAddresses:        []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	req, err := x509.CreateCertificateRequest(rand.Reader, template, rsaKey)
	require.NoError(t, err)

	csr := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: req,
	})

	return key, csr
}

func generateRootCert(t *testing.T, key crypto.Signer) (*x509.Certificate, []byte) {
	subjectKeyIdentifier := calculateSubjectKeyIdentifier(t, key.Public())

	template := &x509.Certificate{
		SerialNumber: generateSerial(t),
		Subject: pkix.Name{
			Organization: []string{"Awesomeness, Inc."},
			Country:      []string{"US"},
			Locality:     []string{"San Francisco"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		SubjectKeyId:          subjectKeyIdentifier,
		AuthorityKeyId:        subjectKeyIdentifier,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	require.NoError(t, err)

	rootCert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	rootCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})

	return rootCert, rootCertPEM
}

// generateSerial generates a serial number using the maximum number of octets (20) allowed by RFC 5280 4.1.2.2
// (Adapted from https://github.com/cloudflare/cfssl/blob/828c23c22cbca1f7632b9ba85174aaa26e745340/signer/local/local.go#L407-L418)
func generateSerial(t *testing.T) *big.Int {
	serialNumber := make([]byte, 20)
	_, err := io.ReadFull(rand.Reader, serialNumber)
	require.NoError(t, err)

	return new(big.Int).SetBytes(serialNumber)
}

// calculateSubjectKeyIdentifier implements a common method to generate a key identifier
// from a public key, namely, by composing it from the 160-bit SHA-1 hash of the bit string
// of the public key (cf. https://tools.ietf.org/html/rfc5280#section-4.2.1.2).
// (Adapted from https://github.com/jsha/minica/blob/master/main.go).
func calculateSubjectKeyIdentifier(t *testing.T, pubKey crypto.PublicKey) []byte {
	spkiASN1, err := x509.MarshalPKIXPublicKey(pubKey)
	require.NoError(t, err)

	var spki struct {
		Algorithm        pkix.AlgorithmIdentifier
		SubjectPublicKey asn1.BitString
	}
	_, err = asn1.Unmarshal(spkiASN1, &spki)
	require.NoError(t, err)

	skid := sha1.Sum(spki.SubjectPublicKey.Bytes)
	return skid[:]
}

// signCSR signs a certificate signing request with the given CA certificate and private key
func signCSR(t *testing.T, csr []byte, caKey crypto.Signer, caCert *x509.Certificate) []byte {
	block, _ := pem.Decode(csr)
	require.NotNil(t, block, "failed to decode CSR")

	certificateRequest, err := x509.ParseCertificateRequest(block.Bytes)
	require.NoError(t, err)

	require.NoError(t, certificateRequest.CheckSignature())

	template := x509.Certificate{
		Subject:               certificateRequest.Subject,
		PublicKeyAlgorithm:    certificateRequest.PublicKeyAlgorithm,
		PublicKey:             certificateRequest.PublicKey,
		SignatureAlgorithm:    certificateRequest.SignatureAlgorithm,
		Signature:             certificateRequest.Signature,
		SerialNumber:          generateSerial(t),
		Issuer:                caCert.Issuer,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		SubjectKeyId:          calculateSubjectKeyIdentifier(t, certificateRequest.PublicKey),
		BasicConstraintsValid: true,
		IPAddresses:           certificateRequest.IPAddresses,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, certificateRequest.PublicKey, caKey)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
}

// generateKey generates a 1024-bit RSA private key
func generateKey(t *testing.T) (crypto.Signer, []byte) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	return key, keyPEM
}
