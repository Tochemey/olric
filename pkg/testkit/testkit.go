/*
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

package testkit

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

// GenerateRootCert generates a root certificate for unit tests
func GenerateRootCert(t *testing.T, key crypto.Signer) (*x509.Certificate, []byte) {
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
	if err != nil {
		t.Fatal(err)
	}

	rootCert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	rootCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})

	return rootCert, rootCertPEM
}

// SignCSR signs a certificate signing request with the given CA certificate and private key
func SignCSR(t *testing.T, csr []byte, caKey crypto.Signer, caCert *x509.Certificate) []byte {
	block, _ := pem.Decode(csr)
	if block == nil {
		t.Fatal("failed to decode csr")
	}

	certificateRequest, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	if err := certificateRequest.CheckSignature(); err != nil {
		t.Fatal(err)
	}

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
	if err != nil {
		t.Fatal(err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
}

// GenerateKey generates a 2048-bit RSA private key
func GenerateKey(t *testing.T) (crypto.Signer, []byte) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	return key, keyPEM
}

// GetServerAndClientTLSConfig returns both server and client TLS configs useful for unit tests
func GetServerAndClientTLSConfig(t *testing.T) (serverConfig *tls.Config, clientConfig *tls.Config) {
	caKey, _ := GenerateKey(t)
	caCert, _ := GenerateRootCert(t, caKey)

	clientKeyPEM, deviceCsrPEM := generateKeyAndCSR(t)
	clientCertPEM := SignCSR(t, deviceCsrPEM, caKey, caCert)
	clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	serverKeyPEM, serverCsrPEM := generateKeyAndCSR(t)
	serverCertPEM := SignCSR(t, serverCsrPEM, caKey, caCert)
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	clientPool := x509.NewCertPool()
	clientPool.AddCert(caCert)

	serverPool := x509.NewCertPool()
	serverPool.AddCert(caCert)

	return &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientPool,
		}, &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      serverPool,
		}
}

func generateKeyAndCSR(t *testing.T) ([]byte, []byte) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}

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
	if err != nil {
		t.Fatal(err)
	}

	csr := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: req,
	})

	return key, csr
}

// generateSerial generates a serial number using the maximum number of octets (20) allowed by RFC 5280 4.1.2.2
// (Adapted from https://github.com/cloudflare/cfssl/blob/828c23c22cbca1f7632b9ba85174aaa26e745340/signer/local/local.go#L407-L418)
func generateSerial(t *testing.T) *big.Int {
	serialNumber := make([]byte, 20)
	_, err := io.ReadFull(rand.Reader, serialNumber)
	if err != nil {
		t.Fatal(err)
	}

	return new(big.Int).SetBytes(serialNumber)
}

// calculateSubjectKeyIdentifier implements a common method to generate a key identifier
// from a public key, namely, by composing it from the 160-bit SHA-1 hash of the bit string
// of the public key (cf. https://tools.ietf.org/html/rfc5280#section-4.2.1.2).
// (Adapted from https://github.com/jsha/minica/blob/master/main.go).
func calculateSubjectKeyIdentifier(t *testing.T, pubKey crypto.PublicKey) []byte {
	spkiASN1, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		t.Fatal(err)
	}

	var spki struct {
		Algorithm        pkix.AlgorithmIdentifier
		SubjectPublicKey asn1.BitString
	}
	_, err = asn1.Unmarshal(spkiASN1, &spki)
	if err != nil {
		t.Fatal(err)
	}

	skid := sha1.Sum(spki.SubjectPublicKey.Bytes)
	return skid[:]
}
