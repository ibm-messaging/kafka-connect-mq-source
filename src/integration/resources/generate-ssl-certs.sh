#!/bin/bash
# Script to generate SSL certificates for MQ integration tests

# Don't exit on error for keytool warnings about trusted certs
set +e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/ssl-certs"

# Check if certificates already exist
if [ -f "$CERT_DIR/client.jks" ] && [ -f "$CERT_DIR/truststore.jks" ] && [ -f "$CERT_DIR/server.p12" ]; then
    echo "SSL certificates already exist in $CERT_DIR - skipping generation"
    echo "To regenerate, delete the ssl-certs directory and run again"
    exit 0
fi

# Clean up existing certificates
rm -rf "$CERT_DIR"
mkdir -p "$CERT_DIR"

echo "Generating SSL certificates for MQ integration tests..."

# Find openssl.cnf location
OPENSSL_CNF=""
for path in /etc/ssl/openssl.cnf /usr/lib/ssl/openssl.cnf /etc/pki/tls/openssl.cnf /System/Library/OpenSSL/openssl.cnf; do
    if [ -f "$path" ]; then
        OPENSSL_CNF="$path"
        break
    fi
done

# Generate CA private key and certificate using openssl (with CA:TRUE constraint)
if [ -n "$OPENSSL_CNF" ]; then
    openssl req -new -x509 -days 3650 -keyout "$CERT_DIR/ca.key" \
        -out "$CERT_DIR/ca.crt" -passout pass:password \
        -subj "/C=US/ST=Test/L=Test/O=IBM/OU=Testing/CN=Test CA" \
        -extensions v3_ca -config <(cat "$OPENSSL_CNF" <(printf "\n[v3_ca]\nbasicConstraints=CA:TRUE\nkeyUsage=keyCertSign,cRLSign"))
else
    # Fallback without config file
    openssl req -new -x509 -days 3650 -keyout "$CERT_DIR/ca.key" \
        -out "$CERT_DIR/ca.crt" -passout pass:password \
        -subj "/C=US/ST=Test/L=Test/O=IBM/OU=Testing/CN=Test CA"
fi

# Import CA cert and key into JKS keystore for keytool operations
openssl pkcs12 -export -in "$CERT_DIR/ca.crt" -inkey "$CERT_DIR/ca.key" \
    -passin pass:password -out "$CERT_DIR/ca.p12" -passout pass:password -name ca

keytool -importkeystore -srckeystore "$CERT_DIR/ca.p12" -srcstoretype PKCS12 \
    -srcstorepass password -destkeystore "$CERT_DIR/ca.jks" -deststorepass password

# Generate server keystore for MQ
keytool -genkeypair -alias mqserver -keyalg RSA -keysize 2048 -validity 3650 \
    -dname "CN=localhost, OU=Testing, O=IBM, L=Test, ST=Test, C=US" \
    -keystore "$CERT_DIR/server.jks" -storepass password -keypass password

# Generate CSR for server
keytool -certreq -alias mqserver -keystore "$CERT_DIR/server.jks" \
    -storepass password -file "$CERT_DIR/server.csr"

# Sign server certificate with CA
keytool -gencert -alias ca -keystore "$CERT_DIR/ca.jks" -storepass password \
    -infile "$CERT_DIR/server.csr" -outfile "$CERT_DIR/server.crt" -validity 3650

# Import CA cert into server keystore (trusted cert, no keypass needed)
# Ignore warning about trusted certificates not being password-protected
keytool -importcert -alias ca -keystore "$CERT_DIR/server.jks" \
    -storepass password -file "$CERT_DIR/ca.crt" -noprompt 2>/dev/null || true

# Import signed server cert into server keystore
keytool -importcert -alias mqserver -keystore "$CERT_DIR/server.jks" \
    -storepass password -keypass password -file "$CERT_DIR/server.crt" 2>/dev/null

# Create client keystore
keytool -genkeypair -alias mqclient -keyalg RSA -keysize 2048 -validity 3650 \
    -dname "CN=MQ Client, OU=Testing, O=IBM, L=Test, ST=Test, C=US" \
    -keystore "$CERT_DIR/client.jks" -storepass password -keypass password

# Generate CSR for client
keytool -certreq -alias mqclient -keystore "$CERT_DIR/client.jks" \
    -storepass password -file "$CERT_DIR/client.csr"

# Sign client certificate with CA
keytool -gencert -alias ca -keystore "$CERT_DIR/ca.jks" -storepass password \
    -infile "$CERT_DIR/client.csr" -outfile "$CERT_DIR/client.crt" -validity 3650

# Import CA cert into client keystore (trusted cert, no keypass needed)
# Ignore warning about trusted certificates not being password-protected
keytool -importcert -alias ca -keystore "$CERT_DIR/client.jks" \
    -storepass password -file "$CERT_DIR/ca.crt" -noprompt 2>/dev/null || true

# Import signed client cert into client keystore
keytool -importcert -alias mqclient -keystore "$CERT_DIR/client.jks" \
    -storepass password -keypass password -file "$CERT_DIR/client.crt" 2>/dev/null

# Create truststore with CA certificate
keytool -importcert -alias ca -keystore "$CERT_DIR/truststore.jks" \
    -storepass password -file "$CERT_DIR/ca.crt" -noprompt

# Convert server keystore to PKCS12 for MQ
keytool -importkeystore -srckeystore "$CERT_DIR/server.jks" \
    -srcstorepass password -destkeystore "$CERT_DIR/server.p12" \
    -deststoretype PKCS12 -deststorepass password -destkeypass password

# Extract private key and certificate from PKCS12 for MQ (PEM format)
# MQ prefers separate key and cert files
openssl pkcs12 -in "$CERT_DIR/server.p12" -passin pass:password \
    -nocerts -nodes -out "$CERT_DIR/server.key"

openssl pkcs12 -in "$CERT_DIR/server.p12" -passin pass:password \
    -nokeys -out "$CERT_DIR/server-cert.pem"

# Set proper permissions
chmod 600 "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/server-cert.pem"
chmod 644 "$CERT_DIR/ca.crt"

echo "SSL certificates generated successfully in $CERT_DIR"
echo "Files created:"
echo "  - ca.jks (CA keystore)"
echo "  - ca.crt (CA certificate in PEM format)"
echo "  - server.jks (MQ server keystore)"
echo "  - server.p12 (MQ server keystore in PKCS12 format)"
echo "  - server.key (MQ server private key in PEM format)"
echo "  - server-cert.pem (MQ server certificate in PEM format)"
echo "  - client.jks (Client keystore for tests)"
echo "  - truststore.jks (Truststore with CA cert)"
