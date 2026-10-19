# TLS test identity

`localhost.cert.der` is a self-signed ECDSA certificate for `localhost`.
`localhost.key.der` is its **publicly committed, test-only** PKCS#8 private key.
Never use this identity outside tests. Tests trust only this certificate and do
not load system roots or contact external services.

Generate replacements with OpenSSL (the certificate is valid for 100 years):

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -noenc \
  -subj /CN=localhost -addext subjectAltName=DNS:localhost \
  -addext basicConstraints=critical,CA:FALSE \
  -addext keyUsage=critical,digitalSignature \
  -addext extendedKeyUsage=serverAuth -days 36500 \
  -keyout key.pem -out cert.pem
openssl x509 -in cert.pem -outform DER -out localhost.cert.der
openssl pkcs8 -topk8 -nocrypt -in key.pem -outform DER -out localhost.key.der
rm key.pem cert.pem
```
