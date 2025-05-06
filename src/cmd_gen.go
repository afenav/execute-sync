package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"

	"github.com/gofiber/fiber/v2/log"
	"github.com/urfave/cli/v2"
)

func GenCommand() *cli.Command {
	return &cli.Command{
		Name:        "gen",
		Usage:       "Snowflake Generate Keypair",
		Description: "Generate RSA Keypair for Snowflake's JWT Auth",
		Action: func(cCtx *cli.Context) error {
			privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				log.Fatalf("Error generating RSA key: %v", err)
			}

			pkcs8DER, err := x509.MarshalPKCS8PrivateKey(privateKey)
			if err != nil {
				log.Fatalf("Error marshaling key to PKCS#8: %v", err)
			}

			b64 := base64.URLEncoding.EncodeToString(pkcs8DER)

			fmt.Println("============================================================")
			fmt.Println("Snowflake Key for privateKey parameter in DSN:")
			fmt.Println("============================================================")
			fmt.Println(b64)
			fmt.Println()

			// Encode the private key to PEM format
			privatePEM := pem.EncodeToMemory(&pem.Block{
				Type:  "PRIVATE KEY",
				Bytes: pkcs8DER,
			})

			fmt.Println("============================================================")
			fmt.Println("Snowflake Key in PEM format:")
			fmt.Println("============================================================")
			fmt.Println(string(privatePEM))
			fmt.Println()

			pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
			if err != nil {
				log.Fatalf("Error marshaling public key: %v", err)
			}
			pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes})

			fmt.Println("============================================================")
			fmt.Println("Public Key (PEM) - Register in Snowflake with:")
			fmt.Println("ALTER USER xxx SET RSA_PUBLIC_KEY='<paste below>';")
			fmt.Println("============================================================")
			fmt.Print(string(pubPEM))
			fmt.Println("============================================================")
			return nil
		},
	}
}
