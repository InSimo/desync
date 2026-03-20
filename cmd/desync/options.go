package main

import (
	"errors"

	"github.com/folbricht/desync/cmd/shared/cmdshared"
	"github.com/spf13/pflag"
)

// cmdStoreOptions is an alias for the shared type in cmdshared.
type cmdStoreOptions = cmdshared.CmdStoreOptions

// addStoreOptions registers common store option flags on f and links them to o.
func addStoreOptions(o *cmdStoreOptions, f *pflag.FlagSet) {
	cmdshared.AddStoreOptions(o, f)
}

// cmdServerOptions hold command line options used in HTTP servers.
type cmdServerOptions struct {
	cert      string
	key       string
	mutualTLS bool
	clientCA  string
	auth      string
}

func (o cmdServerOptions) validate() error {
	if (o.key == "") != (o.cert == "") {
		return errors.New("--key and --cert options need to be provided together")
	}
	return nil
}

// Add common HTTP server options to a command flagset.
func addServerOptions(o *cmdServerOptions, f *pflag.FlagSet) {
	f.StringVar(&o.cert, "cert", "", "cert file in PEM format, requires --key")
	f.StringVar(&o.key, "key", "", "key file in PEM format, requires --cert")
	f.BoolVar(&o.mutualTLS, "mutual-tls", false, "require valid client certificate")
	f.StringVar(&o.clientCA, "client-ca", "", "acceptable client certificate or CA")
	f.StringVar(&o.auth, "authorization", "", "expected value of the authorization header in requests")
}
