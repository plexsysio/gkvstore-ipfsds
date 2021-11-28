package ipfsdsStore_test

import (
	"testing"

	"github.com/ipfs/go-datastore"
	ipfsdsStore "github.com/plexsysio/gkvstore-ipfsds"
	"github.com/plexsysio/gkvstore/testsuite"
)

func TestSuite(t *testing.T) {
	testsuite.RunTestsuite(t, ipfsdsStore.New(datastore.NewMapDatastore()), testsuite.Advanced)
}

func BenchmarkSuite(b *testing.B) {
	testsuite.BenchmarkSuite(b, ipfsdsStore.New(datastore.NewMapDatastore()))
}
