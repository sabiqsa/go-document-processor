package memory_test

import (
	"testing"

	"github.com/sabiqsa/go-document-processor/docproc"
	"github.com/sabiqsa/go-document-processor/docproc/memory"
	"github.com/sabiqsa/go-document-processor/docproc/repotest"
)

func TestRepositoryContract(t *testing.T) {
	repotest.Run(t, func(*testing.T) docproc.Repository { return memory.New() })
}
