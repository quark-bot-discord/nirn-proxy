package lib

import (
	"io"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestMain(m *testing.M) {
	l := logrus.New()
	l.SetOutput(io.Discard)
	SetLogger(l)
	os.Exit(m.Run())
}
