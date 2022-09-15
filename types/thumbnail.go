package types

import (
	"io"
)

type Thumbnail struct {
	Origin      string
	MediaId     string
	Width       int
	Height      int
	Method      string // "crop" or "scale"
	Animated    bool
	ContentType string
	SizeBytes   int64
	DatastoreId string
	Location    string
	CreationTs  int64
	Sha256Hash  string
}

type StreamedOrRedirectedThumbnail struct {
	Thumbnail   *Thumbnail
	Stream      io.ReadCloser
	RedirectURL string
}
