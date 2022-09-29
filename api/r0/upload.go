package r0

import (
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/turt2live/matrix-media-repo/api"
	"github.com/turt2live/matrix-media-repo/common"
	"github.com/turt2live/matrix-media-repo/common/rcontext"
	"github.com/turt2live/matrix-media-repo/controllers/info_controller"
	"github.com/turt2live/matrix-media-repo/controllers/upload_controller"
	"github.com/turt2live/matrix-media-repo/internal_cache"
	"github.com/turt2live/matrix-media-repo/quota"
	"github.com/turt2live/matrix-media-repo/storage"
	"github.com/turt2live/matrix-media-repo/storage/datastore"
	"github.com/turt2live/matrix-media-repo/util"
	"github.com/turt2live/matrix-media-repo/util/cleanup"
)

type MediaUploadedResponse struct {
	ContentUri string `json:"content_uri"`
	Blurhash   string `json:"xyz.amorgan.blurhash,omitempty"`
}

type MediaCreatedResponse struct {
	ContentUri      string `json:"content_uri"`
	UnusedExpiresAt int64  `json:"unused_expires_at"`
	UploadURL       string `json:"upload_url,omitempty"`
}

func CreateMedia(r *http.Request, rctx rcontext.RequestContext, user api.UserInfo) interface{} {
	media, ds, err := upload_controller.CreateMedia(r.Host, rctx)
	if err != nil {
		rctx.Log.Error("Unexpected error creating media reference: " + err.Error())
		return api.InternalServerError("Unexpected Error")
	}

	uploadURL := ""
	if ds.ShouldRedirectUpload() {
		rctx.Log.Debug("getting pre-signed upload URL")
		var location string
		uploadURL, location, err = ds.GetUploadURL(rctx)
		if err != nil {
			rctx.Log.Error("error getting pre-signed upload URL: ", err)
			uploadURL = "" // just in case
		} else {
			rctx.Log.Debug("got upload URL: ", uploadURL)
			media.Location = location
		}
	} else {
		rctx.Log.Debug("not redirecting upload")
	}

	if err = upload_controller.PersistMedia(media, user.UserId, rctx); err != nil {
		rctx.Log.Error("Unexpected error persisting media reference: " + err.Error())
		return api.InternalServerError("Unexpected Error")
	}

	roomID := r.URL.Query().Get("room_id")
	if roomID != "" {
		if err = upload_controller.AddMediaReference(media.Origin, media.MediaId, roomID, rctx); err != nil {
			rctx.Log.Error("error storing room reference for media upload: ", err)
			return api.InternalServerError("Unexpected Error")
		}
	}

	return &MediaCreatedResponse{
		ContentUri:      media.MxcUri(),
		UnusedExpiresAt: time.Now().Unix() + int64(rctx.Config.Features.MSC2246Async.AsyncUploadExpirySecs),
		UploadURL:       uploadURL,
	}
}

func UploadComplete(r *http.Request, rctx rcontext.RequestContext, user api.UserInfo) interface{} {
	params := mux.Vars(r)
	server := params["server"]
	mediaId := params["mediaId"]

	defer cleanup.DumpAndCloseStream(r.Body)

	rctx = rctx.LogWithFields(logrus.Fields{
		"server":  server,
		"mediaId": mediaId,
	})

	rctx.Log.Debug("handling upload complete callback")

	if server != "" && (!util.IsServerOurs(server) || server != r.Host) {
		rctx.Log.Debug("got upload_complete for media from a different server, 404ing")
		return api.NotFoundError()
	}

	db := storage.GetDatabase().GetMediaStore(rctx)

	media, err := db.Get(server, mediaId)
	if err != nil {
		rctx.Log.Debug("got upload_complete for media that isn't in db, 404ing")
		return api.NotFoundError()
	}

	if media.Location == "" {
		rctx.Log.Warn("received upload_complete for a media with no location")
		return api.NotFoundError()
	}

	if media.SizeBytes > 0 {
		rctx.Log.Info("received upload_complete for a media which already has a size set, not re-scanning")
		return struct{}{}
	}

	ds, err := datastore.LocateDatastore(rctx, media.DatastoreId)
	if err != nil {
		rctx.Log.Warn("error getting datasource for upload_complete: ", err)
		return api.InternalServerError("unexpected error processing upload")
	}

	info, err := ds.ObjectInfo(rctx, media.Location)
	if err != nil {
		rctx.Log.Error("error getting info about object for upload_complete: ", err)
		return api.InternalServerError("unexpected error processing upload")
	}

	media.ContentType = info.ContentType
	media.SizeBytes = info.Size

	if err := db.Update(media); err != nil {
		rctx.Log.Error("error updating media in db: ", err)
		return api.InternalServerError("unexpected error processing upload")
	}

	util.NotifyUpload(rctx, server, mediaId)

	if err := internal_cache.Get().NotifyUpload(server, mediaId, rctx); err != nil {
		rctx.Log.Warn("Unexpected error trying to notify cache about media: " + err.Error())
	}

	go func() {
		// Download the file to get the hash
		f, err := ds.DownloadFile(media.Location)
		if err != nil {
			rctx.Log.Error("error getting uploaded file for upload_complete: ", err)
			return
		}
		defer f.Close()

		hash, err := util.GetSha256HashOfStream(f)
		if err != nil {
			rctx.Log.Error("error hashing uploaded file: ", err)
			return
		}

		media.Sha256Hash = hash

		// db variable used in parent function will have a cancelled context by the time we get here
		outOfContextDB := storage.GetDatabase().GetMediaStore(rcontext.Initial())
		if err := outOfContextDB.Update(media); err != nil {
			rctx.Log.Error("error updating media entry in db: ", err)
			return
		}
	}()

	rctx.Log.Debug("finished handling upload_complete")

	return &MediaUploadedResponse{
		ContentUri: media.MxcUri(),
	}
}

func UploadMedia(r *http.Request, rctx rcontext.RequestContext, user api.UserInfo) interface{} {
	var server = ""
	var mediaId = ""

	filename := filepath.Base(r.URL.Query().Get("filename"))
	defer cleanup.DumpAndCloseStream(r.Body)

	if rctx.Config.Features.MSC2246Async.Enabled {
		params := mux.Vars(r)
		server = params["server"]
		mediaId = params["mediaId"]
	}

	rctx = rctx.LogWithFields(logrus.Fields{
		"server":   server,
		"mediaId":  mediaId,
		"filename": filename,
	})

	if server != "" && (!util.IsServerOurs(server) || server != r.Host) {
		return api.NotFoundError()
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream" // binary
	}

	if upload_controller.IsRequestTooLarge(r.ContentLength, r.Header.Get("Content-Length"), rctx) {
		io.Copy(ioutil.Discard, r.Body) // Ditch the entire request
		return api.RequestTooLarge()
	}

	if upload_controller.IsRequestTooSmall(r.ContentLength, r.Header.Get("Content-Length"), rctx) {
		io.Copy(ioutil.Discard, r.Body) // Ditch the entire request
		return api.RequestTooSmall()
	}

	inQuota, err := quota.IsUserWithinQuota(rctx, user.UserId)
	if err != nil {
		io.Copy(ioutil.Discard, r.Body) // Ditch the entire request
		rctx.Log.Error("Unexpected error checking quota: " + err.Error())
		sentry.CaptureException(err)
		return api.InternalServerError("Unexpected Error")
	}
	if !inQuota {
		io.Copy(ioutil.Discard, r.Body) // Ditch the entire request
		return api.QuotaExceeded()
	}

	contentLength := upload_controller.EstimateContentLength(r.ContentLength, r.Header.Get("Content-Length"))
	media, err := upload_controller.UploadMedia(r.Body, contentLength, contentType, filename, user.UserId, r.Host, mediaId, rctx)
	if err != nil {
		io.Copy(ioutil.Discard, r.Body) // Ditch the entire request

		if err == common.ErrMediaQuarantined {
			return api.BadRequest("This file is not permitted on this server")
		} else if err == common.ErrCannotOverwriteMedia {
			return api.CannotOverwriteMedia()
		} else if err == common.ErrMediaNotFound {
			return api.NotFoundError()
		}

		rctx.Log.Error("Unexpected error storing media: " + err.Error())
		sentry.CaptureException(err)
		return api.ServiceUnavailable()
	}

	if rctx.Config.Features.MSC2448Blurhash.Enabled && r.URL.Query().Get("xyz.amorgan.generate_blurhash") == "true" {
		hash, err := info_controller.GetOrCalculateBlurhash(media, rctx)
		if err != nil {
			rctx.Log.Warn("Failed to calculate blurhash: " + err.Error())
			sentry.CaptureException(err)
		}

		return &MediaUploadedResponse{
			ContentUri: media.MxcUri(),
			Blurhash:   hash,
		}
	}

	roomID := r.URL.Query().Get("room_id")
	if roomID != "" {
		if err = upload_controller.AddMediaReference(media.Origin, media.MediaId, roomID, rctx); err != nil {
			rctx.Log.Error("error storing room reference for media upload: ", err)
			return api.InternalServerError("unexpected error")
		}
	}

	return &MediaUploadedResponse{
		ContentUri: media.MxcUri(),
	}
}
