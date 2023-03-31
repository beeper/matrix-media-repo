package r0

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/turt2live/matrix-media-repo/api"
	"github.com/turt2live/matrix-media-repo/common/rcontext"
	"github.com/turt2live/matrix-media-repo/controllers/upload_controller"
)

type AddMediaReferenceBody struct {
	RoomID string `json:"room_id"`
}

func AddMediaReference(r *http.Request, rctx rcontext.RequestContext, user api.UserInfo) interface{} {
	params := mux.Vars(r)
	server := params["server"]
	mediaID := params["mediaId"]
	rctx = rctx.LogWithFields(logrus.Fields{
		"server":  server,
		"mediaId": mediaID,
	})

	defer r.Body.Close()
	body := AddMediaReferenceBody{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return api.BadRequest("error parsing request body as json")
	}

	rctx.Log.Info("media referenced in room ", body.RoomID)

	if err := upload_controller.AddMediaReference(server, mediaID, body.RoomID, rctx); err != nil {
		rctx.Log.Error("error storing room reference for media upload: ", err)
		return api.InternalServerError("unexpected error")
	}
	return api.EmptyResponse{}
}
