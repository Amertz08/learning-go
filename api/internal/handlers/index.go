package handlers

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
)

type IndexResponse struct {
	Params url.Values `json:"params"`
}

func IndexHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for k, v := range r.URL.Query() {
			fmt.Println(k, v)
		}
		logger.Info("hit index")

		resp := &IndexResponse{Params: r.URL.Query()}

		writeJSONResponse(w, http.StatusOK, resp)
	}
}
