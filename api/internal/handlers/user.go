package handlers

import (
	"context"
	"log/slog"
	"net/http"
)

type UserRequest struct {
	First *string `json:"first"`
	Last  *string `json:"last"`
}

func (u *UserRequest) Valid() bool {
	if u.First == nil || u.Last == nil {
		return false
	}
	return true
}

type UserResponse struct {
	Id string `json:"id"`
}

type UserStore interface {
	Create(context.Context, string, string) (string, error)
}

type UserHandler struct {
	logger    *slog.Logger
	userStore UserStore
}

func NewUserHandler(logger *slog.Logger, store UserStore) http.Handler {
	return &UserHandler{logger: logger, userStore: store}
}

func (h *UserHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data, err := decodeRequest[UserRequest](r)

	if err != nil {
		h.logger.Error("error decoding request", slog.Any("error", err))
		writeServerErrorResponse(w, "cannot decode request")
		return
	}

	if !data.Valid() {
		writeClientErrorResponse(w, "invalid request")
		return
	}

	id, err := h.userStore.Create(r.Context(), *data.First, *data.Last)
	if err != nil {
		h.logger.Error("error creating user", slog.Any("error", err))
		writeClientErrorResponse(w, "invalid user")
		return
	}
	writeJSONResponse(w, http.StatusOK, id)

}
