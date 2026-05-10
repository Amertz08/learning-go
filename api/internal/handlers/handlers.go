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

type SumRequest struct {
	A *int `json:"a"`
	B *int `json:"b"`
}

func (r *SumRequest) Valid() bool {
	if r.A == nil || r.B == nil {
		return false
	}
	return true
}

type SumResponse struct {
	Sum int `json:"sum"`
}

type SumHandler struct {
	logger *slog.Logger

	Service SumService
}

type SumService func(int, int) int

func NewSumHandler(logger *slog.Logger, serv SumService) http.Handler {
	return &SumHandler{
		Service: serv,
		logger:  logger,
	}
}

func (s *SumHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data, err := decodeRequest[SumRequest](r)

	if err != nil {
		s.logger.Error("error decoding response", slog.Any("error", err))
		writeServerErrorResponse(w, "cannot decode request")
		return
	}

	if !data.Valid() {
		writeClientErrorResponse(w, "missing parameters")
		return
	}

	resp := &SumResponse{Sum: s.Service(*data.A, *data.B)}
	writeJSONResponse(w, http.StatusOK, resp)
}

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
	Create(string, string) (string, error)
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

	id, err := h.userStore.Create(*data.First, *data.Last)
	if err != nil {
		h.logger.Error("error creating user", slog.Any("error", err))
		writeClientErrorResponse(w, "invalid user")
		return
	}
	writeJSONResponse(w, http.StatusOK, id)

}
