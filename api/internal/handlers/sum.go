package handlers

import (
	"log/slog"
	"net/http"
)

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
