package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type IndexResponse struct {
	Params url.Values `json:"params"`
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	for k, v := range r.URL.Query() {
		fmt.Println(k, v)
	}
	resp := &IndexResponse{Params: r.URL.Query()}

	writeJSONResponse(w, http.StatusOK, resp)
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
	Service func(int, int) int
}

func (s *SumHandler) Handle(w http.ResponseWriter, r *http.Request) {
	var data SumRequest

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		writeServerErrorResponse(w, "could not decode request")
		return
	}
	if !data.Valid() {
		writeClientErrorResponse(w, "missing parameters")
		return
	}

	resp := &SumResponse{Sum: s.Service(*data.A, *data.B)}
	writeJSONResponse(w, http.StatusOK, resp)
}
