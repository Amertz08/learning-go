package handlers

import (
	"encoding/json"
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

func SumHandler(w http.ResponseWriter, r *http.Request) {
	var data SumRequest

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		writeServerErrorResponse(w, "could not decode request")
		return
	}
	if !data.Valid() {
		writeClientErrorResponse(w, "missing parameters")
		return
	}

	resp := &SumResponse{Sum: *data.A + *data.B}
	writeJSONResponse(w, http.StatusOK, resp)
}
