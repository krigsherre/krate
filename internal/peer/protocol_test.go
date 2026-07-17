package peer

import (
	"testing"

	kratev1 "github.com/krigsherre/krate/internal/peer/peerpb"
)

func TestValidateTransferRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     *kratev1.TransferRequest
		wantErr bool
	}{
		{"valid", &kratev1.TransferRequest{LimitKey: "k", Requested: 1, OriginId: "o"}, false},
		{"missing key", &kratev1.TransferRequest{Requested: 1, OriginId: "o"}, true},
		{"zero requested", &kratev1.TransferRequest{LimitKey: "k", OriginId: "o"}, true},
		{"missing origin", &kratev1.TransferRequest{LimitKey: "k", Requested: 1}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTransferRequest(tc.req)
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}
