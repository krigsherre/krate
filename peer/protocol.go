package peer

import (
	"errors"

	kratev1 "github.com/krigsherre/krate/peer/peerpb"
)

func ValidateTransferRequest(req *kratev1.TransferRequest) error {
	if req.LimitKey == "" {
		return errors.New("missing limit_key")
	}
	if req.Requested == 0 {
		return errors.New("requested must be > 0")
	}
	if req.OriginId == "" {
		return errors.New("missing origin_id")
	}
	return nil
}
