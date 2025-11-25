package smpp

import (
	"log"
	"time"

	"github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu"
)

type transceiver struct {
	trx *smpp.Transceiver
}

func newTransceiver(host, systemID, password, systemType string) *transceiver {
	trx := &smpp.Transceiver{
		Addr:       host,
		User:       systemID,
		Passwd:     password,
		SystemType: systemType,

		// Auto reconnect logic
		BindInterval: 5 * time.Second,

		// Rate limit (optional)
		// RateLimit: 10, // 10 req/sec

		// Handler for Delivery Receipts + MO messages
		Handler: func(p pdu.Body) {
			log.Printf("Received PDU: %#v\n", p)
		},
	}

	return &transceiver{trx: trx}

}
