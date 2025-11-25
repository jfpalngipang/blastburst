package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	baseAPIURL = "https://sms.8x8.com/api/v1/subaccounts"
	subAccount = "XN_OTP"
	apiToken   = "GL7UudBX8SfGqqKr65wp5cMJceVT5VChAFn4vTOKfE" // TODO: move to env
)

type SendSMSRequest struct {
	To   string `json:"to"`
	From string `json:"from"`
	Text string `json:"text"`
}

type BatchSMSRequest struct {
	From     string           `json:"from"`
	Messages []SendSMSRequest `json:"messages"`
}

// queue of batches to send to 8x8
var batchQueue = make(chan []SendSMSRequest, 100000)

// ============================
// JSON BATCH ENDPOINT (/batch-send)
// ============================

func batchSendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}

	var req BatchSMSRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, "messages array is empty", http.StatusBadRequest)
		return
	}

	// default From if not provided at message level
	for i := range req.Messages {
		if req.Messages[i].From == "" {
			if req.From != "" {
				req.Messages[i].From = req.From
			} else {
				req.Messages[i].From = "MegaPerya"
			}
		}
		if req.Messages[i].To == "" || req.Messages[i].Text == "" {
			http.Error(w, "each message must have 'to' and 'text'", http.StatusBadRequest)
			return
		}
		req.Messages[i].To = normalizeNumber(req.Messages[i].To)
	}

	// Split into batches of max 1000 (8x8 limit)
	const batchSize = 1000
	total := len(req.Messages)
	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}

		chunk := make([]SendSMSRequest, end-i)
		copy(chunk, req.Messages[i:end])

		batchQueue <- chunk
	}

	resp := map[string]any{
		"status":         "accepted",
		"total_messages": total,
		"total_batches":  (total + batchSize - 1) / batchSize,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ============================
// Workers
// ============================

func worker(id int) {
	log.Printf("[Worker %d] started\n", id)

	for batch := range batchQueue {
		log.Printf("[Worker %d] processing batch of %d messages\n", id, len(batch))
		err := sendBatchTo8x8(batch)
		if err != nil {
			log.Printf("[Worker %d] batch failed, retrying… error=%v\n", id, err)
			time.Sleep(2 * time.Second)
			// simple retry: requeue the same batch
			batchQueue <- batch
		}
	}
}

// ============================
// 8x8 BATCH SENDER
// ============================

func sendBatchTo8x8(batch []SendSMSRequest) error {
	payload := make([]map[string]string, 0, len(batch))

	for _, m := range batch {
		payload = append(payload, map[string]string{
			"source":      m.From,
			"destination": m.To,
			"text":        m.Text,
			"encoding":    "AUTO",
		})
	}

	body, _ := json.Marshal(map[string]any{
		"messages": payload,
	})

	// correct batch endpoint
	url := baseAPIURL + "/" + subAccount + "/messages/batch"

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)
	log.Printf("8x8 batch response (%d): %s\n", resp.StatusCode, string(responseBody))

	if resp.StatusCode >= 300 {
		return errors.New("8x8 batch API returned non-2xx status")
	}

	return nil
}

// ============================
// SINGLE SEND JSON ENDPOINT (/send-sms)
// ============================

func sendSMSHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}

	var req SendSMSRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if req.To == "" || req.Text == "" {
		http.Error(w, "fields 'to' and 'text' are required", http.StatusBadRequest)
		return
	}
	if req.From == "" {
		req.From = "MegaPerya"
	}
	req.To = normalizeNumber(req.To)

	payload := map[string]string{
		"source":      req.From,
		"destination": req.To,
		"text":        req.Text,
		"encoding":    "AUTO",
	}
	jsonPayload, _ := json.Marshal(payload)

	url := baseAPIURL + "/" + subAccount + "/messages"

	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		http.Error(w, "failed building request", http.StatusInternalServerError)
		return
	}

	httpReq.Header.Set("Authorization", "Bearer "+apiToken)
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Do(httpReq)
	if err != nil {
		log.Println("8x8 REST error:", err)
		http.Error(w, "failed to send SMS", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)
}

// ============================
// CSV UPLOAD ENDPOINT (/upload-batch)
// ============================

func uploadBatchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST with multipart/form-data", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "failed to parse multipart form", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing 'file' field", http.StatusBadRequest)
		return
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	reader.FieldsPerRecord = -1 // allow variable columns

	const batchSize = 1000
	batch := make([]SendSMSRequest, 0, batchSize)
	total := 0
	batches := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("CSV read error:", err)
			continue
		}

		// Expected CSV columns: to, text, from(optional)
		if len(record) < 2 {
			log.Println("Skipping invalid row:", record)
			continue
		}

		to := normalizeNumber(record[0])
		text := record[1]
		from := "MegaPerya"
		if len(record) >= 3 && strings.TrimSpace(record[2]) != "" {
			from = strings.TrimSpace(record[2])
		}

		batch = append(batch, SendSMSRequest{
			To:   to,
			From: from,
			Text: text,
		})

		if len(batch) == batchSize {
			batchQueue <- batch
			batches++
			batch = make([]SendSMSRequest, 0, batchSize)
		}

		total++
	}

	if len(batch) > 0 {
		batchQueue <- batch
		batches++
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":         "uploaded",
		"total_messages": total,
		"total_batches":  batches,
	})
}

// ============================
// Helpers & main()
// ============================

func normalizeNumber(num string) string {
	num = strings.TrimSpace(num)
	if strings.HasPrefix(num, "09") {
		return "+63" + num[1:]
	}
	if !strings.HasPrefix(num, "+") {
		return "+" + num
	}
	return num
}

func main() {
	// start workers first
	for i := 0; i < 10; i++ {
		go worker(i)
	}

	http.HandleFunc("/send-sms", sendSMSHandler)         // single JSON
	http.HandleFunc("/batch-send", batchSendHandler)     // JSON batch
	http.HandleFunc("/upload-batch", uploadBatchHandler) // CSV upload

	log.Println("REST SMS API running on :5000")
	log.Fatal(http.ListenAndServe(":5000", nil))
}
