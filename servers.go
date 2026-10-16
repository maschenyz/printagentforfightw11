package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ServerList รับได้ทั้งค่าเดียว "http://..." และรายการ ["http://...", "http://..."]
// ลำดับในรายการคือลำดับที่ลองก่อนหลัง
type ServerList []string

func (s *ServerList) UnmarshalJSON(b []byte) error {
	var one string
	if json.Unmarshal(b, &one) == nil {
		*s = nil
		if t := strings.TrimSpace(one); t != "" {
			*s = ServerList{t}
		}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return fmt.Errorf("server_url ต้องเป็นข้อความหรือรายการข้อความ: %v", err)
	}
	*s = nil
	for _, u := range many {
		if t := strings.TrimSpace(u); t != "" {
			*s = append(*s, t)
		}
	}
	return nil
}

func (s ServerList) String() string { return strings.Join(s, ", ") }

var (
	activeURL      string    // ที่อยู่ตัวควบคุมที่ใช้ได้ล่าสุด (ป้องกันด้วย amu)
	lastPrimaryTry time.Time // ครั้งล่าสุดที่ลองกลับไปที่อยู่แรกใน config
)

func activeServer() string {
	amu.Lock()
	defer amu.Unlock()
	return activeURL
}

// call ลองที่อยู่ที่ใช้ได้ล่าสุดก่อน แล้วไล่ตัวอื่นตามลำดับใน config
// ลองตัวถัดไปทุกครั้งที่ไม่ได้คำตอบ 2xx (ต่อไม่ได้, 401, 404, 5xx ฯลฯ)
// เพราะชื่อเครื่องอาจชี้ไปเครื่องผิดที่ตอบกลับมาได้เหมือนกัน
// ทุกคำสั่งที่ agent ส่งเป็นแบบส่งซ้ำได้ปลอดภัย (งานพิมพ์ใช้ job ID กันซ้ำ)
func call(cfg *Config, method, path string, body []byte) ([]byte, error) {
	amu.Lock()
	order := []string{}
	if activeURL != "" {
		order = append(order, activeURL)
	}
	// ถ้าตอนนี้ใช้ที่อยู่สำรองอยู่ ลองกลับไปที่อยู่แรกทุก 60 วินาที (เช่น ชื่อเครื่องกลับมาใช้ได้แล้ว)
	if len(cfg.ServerURL) > 1 && activeURL != "" && activeURL != cfg.ServerURL[0] &&
		time.Since(lastPrimaryTry) > 60*time.Second {
		lastPrimaryTry = time.Now()
		order = append([]string{cfg.ServerURL[0]}, order...)
	}
	amu.Unlock()
	for _, u := range cfg.ServerURL {
		if !contains(order, u) {
			order = append(order, u)
		}
	}

	errs := []string{}
	for _, base := range order {
		b, err := callOne(cfg.Token, base, method, path, body)
		if err == nil {
			useServer(base)
			return b, nil
		}
		errs = append(errs, err.Error())
	}
	switch len(errs) {
	case 0:
		return nil, fmt.Errorf("ไม่มีที่อยู่ตัวควบคุมใน config")
	case 1:
		return nil, fmt.Errorf("%s", errs[0])
	}
	return nil, fmt.Errorf("ทุกที่อยู่ใช้ไม่ได้: %s", short(strings.Join(errs, " | "), 400))
}

func callOne(token, base, method, path string, body []byte) ([]byte, error) {
	full := strings.TrimRight(base, "/") + path
	req, err := http.NewRequest(method, full, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s: HTTP %d", full, resp.StatusCode)
	}
	return b, nil
}

func useServer(base string) {
	amu.Lock()
	defer amu.Unlock()
	if activeURL != base {
		log.Printf("controller: เชื่อมผ่าน %s", base)
		activeURL = base
	}
}
