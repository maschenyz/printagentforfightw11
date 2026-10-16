package main

import (
	"bufio"
	"crypto/rand"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

//go:embed web/agent.html
var agentHTML []byte

var RunID string // รหัสของโปรเซสนี้ (สุ่มใหม่ทุกครั้งที่เปิด) ให้ตัวควบคุมใช้ตรวจจับ agent_id ซ้ำ

type LocalPrinter struct {
	Name   string `json:"name"`
	Driver string `json:"driver"`
	Port   string `json:"port"`
	Status string `json:"status"`
	Jobs   int    `json:"jobs"`
}

type VirtualSpec struct {
	Profile string `json:"profile"` // share ID
	Name    string `json:"name"`    // ชื่อเครื่องพิมพ์จำลองที่ผู้ใช้เห็น
	Port    int    `json:"port"`
	Driver  string `json:"driver"`
}

type PollJob struct {
	ID      string `json:"id"`
	Profile string `json:"profile"`
	Printer string `json:"printer"` // ชื่อเครื่องพิมพ์จริงบนเครื่องนี้
	Data    string `json:"data"`    // base64
}

type syncReq struct {
	Run      string            `json:"run"`
	AgentID  string            `json:"agent_id"`
	Hostname string            `json:"hostname"`
	Printers []LocalPrinter    `json:"printers"`
	Virtuals map[string]string `json:"virtuals"` // share ID -> "ok" หรือข้อความปัญหา
}

type syncResp struct {
	GroupName string        `json:"group_name"`
	Virtuals  []VirtualSpec `json:"virtuals"`
	Jobs      []PollJob     `json:"jobs"`
}

type createdVirtual struct {
	Name     string `json:"name"`
	PortName string `json:"port_name"`
}

func portName(p int) string { return fmt.Sprintf("HOSP_%d", p) }

var (
	httpClient = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, // ที่อยู่ที่เข้าไม่ได้ ข้ามไปลองตัวถัดไปเร็วๆ
	}}
	done       = map[string]bool{} // job ID ที่พิมพ์แล้ว กันพิมพ์ซ้ำ
	doneFile   *os.File
	st         agentStatus

	amu           sync.Mutex // ป้องกันตัวแปรด้านล่างทั้งหมด
	inventory     []LocalPrinter
	vstat         = map[string]string{}
	listeners     = map[int]net.Listener{}
	portProfile   = map[int]string{}
	listenErr     = map[int]string{}
	desired       []VirtualSpec
	groupName     string
	reconciling   bool
	lastSig       string
	lastReconcile time.Time

	offeredSpecs []VirtualSpec // เครื่องพิมพ์ที่ตัวควบคุมให้สิทธิ์เครื่องนี้
	connected    = map[string]bool{} // ที่ผู้ใช้กดเชื่อมต่อแล้ว
)

type agentStatus struct {
	mu      sync.Mutex
	lastOK  time.Time
	lastErr string
}

func (s *agentStatus) set(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		if s.lastErr != "" || s.lastOK.IsZero() {
			log.Println("connected to controller")
		}
		s.lastOK = time.Now()
		s.lastErr = ""
		return
	}
	if s.lastErr != err.Error() {
		log.Println("controller error:", err)
	}
	s.lastErr = err.Error()
}

func startAgent(cfg *Config) {
	RunID = newID()
	loadDone(cfg)
	loadConnected(cfg)
	go inventoryLoop()
	go flushLoop(cfg)
	go syncLoop(cfg)
	go uiServer(cfg)
	log.Printf("agent %s (%s) started, controller %s", cfg.AgentID, Hostname, cfg.ServerURL.String())
}

// ---------- รายการเครื่องพิมพ์จริงในเครื่องนี้ ----------

func inventoryLoop() {
	lastErr := ""
	for {
		list, err := listPrinters()
		if err != nil {
			if err.Error() != lastErr {
				log.Println("inventory:", err)
			}
			lastErr = err.Error()
		} else {
			lastErr = ""
			amu.Lock()
			inventory = list
			amu.Unlock()
		}
		time.Sleep(30 * time.Second)
	}
}

// ---------- ซิงก์กับตัวควบคุม: heartbeat + รายงาน + รับงาน + รับรายการเครื่องพิมพ์ที่ใช้ได้ ----------

func virtStatusLocked(v VirtualSpec) string {
	if e, ok := listenErr[v.Port]; ok {
		return e
	}
	if s := vstat[v.Profile]; s != "" {
		return s
	}
	return "รอสร้างเครื่องพิมพ์จำลอง"
}

func syncLoop(cfg *Config) {
	for {
		amu.Lock()
		req := syncReq{AgentID: cfg.AgentID, Hostname: Hostname, Run: RunID,
			Printers: append([]LocalPrinter{}, inventory...), Virtuals: map[string]string{}}
		for _, v := range desired {
			req.Virtuals[v.Profile] = virtStatusLocked(v)
		}
		amu.Unlock()

		body, _ := json.Marshal(req)
		b, err := call(cfg, "POST", "/api/agent/sync", body)
		st.set(err)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		var sr syncResp
		if err := json.Unmarshal(b, &sr); err != nil {
			log.Println("sync decode:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		amu.Lock()
		groupName = sr.GroupName
		amu.Unlock()
		amu.Lock()
		offeredSpecs = sr.Virtuals
		amu.Unlock()
		applyDesired(cfg, wantedSpecs(cfg))
		for _, j := range sr.Jobs {
			runJob(cfg, j)
		}
		time.Sleep(3 * time.Second)
	}
}

func applyDesired(cfg *Config, specs []VirtualSpec) {
	sort.Slice(specs, func(i, j int) bool { return specs[i].Profile < specs[j].Profile })
	sb, _ := json.Marshal(specs)
	sig := string(sb)

	amu.Lock()
	desired = specs
	want := map[string]bool{}
	for _, v := range specs {
		want[v.Profile] = true
	}
	for p := range vstat {
		if !want[p] {
			delete(vstat, p)
		}
	}
	syncListenersLocked(cfg, specs)
	need := !reconciling && (sig != lastSig || time.Since(lastReconcile) > 2*time.Minute)
	if need {
		reconciling = true
		lastSig = sig
	}
	amu.Unlock()
	if need {
		go reconcile(cfg, specs)
	}
}

// ---------- ฟังพอร์ต localhost ให้เครื่องพิมพ์จำลอง ----------

func syncListenersLocked(cfg *Config, specs []VirtualSpec) {
	want := map[int]string{}
	for _, v := range specs {
		want[v.Port] = v.Profile
	}
	for p, ln := range listeners {
		if _, ok := want[p]; !ok {
			ln.Close()
			delete(listeners, p)
			delete(portProfile, p)
			delete(listenErr, p)
		}
	}
	for p, prof := range want {
		portProfile[p] = prof
		if _, ok := listeners[p]; ok {
			continue
		}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err != nil {
			listenErr[p] = fmt.Sprintf("เปิดพอร์ต 127.0.0.1:%d ไม่ได้ (มีโปรแกรมอื่นใช้อยู่?)", p)
			continue
		}
		delete(listenErr, p)
		listeners[p] = ln
		log.Printf("listening 127.0.0.1:%d -> %s", p, prof)
		go acceptLoop(cfg, p, ln)
	}
}

func acceptLoop(cfg *Config, port int, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return // ถูกปิดเพราะเลิกใช้พอร์ตนี้
		}
		go receive(cfg, port, c)
	}
}

func receive(cfg *Config, port int, c net.Conn) {
	defer c.Close()
	amu.Lock()
	profile := portProfile[port]
	amu.Unlock()
	if profile == "" {
		return
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Minute))
	data, err := io.ReadAll(io.LimitReader(c, 32<<20))
	if err != nil || len(data) == 0 {
		log.Printf("[%s] discard job (err=%v, bytes=%d)", profile, err, len(data))
		return // ข้อมูลไม่ครบ ทิ้ง ดีกว่าพิมพ์สลากครึ่งๆ
	}
	id := newID()
	// เขียนลงดิสก์ก่อนเสมอ ให้งานไม่หายแม้ตัวควบคุมล่มหรือเครือข่ายหลุด
	name := filepath.Join(cfg.outbox(), id+"__"+profile+".bin")
	if err := os.WriteFile(name, data, 0o600); err != nil {
		log.Println("write outbox:", err)
		return
	}
	log.Printf("[%s] job %s queued (%d bytes)", profile, id, len(data))
}

// ---------- ส่งงานขึ้นตัวควบคุม ----------

func flushLoop(cfg *Config) {
	for {
		flushOnce(cfg)
		time.Sleep(2 * time.Second)
	}
}

func flushOnce(cfg *Config) {
	files, _ := filepath.Glob(filepath.Join(cfg.outbox(), "*.bin"))
	ttl := time.Duration(cfg.JobTTLMinutes) * time.Minute
	for _, f := range files {
		base := strings.TrimSuffix(filepath.Base(f), ".bin")
		parts := strings.SplitN(base, "__", 2)
		if len(parts) != 2 {
			continue
		}
		id, profile := parts[0], parts[1]
		fi, err := os.Stat(f)
		if err != nil {
			continue
		}
		if time.Since(fi.ModTime()) > ttl { // ไม่พิมพ์สลากเก่าที่ค้างนานเกินไป
			os.Remove(f)
			log.Printf("job %s dropped: ค้างในเครื่องเกิน %d นาที", id, cfg.JobTTLMinutes)
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		body, _ := json.Marshal(map[string]string{
			"id": id, "profile": profile, "agent_id": cfg.AgentID,
			"data": base64.StdEncoding.EncodeToString(data),
		})
		// ตัวควบคุมใช้ id เป็น idempotency key: ส่งซ้ำจะไม่สร้างงานใหม่
		resp, err := call(cfg, "POST", "/api/jobs", body)
		if err != nil {
			st.set(err)
			return // ตัวควบคุมยังไม่พร้อม ลองรอบหน้า
		}
		st.set(nil)
		var r struct {
			Rejected string `json:"rejected"`
		}
		json.Unmarshal(resp, &r)
		os.Remove(f)
		if r.Rejected != "" {
			log.Printf("job %s REJECTED: %s", id, r.Rejected)
		} else {
			log.Printf("job %s submitted", id)
		}
	}
}

// ---------- พิมพ์งานที่ตัวควบคุมส่งมาให้เครื่องพิมพ์จริงของเครื่องนี้ ----------

func runJob(cfg *Config, j PollJob) {
	status, msg := "done", ""
	if !done[j.ID] { // เคยพิมพ์แล้ว (ack ครั้งก่อนอาจหาย) ตอบ done ซ้ำ ไม่พิมพ์ใหม่
		data, derr := base64.StdEncoding.DecodeString(j.Data)
		if derr != nil {
			status, msg = "error", "bad payload"
		} else if err := printRaw(j.Printer, j.ID, data); err != nil {
			status, msg = "error", err.Error()
		} else {
			markDone(j.ID)
			log.Printf("job %s printed on %q", j.ID, j.Printer)
		}
	}
	if status == "error" {
		log.Printf("job %s error: %s", j.ID, msg)
	}
	body, _ := json.Marshal(map[string]string{"agent_id": cfg.AgentID, "status": status, "error": msg})
	if _, err := call(cfg, "POST", "/api/agent/jobs/"+j.ID+"/ack", body); err != nil {
		log.Println("ack:", err)
	}
}

// ---------- สร้าง/ลบเครื่องพิมพ์จำลองตามนโยบายของ admin ----------

func reconcile(cfg *Config, specs []VirtualSpec) {
	defer func() {
		amu.Lock()
		reconciling = false
		lastReconcile = time.Now()
		amu.Unlock()
	}()
	cpath := filepath.Join(cfg.DataDir, "virtuals.json")
	created := map[string]createdVirtual{} // เฉพาะที่ agent นี้สร้างเอง
	if b, err := os.ReadFile(cpath); err == nil {
		json.Unmarshal(b, &created)
	}
	want := map[string]bool{}
	for _, v := range specs {
		want[v.Profile] = true
	}
	for p, cv := range created {
		if !want[p] { // ไม่ได้รับสิทธิ์แล้ว (ย้ายกลุ่ม/เลิกแชร์) ลบเครื่องจำลอง
			removeVirtual(cv.Name, cv.PortName)
			delete(created, p)
			log.Printf("removed virtual printer %q", cv.Name)
		}
	}
	for _, v := range specs {
		if cv, ok := created[v.Profile]; ok && cv.Name != v.Name { // admin เปลี่ยนชื่อ
			removeVirtual(cv.Name, cv.PortName)
			delete(created, v.Profile)
		}
		status := ""
		if strings.TrimSpace(v.Driver) == "" {
			status = "ยังไม่ได้ระบุไดรเวอร์ (admin ตั้งที่หน้าเครื่องพิมพ์ที่แชร์)"
		} else {
			status = ensureVirtual(v.Name, v.Driver, v.Port)
		}
		if status == "ok" {
			created[v.Profile] = createdVirtual{v.Name, portName(v.Port)}
		}
		amu.Lock()
		if vstat[v.Profile] != status {
			log.Printf("virtual printer %q: %s", v.Name, status)
		}
		vstat[v.Profile] = status
		amu.Unlock()
	}
	b, _ := json.MarshalIndent(created, "", "  ")
	os.WriteFile(cpath, b, 0o644)
}

// ---------- หน้าสถานะของเครื่องนี้ (localhost เท่านั้น) ----------

func uiServer(cfg *Config) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(agentHTML)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		files, _ := filepath.Glob(filepath.Join(cfg.outbox(), "*.bin"))
		st.mu.Lock()
		up := st.lastErr == "" && !st.lastOK.IsZero() && time.Since(st.lastOK) < 15*time.Second
		lastOK := "-"
		if !st.lastOK.IsZero() {
			lastOK = st.lastOK.Format("15:04:05")
		}
		lastErr := st.lastErr
		st.mu.Unlock()

		amu.Lock()
		vs := []map[string]any{}
		for _, v := range offeredSpecs {
			isConn := cfg.AutoConnect || connected[v.Profile]
			status := "ยังไม่เชื่อมต่อ"
			if isConn {
				status = virtStatusLocked(v)
			}
			vs = append(vs, map[string]any{"profile": v.Profile, "name": v.Name, "port": v.Port,
				"connected": isConn, "status": status})
		}
		grp := groupName
		amu.Unlock()

		writeJSON(w, map[string]any{
			"agent_id": cfg.AgentID, "hostname": Hostname, "server_url": cfg.ServerURL.String(), "active_server": activeServer(),
			"connected": up, "last_ok": lastOK, "last_err": lastErr,
			"group": grp, "offered": vs, "auto_connect": cfg.AutoConnect, "outbox": len(files), "log": tailLog(cfg, 40),
		})
	})
	mux.HandleFunc("/connect", connectHandler(cfg, true))
	mux.HandleFunc("/disconnect", connectHandler(cfg, false))
	port := cfg.UIPort
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// กัน DNS rebinding: รับเฉพาะ Host ที่เป็น localhost จริง
		if r.Host != fmt.Sprintf("localhost:%d", port) && r.Host != fmt.Sprintf("127.0.0.1:%d", port) {
			http.Error(w, "forbidden", 403)
			return
		}
		// กัน CSRF จากเว็บอื่น: คำสั่งแก้ไขต้องมี header นี้ ซึ่งเว็บอื่นใส่ข้ามเว็บไม่ได้
		if r.Method == http.MethodPost && r.Header.Get("X-Requested-With") != "agent" {
			http.Error(w, "forbidden", 403)
			return
		}
		mux.ServeHTTP(w, r)
	})
	log.Printf("agent UI: http://localhost:%d", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), h))
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func loadDone(cfg *Config) {
	p := filepath.Join(cfg.DataDir, "done.log")
	if f, err := os.Open(p); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			done[strings.TrimSpace(sc.Text())] = true
		}
		f.Close()
	}
	var err error
	doneFile, err = os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	must(err)
}

func markDone(id string) {
	done[id] = true
	fmt.Fprintln(doneFile, id)
	doneFile.Sync()
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return time.Now().Format("20060102T150405") + "-" + hex.EncodeToString(b)
}

// ---------- ผู้ใช้กด "เชื่อมต่อ" / "ยกเลิกการเชื่อมต่อ" เครื่องพิมพ์ที่ได้รับสิทธิ์ ----------

// wantedSpecs คือเครื่องพิมพ์ที่ "ได้สิทธิ์" และ "ผู้ใช้เลือกเชื่อมต่อแล้ว" (หรือเปิด auto_connect)
func wantedSpecs(cfg *Config) []VirtualSpec {
	amu.Lock()
	defer amu.Unlock()
	out := []VirtualSpec{}
	for _, v := range offeredSpecs {
		if cfg.AutoConnect || connected[v.Profile] {
			out = append(out, v)
		}
	}
	return out
}

func connectHandler(cfg *Config, on bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var in struct {
			Profile string `json:"profile"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<12)).Decode(&in) != nil || in.Profile == "" {
			http.Error(w, "bad request", 400)
			return
		}
		amu.Lock()
		found := false
		for _, v := range offeredSpecs {
			if v.Profile == in.Profile {
				found = true
			}
		}
		if on && !found { // เชื่อมได้เฉพาะที่ตัวควบคุมให้สิทธิ์เท่านั้น
			amu.Unlock()
			http.Error(w, "ไม่มีเครื่องพิมพ์นี้ในรายการที่ได้รับสิทธิ์", 400)
			return
		}
		if on {
			connected[in.Profile] = true
		} else {
			delete(connected, in.Profile)
		}
		amu.Unlock()
		saveConnected(cfg)
		log.Printf("user %s printer %s", map[bool]string{true: "connected", false: "disconnected"}[on], in.Profile)
		applyDesired(cfg, wantedSpecs(cfg)) // สร้าง/ลบเครื่องพิมพ์จำลองทันที ไม่ต้องรอรอบซิงก์
		writeJSON(w, map[string]any{"ok": true})
	}
}

func loadConnected(cfg *Config) {
	b, err := os.ReadFile(filepath.Join(cfg.DataDir, "connected.json"))
	if err != nil {
		return
	}
	var ids []string
	if json.Unmarshal(b, &ids) != nil {
		return
	}
	amu.Lock()
	for _, id := range ids {
		connected[id] = true
	}
	amu.Unlock()
}

func saveConnected(cfg *Config) {
	amu.Lock()
	ids := []string{}
	for p := range connected {
		ids = append(ids, p)
	}
	amu.Unlock()
	sort.Strings(ids)
	b, _ := json.Marshal(ids)
	os.WriteFile(filepath.Join(cfg.DataDir, "connected.json"), b, 0o644)
}
