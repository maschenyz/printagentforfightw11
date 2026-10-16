package main

import (
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

//go:embed web/admin.html
var adminHTML []byte

type Group struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Machine struct {
	ID       string            `json:"id"`
	Hostname string            `json:"hostname"`
	IP       string            `json:"ip"`
	Group    string            `json:"group"` // ว่าง = ยังไม่จัดกลุ่ม (ใช้/แชร์อะไรไม่ได้)
	Printers []LocalPrinter    `json:"printers"`
	Virtuals map[string]string `json:"virtuals"` // share ID -> สถานะเครื่องพิมพ์จำลองบนเครื่องนี้
	LastSeen time.Time         `json:"last_seen"`
	online   bool
	lastRun  string              // run ID ล่าสุดที่ซิงก์เข้ามา
	runs     map[string]*runSeen // run ID ที่เห็นเมื่อไม่นานมานี้
	dupUntil time.Time           // ก่อนเวลานี้ = พบ agent_id ซ้ำ
	dupInfo  string
}

type runSeen struct {
	IP   string
	Seen time.Time
}

type Share struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`    // ชื่อที่ผู้ใช้เห็นในเครื่องลูก
	Host    string   `json:"host"`    // เครื่องที่ต่อเครื่องพิมพ์จริง
	Real    string   `json:"real"`    // ชื่อเครื่องพิมพ์จริงในเครื่องโฮสต์
	Driver  string   `json:"driver"`  // ไดรเวอร์ที่เครื่องผู้ใช้ต้องมี
	Port    int      `json:"port"`    // พอร์ต localhost ของเครื่องพิมพ์จำลอง
	Allowed []string `json:"allowed"` // กลุ่มที่ใช้ได้
	Enabled bool     `json:"enabled"`
}

type JobMeta struct {
	ID          string     `json:"id"`
	Printer     string     `json:"printer"` // share ID
	PrinterName string     `json:"printer_name"`
	From        string     `json:"from"`
	Host        string     `json:"host"`
	Status      string     `json:"status"` // pending|done|error|rejected|cancelled|expired
	Error       string     `json:"error,omitempty"`
	Size        int        `json:"size"`
	Attempts    int        `json:"attempts"`
	Created     time.Time  `json:"created"`
	Sent        *time.Time `json:"sent,omitempty"`
	Done        *time.Time `json:"done,omitempty"`
}

type persisted struct {
	Groups    []*Group            `json:"groups"`
	Machines  map[string]*Machine `json:"machines"`
	Shares    []*Share            `json:"shares"`
	Jobs      []*JobMeta          `json:"jobs"`
	NextPort  int                 `json:"next_port"`
	NextShare int                 `json:"next_share"`
	NextGroup int                 `json:"next_group"`
}

type Server struct {
	mu        sync.Mutex
	cfg       *Config
	dir       string
	dirty     bool
	Groups    []*Group
	Machines  map[string]*Machine
	Shares    []*Share
	Jobs      []*JobMeta
	NextPort  int
	NextShare int
	NextGroup int
}

func startServer(cfg *Config) {
	if cfg.Token == "" || cfg.AdminPassword == "" {
		log.Fatal("controller mode needs token and admin_password in config")
	}
	s := &Server{cfg: cfg, dir: filepath.Join(cfg.DataDir, "server"),
		Groups: []*Group{}, Machines: map[string]*Machine{}, Shares: []*Share{}, Jobs: []*JobMeta{}, NextPort: 9030}
	must(os.MkdirAll(filepath.Join(s.dir, "jobs"), 0o755))
	s.load()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/jobs", s.postJob)
	mux.HandleFunc("/api/agent/sync", s.syncAgent)
	mux.HandleFunc("/api/agent/jobs/", s.ackJob)
	mux.HandleFunc("/admin", s.admin(s.adminPage))
	mux.HandleFunc("/admin/api/state", s.admin(s.adminState))
	mux.HandleFunc("/admin/api/group/create", s.admin(s.act(s.aGroupCreate)))
	mux.HandleFunc("/admin/api/group/rename", s.admin(s.act(s.aGroupRename)))
	mux.HandleFunc("/admin/api/group/delete", s.admin(s.act(s.aGroupDelete)))
	mux.HandleFunc("/admin/api/machine/group", s.admin(s.act(s.aMachineGroup)))
	mux.HandleFunc("/admin/api/machine/delete", s.admin(s.act(s.aMachineDelete)))
	mux.HandleFunc("/admin/api/share/create", s.admin(s.act(s.aShareCreate)))
	mux.HandleFunc("/admin/api/share/update", s.admin(s.act(s.aShareUpdate)))
	mux.HandleFunc("/admin/api/share/delete", s.admin(s.act(s.aShareDelete)))
	mux.HandleFunc("/admin/api/job/retry", s.admin(s.act(s.aJob("retry"))))
	mux.HandleFunc("/admin/api/job/cancel", s.admin(s.act(s.aJob("cancel"))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/admin", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})
	go s.sweep()
	go func() { log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", cfg.ServerPort), mux)) }()
	log.Printf("controller: admin at http://<ip-เครื่องนี้>:%d/admin", cfg.ServerPort)
}

// ---------- state ----------

func (s *Server) statePath() string { return filepath.Join(s.dir, "state.json") }
func (s *Server) jobPath(id string) string {
	return filepath.Join(s.dir, "jobs", id+".bin")
}

func (s *Server) load() {
	b, err := os.ReadFile(s.statePath())
	if err != nil {
		return
	}
	var p persisted
	if json.Unmarshal(b, &p) != nil {
		return
	}
	if p.Groups != nil {
		s.Groups = p.Groups
	}
	if p.Machines != nil {
		s.Machines = p.Machines
	}
	for _, m := range s.Machines {
		if m.Virtuals == nil {
			m.Virtuals = map[string]string{}
		}
	}
	if p.Shares != nil {
		s.Shares = p.Shares
	}
	if p.Jobs != nil {
		s.Jobs = p.Jobs
	}
	if p.NextPort > s.NextPort {
		s.NextPort = p.NextPort
	}
	s.NextShare, s.NextGroup = p.NextShare, p.NextGroup
}

// เรียกตอนถือ lock เท่านั้น
func (s *Server) save() {
	p := persisted{Groups: s.Groups, Machines: s.Machines, Shares: s.Shares, Jobs: s.Jobs,
		NextPort: s.NextPort, NextShare: s.NextShare, NextGroup: s.NextGroup}
	b, _ := json.MarshalIndent(p, "", "  ")
	tmp := s.statePath() + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		os.Rename(tmp, s.statePath())
	}
	s.dirty = false
}

func (s *Server) group(id string) *Group {
	for _, g := range s.Groups {
		if g.ID == id {
			return g
		}
	}
	return nil
}

func (s *Server) share(id string) *Share {
	for _, sh := range s.Shares {
		if sh.ID == id {
			return sh
		}
	}
	return nil
}

func (s *Server) find(id string) *JobMeta {
	for _, j := range s.Jobs {
		if j.ID == id {
			return j
		}
	}
	return nil
}

func (s *Server) trim() {
	for len(s.Jobs) > 500 && s.Jobs[0].Status != "pending" {
		os.Remove(s.jobPath(s.Jobs[0].ID))
		s.Jobs = s.Jobs[1:]
	}
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func allowed(sh *Share, m *Machine) bool {
	return m != nil && m.Group != "" && contains(sh.Allowed, m.Group)
}

func validName(s string) bool {
	if s == "" || len(s) > 80 {
		return false
	}
	for _, c := range s {
		if !okRune(c) {
			return false
		}
	}
	return true
}

// ตรวจสถานะออนไลน์ + ยกเลิกงานที่ค้างนานเกิน TTL (ไม่ให้สลากเก่าถูกพิมพ์ทีหลัง)
func (s *Server) sweep() {
	for range time.Tick(10 * time.Second) {
		s.mu.Lock()
		now := time.Now()
		ttl := time.Duration(s.cfg.JobTTLMinutes) * time.Minute
		changed := false
		for _, j := range s.Jobs {
			if j.Status == "pending" && now.Sub(j.Created) > ttl {
				j.Status = "expired"
				j.Error = fmt.Sprintf("ค้างเกิน %d นาที จึงยกเลิกเพื่อไม่ให้สลากเก่าถูกพิมพ์ภายหลัง", s.cfg.JobTTLMinutes)
				os.Remove(s.jobPath(j.ID))
				log.Printf("controller: job %s expired (printer %s, host %s)", j.ID, j.PrinterName, j.Host)
				changed = true
			}
		}
		for _, m := range s.Machines {
			if m.online && now.Sub(m.LastSeen) > 30*time.Second {
				m.online = false
				log.Printf("controller: machine %s OFFLINE", m.ID)
			}
		}
		if changed || s.dirty {
			s.save()
		}
		s.mu.Unlock()
	}
}

// ---------- auth ----------

func (s *Server) bearer(r *http.Request) bool {
	return subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.cfg.Token)) == 1
}

func (s *Server) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, pw, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(pw), []byte(s.cfg.AdminPassword)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="print controller"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// กัน CSRF: คำสั่งแก้ไขต้องมี header นี้ ซึ่งเว็บอื่นใส่ข้ามเว็บไม่ได้
		if r.Method == http.MethodPost && r.Header.Get("X-Requested-With") != "admin" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// ---------- API สำหรับ agent ----------

func (s *Server) postJob(w http.ResponseWriter, r *http.Request) {
	if !s.bearer(r) {
		http.Error(w, "unauthorized", 401)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var in struct {
		ID      string `json:"id"`
		Profile string `json:"profile"`
		AgentID string `json:"agent_id"`
		Data    string `json:"data"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<20)
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil ||
		!validName(in.ID) || !validName(in.Profile) || !validName(in.AgentID) {
		http.Error(w, "bad request", 400)
		return
	}
	data, err := base64.StdEncoding.DecodeString(in.Data)
	if err != nil || len(data) == 0 {
		http.Error(w, "bad data", 400)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.find(in.ID) != nil { // ส่งซ้ำ: รับทราบแต่ไม่สร้างงานใหม่
		writeJSON(w, map[string]any{"ok": true, "dup": true})
		return
	}
	m := s.Machines[in.AgentID]
	sh := s.share(in.Profile)
	reject := ""
	switch {
	case sh == nil:
		reject = "ไม่พบเครื่องพิมพ์ที่แชร์ (อาจถูกยกเลิกการแชร์แล้ว)"
	case !sh.Enabled:
		reject = "เครื่องพิมพ์นี้ถูกปิดการแชร์"
	case !allowed(sh, m):
		reject = "เครื่อง " + in.AgentID + " ไม่ได้อยู่ในกลุ่มที่มีสิทธิ์ใช้เครื่องพิมพ์นี้"
	}
	meta := &JobMeta{ID: in.ID, Printer: in.Profile, PrinterName: in.Profile, From: in.AgentID,
		Size: len(data), Created: time.Now()}
	if sh != nil {
		meta.PrinterName, meta.Host = sh.Name, sh.Host
	}
	if reject != "" {
		meta.Status, meta.Error = "rejected", reject
		s.Jobs = append(s.Jobs, meta)
		s.trim()
		s.save()
		log.Printf("controller: job %s from %s REJECTED: %s", in.ID, in.AgentID, reject)
		writeJSON(w, map[string]any{"ok": false, "rejected": reject}) // ไม่เก็บเนื้อหางานที่ถูกปฏิเสธ
		return
	}
	if err := os.WriteFile(s.jobPath(in.ID), data, 0o600); err != nil {
		http.Error(w, "storage error", 500)
		return
	}
	meta.Status = "pending"
	s.Jobs = append(s.Jobs, meta)
	s.trim()
	s.save()
	log.Printf("controller: job %s %s -> %s@%s (%d bytes)", in.ID, in.AgentID, sh.Name, sh.Host, len(data))
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) syncAgent(w http.ResponseWriter, r *http.Request) {
	if !s.bearer(r) {
		http.Error(w, "unauthorized", 401)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req syncReq
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req) != nil || !validName(req.AgentID) {
		http.Error(w, "bad request", 400)
		return
	}
	if len(req.Printers) > 100 {
		req.Printers = req.Printers[:100]
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	m := s.Machines[req.AgentID]
	if m == nil {
		m = &Machine{ID: req.AgentID, Virtuals: map[string]string{}}
		s.Machines[req.AgentID] = m
		s.dirty = true
		log.Printf("controller: new machine %s (%s) from %s — ยังไม่จัดกลุ่ม", req.AgentID, req.Hostname, host)
	} else if !m.online {
		log.Printf("controller: machine %s ONLINE", m.ID)
	}
	m.online, m.LastSeen, m.IP = true, now, host
	if req.Run != "" && validName(req.Run) {
		if m.runs == nil {
			m.runs = map[string]*runSeen{}
		}
		for k, rs := range m.runs {
			if now.Sub(rs.Seen) > 5*time.Minute {
				delete(m.runs, k)
			}
		}
		// run เดิมกลับมาหลังมี run อื่นคั่น = มี agent อย่างน้อย 2 ตัวใช้ agent_id เดียวกันพร้อมกัน
		// (เช่นเครื่องที่โคลนดิสก์) การรีสตาร์ตปกติไม่เข้าเงื่อนไขนี้ เพราะ run เก่าไม่กลับมาอีก
		if _, seen := m.runs[req.Run]; seen && m.lastRun != "" && m.lastRun != req.Run {
			ips := map[string]bool{host: true}
			for _, rs := range m.runs {
				if now.Sub(rs.Seen) < 2*time.Minute {
					ips[rs.IP] = true
				}
			}
			list := []string{}
			for ip := range ips {
				list = append(list, ip)
			}
			sort.Strings(list)
			if now.After(m.dupUntil) {
				log.Printf("controller: WARNING agent_id %s ซ้ำ (IP %s) หยุดส่งงานพิมพ์ให้ ID นี้จนกว่าจะแก้", m.ID, strings.Join(list, ", "))
			}
			m.dupUntil = now.Add(2 * time.Minute)
			m.dupInfo = fmt.Sprintf("agent_id \"%s\" ซ้ำ: พบมากกว่า 1 เครื่องใช้ ID นี้ (IP %s) แก้ agent_id ให้ไม่ซ้ำ ระหว่างนี้ระบบหยุดส่งงานพิมพ์ให้เครื่องนี้", m.ID, strings.Join(list, ", "))
		}
		m.lastRun = req.Run
		m.runs[req.Run] = &runSeen{IP: host, Seen: now}
	}
	if hn := short(req.Hostname, 64); hn != m.Hostname {
		m.Hostname, s.dirty = hn, true
	}
	a, _ := json.Marshal(m.Printers)
	b, _ := json.Marshal(req.Printers)
	if string(a) != string(b) {
		m.Printers, s.dirty = req.Printers, true
	}
	a, _ = json.Marshal(m.Virtuals)
	b, _ = json.Marshal(req.Virtuals)
	if string(a) != string(b) {
		m.Virtuals, s.dirty = req.Virtuals, true
		if m.Virtuals == nil {
			m.Virtuals = map[string]string{}
		}
	}

	resp := syncResp{Virtuals: []VirtualSpec{}, Jobs: []PollJob{}}
	if g := s.group(m.Group); g != nil {
		resp.GroupName = g.Name
	}
	for _, sh := range s.Shares {
		if sh.Enabled && sh.Host != m.ID && allowed(sh, m) {
			resp.Virtuals = append(resp.Virtuals, VirtualSpec{Profile: sh.ID, Name: sh.Name, Port: sh.Port, Driver: sh.Driver})
		}
	}
	for _, j := range s.Jobs {
		if len(resp.Jobs) >= 5 || now.Before(m.dupUntil) { // agent_id ซ้ำ: ไม่ส่งงานให้ใคร กันสลากไปออกผิดเครื่อง
			break
		}
		if j.Status != "pending" || j.Host != m.ID {
			continue
		}
		sh := s.share(j.Printer)
		if sh == nil || !sh.Enabled {
			continue
		}
		data, err := os.ReadFile(s.jobPath(j.ID))
		if err != nil {
			continue
		}
		resp.Jobs = append(resp.Jobs, PollJob{ID: j.ID, Profile: sh.ID, Printer: sh.Real,
			Data: base64.StdEncoding.EncodeToString(data)})
		if j.Sent == nil {
			t := now
			j.Sent = &t
		}
		j.Attempts++
		s.dirty = true
	}
	writeJSON(w, resp)
}

func (s *Server) ackJob(w http.ResponseWriter, r *http.Request) {
	if !s.bearer(r) {
		http.Error(w, "unauthorized", 401)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/agent/jobs/")
	id := strings.TrimSuffix(rest, "/ack")
	if r.Method != http.MethodPost || id == rest || !validName(id) {
		http.NotFound(w, r)
		return
	}
	var in struct {
		AgentID string `json:"agent_id"`
		Status  string `json:"status"`
		Error   string `json:"error"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&in) != nil {
		http.Error(w, "bad request", 400)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.find(id)
	if j == nil {
		http.NotFound(w, r)
		return
	}
	if j.Host != in.AgentID { // รายงานผลได้เฉพาะเครื่องโฮสต์ของงานนั้น
		http.Error(w, "forbidden", 403)
		return
	}
	now := time.Now()
	switch in.Status {
	case "done":
		j.Status, j.Error, j.Done = "done", "", &now
		os.Remove(s.jobPath(id)) // พิมพ์เสร็จแล้ว ไม่เก็บเนื้อหาสลากไว้
	case "error":
		j.Status, j.Error, j.Done = "error", short(in.Error, 200), &now
	default:
		http.Error(w, "bad status", 400)
		return
	}
	s.save()
	log.Printf("controller: job %s %s %s", id, j.Status, j.Error)
	writeJSON(w, map[string]any{"ok": true})
}

// ---------- หน้า admin ----------

func (s *Server) adminPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(adminHTML)
}

func (s *Server) hint(j *JobMeta) string {
	if j.Status != "pending" {
		return ""
	}
	sh := s.share(j.Printer)
	if sh == nil {
		return "ไม่พบเครื่องพิมพ์ที่แชร์"
	}
	hm := s.Machines[sh.Host]
	if hm == nil || !hm.online {
		return "เครื่องโฮสต์ " + sh.Host + " ออฟไลน์ งานจะรอจนหมดเวลา"
	}
	for _, lp := range hm.Printers {
		if lp.Name == sh.Real && lp.Status != "Normal" && lp.Status != "" {
			return "เครื่องพิมพ์รายงานสถานะ: " + lp.Status
		}
	}
	if j.Sent != nil {
		return "ส่งให้เครื่องโฮสต์แล้ว รอผลการพิมพ์"
	}
	return "รอเครื่องโฮสต์มารับงาน"
}

func (s *Server) adminState(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()

	type mview struct {
		Machine
		Online bool   `json:"online"`
		Dup    string `json:"dup,omitempty"`
	}
	machines := []mview{}
	online := 0
	for _, m := range s.Machines {
		dup := ""
		if now.Before(m.dupUntil) {
			dup = m.dupInfo
		}
		machines = append(machines, mview{Machine: *m, Online: m.online, Dup: dup})
		if m.online {
			online++
		}
	}
	sort.Slice(machines, func(i, j int) bool { return machines[i].Hostname < machines[j].Hostname })

	type sview struct {
		Share
		HostOnline bool   `json:"host_online"`
		Health     string `json:"health"`
	}
	shares := []sview{}
	for _, sh := range s.Shares {
		v := sview{Share: *sh, Health: "ไม่พบเครื่องโฮสต์"}
		if hm := s.Machines[sh.Host]; hm != nil {
			v.HostOnline = hm.online
			v.Health = "ไม่พบเครื่องพิมพ์ในเครื่องโฮสต์"
			for _, lp := range hm.Printers {
				if lp.Name == sh.Real {
					v.Health = lp.Status
					break
				}
			}
		}
		if v.Allowed == nil {
			v.Allowed = []string{}
		}
		shares = append(shares, v)
	}

	type jview struct {
		JobMeta
		Hint string `json:"hint"`
	}
	jobs := []jview{}
	pending, problems, total24, done24 := 0, 0, 0, 0
	hourly := make([]int, 24) // งานที่รับเข้าในแต่ละชั่วโมงย้อนหลัง 24 ชั่วโมง (ช่องสุดท้าย = ชั่วโมงล่าสุด)
	for _, j := range s.Jobs {
		if j.Status == "pending" {
			pending++
		}
		if (j.Status == "error" || j.Status == "rejected" || j.Status == "expired") && now.Sub(j.Created) < 24*time.Hour {
			problems++
		}
		if age := now.Sub(j.Created); age < 24*time.Hour {
			total24++
			if j.Status == "done" {
				done24++
			}
			if idx := 23 - int(age/time.Hour); idx >= 0 && idx < 24 {
				hourly[idx]++
			}
		}
	}
	for i := len(s.Jobs) - 1; i >= 0 && len(jobs) < 200; i-- {
		jobs = append(jobs, jview{*s.Jobs[i], s.hint(s.Jobs[i])})
	}
	writeJSON(w, map[string]any{
		"summary": map[string]int{"online": online, "machines": len(s.Machines), "pending": pending, "problems": problems},
		"groups":  s.Groups, "machines": machines, "shares": shares, "jobs": jobs,
		"log": tailLog(s.cfg, 60), "ttl": s.cfg.JobTTLMinutes,
		"stats": map[string]any{"total24": total24, "done24": done24, "hourly": hourly},
	})
}

// act: ตัวห่อ action ของ admin (ล็อก, ตรวจ error, บันทึกไฟล์, เขียน log)
func (s *Server) act(fn func(dec func(any) error) (string, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		body := http.MaxBytesReader(w, r.Body, 1<<16)
		dec := func(v any) error { return json.NewDecoder(body).Decode(v) }
		s.mu.Lock()
		defer s.mu.Unlock()
		msg, err := fn(dec)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		s.save()
		log.Println("admin:", msg)
		writeJSON(w, map[string]any{"ok": true})
	}
}

func (s *Server) aGroupCreate(dec func(any) error) (string, error) {
	var in struct {
		Name string `json:"name"`
	}
	if err := dec(&in); err != nil {
		return "", err
	}
	n := strings.TrimSpace(in.Name)
	if n == "" || len([]rune(n)) > 40 {
		return "", fmt.Errorf("ชื่อกลุ่มไม่ถูกต้อง")
	}
	for _, g := range s.Groups {
		if g.Name == n {
			return "", fmt.Errorf("มีกลุ่มชื่อนี้อยู่แล้ว")
		}
	}
	s.NextGroup++
	s.Groups = append(s.Groups, &Group{ID: fmt.Sprintf("g%d", s.NextGroup), Name: n})
	return "group created: " + n, nil
}

func (s *Server) aGroupRename(dec func(any) error) (string, error) {
	var in struct{ ID, Name string }
	if err := dec(&in); err != nil {
		return "", err
	}
	g := s.group(in.ID)
	n := strings.TrimSpace(in.Name)
	if g == nil || n == "" || len([]rune(n)) > 40 {
		return "", fmt.Errorf("ข้อมูลไม่ถูกต้อง")
	}
	g.Name = n
	return "group renamed: " + n, nil
}

func (s *Server) aGroupDelete(dec func(any) error) (string, error) {
	var in struct{ ID string }
	if err := dec(&in); err != nil {
		return "", err
	}
	g := s.group(in.ID)
	if g == nil {
		return "", fmt.Errorf("ไม่พบกลุ่ม")
	}
	for _, m := range s.Machines {
		if m.Group == g.ID {
			return "", fmt.Errorf("ยังมีเครื่องอยู่ในกลุ่มนี้ ย้ายออกก่อน")
		}
	}
	keep := []*Group{}
	for _, x := range s.Groups {
		if x.ID != g.ID {
			keep = append(keep, x)
		}
	}
	s.Groups = keep
	for _, sh := range s.Shares {
		al := []string{}
		for _, x := range sh.Allowed {
			if x != g.ID {
				al = append(al, x)
			}
		}
		sh.Allowed = al
	}
	return "group deleted: " + g.Name, nil
}

func (s *Server) aMachineGroup(dec func(any) error) (string, error) {
	var in struct{ ID, Group string }
	if err := dec(&in); err != nil {
		return "", err
	}
	m := s.Machines[in.ID]
	if m == nil {
		return "", fmt.Errorf("ไม่พบเครื่อง")
	}
	if in.Group != "" && s.group(in.Group) == nil {
		return "", fmt.Errorf("ไม่พบกลุ่ม")
	}
	m.Group = in.Group
	return fmt.Sprintf("machine %s -> group %q", m.ID, in.Group), nil
}

func (s *Server) aMachineDelete(dec func(any) error) (string, error) {
	var in struct{ ID string }
	if err := dec(&in); err != nil {
		return "", err
	}
	m := s.Machines[in.ID]
	if m == nil {
		return "", fmt.Errorf("ไม่พบเครื่อง")
	}
	if m.online {
		return "", fmt.Errorf("เครื่องยังออนไลน์อยู่ ลบไม่ได้")
	}
	for _, sh := range s.Shares {
		if sh.Host == m.ID {
			return "", fmt.Errorf("ยกเลิกการแชร์เครื่องพิมพ์ของเครื่องนี้ก่อน")
		}
	}
	delete(s.Machines, m.ID)
	return "machine removed: " + m.ID, nil
}

func (s *Server) aShareCreate(dec func(any) error) (string, error) {
	var in struct {
		Machine string `json:"machine"`
		Printer string `json:"printer"`
	}
	if err := dec(&in); err != nil {
		return "", err
	}
	m := s.Machines[in.Machine]
	if m == nil {
		return "", fmt.Errorf("ไม่พบเครื่อง")
	}
	var lp *LocalPrinter
	for i := range m.Printers {
		if m.Printers[i].Name == in.Printer {
			lp = &m.Printers[i]
		}
	}
	if lp == nil {
		return "", fmt.Errorf("ไม่พบเครื่องพิมพ์ในเครื่องนี้")
	}
	for _, sh := range s.Shares {
		if sh.Host == m.ID && sh.Real == lp.Name {
			return "", fmt.Errorf("แชร์เครื่องพิมพ์นี้อยู่แล้ว")
		}
	}
	s.NextPort++
	if s.NextPort == s.cfg.UIPort {
		s.NextPort++
	}
	s.NextShare++
	sh := &Share{ID: fmt.Sprintf("P%d", s.NextShare), Name: short(m.Hostname+" - "+lp.Name, 80),
		Host: m.ID, Real: lp.Name, Driver: lp.Driver, Port: s.NextPort, Allowed: []string{}, Enabled: true}
	if m.Group != "" {
		sh.Allowed = append(sh.Allowed, m.Group)
	}
	s.Shares = append(s.Shares, sh)
	return fmt.Sprintf("share %s created: %s@%s port %d", sh.ID, lp.Name, m.ID, sh.Port), nil
}

func (s *Server) aShareUpdate(dec func(any) error) (string, error) {
	var in struct {
		ID      string    `json:"id"`
		Name    *string   `json:"name"`
		Driver  *string   `json:"driver"`
		Allowed *[]string `json:"allowed"`
		Enabled *bool     `json:"enabled"`
	}
	if err := dec(&in); err != nil {
		return "", err
	}
	sh := s.share(in.ID)
	if sh == nil {
		return "", fmt.Errorf("ไม่พบเครื่องพิมพ์ที่แชร์")
	}
	if in.Allowed != nil {
		for _, g := range *in.Allowed {
			if s.group(g) == nil {
				return "", fmt.Errorf("ไม่พบกลุ่ม")
			}
		}
	}
	if in.Name != nil {
		n := strings.TrimSpace(*in.Name)
		if n == "" || len([]rune(n)) > 80 {
			return "", fmt.Errorf("ชื่อไม่ถูกต้อง")
		}
		sh.Name = n
	}
	if in.Driver != nil {
		sh.Driver = strings.TrimSpace(*in.Driver)
	}
	if in.Allowed != nil {
		sh.Allowed = append([]string{}, *in.Allowed...)
	}
	if in.Enabled != nil {
		sh.Enabled = *in.Enabled
	}
	return fmt.Sprintf("share %s updated (enabled=%v allowed=%v)", sh.ID, sh.Enabled, sh.Allowed), nil
}

func (s *Server) aShareDelete(dec func(any) error) (string, error) {
	var in struct{ ID string }
	if err := dec(&in); err != nil {
		return "", err
	}
	sh := s.share(in.ID)
	if sh == nil {
		return "", fmt.Errorf("ไม่พบเครื่องพิมพ์ที่แชร์")
	}
	keep := []*Share{}
	for _, x := range s.Shares {
		if x.ID != sh.ID {
			keep = append(keep, x)
		}
	}
	s.Shares = keep
	for _, j := range s.Jobs {
		if j.Printer == sh.ID && j.Status == "pending" {
			j.Status, j.Error = "cancelled", "ยกเลิกการแชร์เครื่องพิมพ์"
			os.Remove(s.jobPath(j.ID))
		}
	}
	return "share deleted: " + sh.Name, nil
}

func (s *Server) aJob(action string) func(func(any) error) (string, error) {
	return func(dec func(any) error) (string, error) {
		var in struct {
			ID string `json:"id"`
		}
		if err := dec(&in); err != nil || !validName(in.ID) {
			return "", fmt.Errorf("bad request")
		}
		j := s.find(in.ID)
		if j == nil {
			return "", fmt.Errorf("ไม่พบงาน")
		}
		switch action {
		case "retry":
			if j.Status != "error" {
				return "", fmt.Errorf("พิมพ์ใหม่ได้เฉพาะงานที่ผิดพลาด")
			}
			if _, err := os.Stat(s.jobPath(j.ID)); err != nil {
				return "", fmt.Errorf("ไม่พบข้อมูลงาน")
			}
			j.Status, j.Error, j.Sent, j.Done, j.Created = "pending", "", nil, nil, time.Now()
		case "cancel":
			if j.Status != "pending" {
				return "", fmt.Errorf("ยกเลิกได้เฉพาะงานที่รออยู่")
			}
			j.Status, j.Error = "cancelled", "ยกเลิกโดย admin"
			os.Remove(s.jobPath(j.ID))
		}
		return fmt.Sprintf("job %s %s", j.ID, action), nil
	}
}
