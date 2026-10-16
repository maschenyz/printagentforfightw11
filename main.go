package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	AutoConnect bool `json:"auto_connect"` // false = ผู้ใช้กด "เชื่อมต่อ" เองที่หน้าสถานะ, true = สร้างให้อัตโนมัติทุกเครื่องที่ได้สิทธิ์
	AgentID       string `json:"agent_id"`   // ว่าง = ใช้ชื่อเครื่อง
	ServerURL     ServerList `json:"server_url"` // ที่อยู่ตัวควบคุม (ถ้าเครื่องนี้เป็นตัวควบคุมเอง ปล่อยว่างได้)
	Token         string `json:"token"`
	DataDir       string `json:"data_dir"`
	UIPort        int    `json:"ui_port"`         // หน้าสถานะของเครื่องนี้ (localhost เท่านั้น)
	ServerPort    int    `json:"server_port"`     // > 0 = เครื่องนี้เป็นตัวควบคุมด้วย
	AdminPassword string `json:"admin_password"`  // ใช้เฉพาะตัวควบคุม
	JobTTLMinutes int    `json:"job_ttl_minutes"` // งานค้างเกินนี้จะถูกยกเลิก ไม่พิมพ์สลากเก่า
}

var Hostname string

func (c *Config) outbox() string { return filepath.Join(c.DataDir, "outbox") }

const svcName = "PrintAgent"

func main() {
	cfgPath := flag.String("config", "config.json", "path to config.json")
	install := flag.Bool("install", false, "ติดตั้งเป็น Windows Service แล้วเริ่มทำงาน (ต้องรันแบบ Administrator)")
	uninstall := flag.Bool("uninstall", false, "ถอด Windows Service ออก (ต้องรันแบบ Administrator)")
	flag.Parse()

	if *install {
		installService(*cfgPath)
		return
	}
	if *uninstall {
		uninstallService()
		return
	}
	if runAsService(func() { runAgent(*cfgPath) }) { // ถูกเรียกโดย Service Manager
		return
	}
	runAgent(*cfgPath) // โหมดเปิดเอง (console)
	select {}
}

func runAgent(cfgPath string) {
	cfg := loadConfig(cfgPath)
	setupLog(cfg)
	must(os.MkdirAll(cfg.outbox(), 0o755))
	if cfg.ServerPort > 0 {
		startServer(cfg)
	}
	startAgent(cfg)
}

func loadConfig(path string) *Config {
	abs, err := filepath.Abs(path)
	must(err)
	b, err := os.ReadFile(abs)
	must(err)
	var c Config
	must(json.Unmarshal(b, &c))
	Hostname, _ = os.Hostname()
	if c.AgentID == "" {
		c.AgentID = sanitizeID(Hostname)
	}
	if c.DataDir == "" {
		c.DataDir = "data"
	}
	if !filepath.IsAbs(c.DataDir) {
		c.DataDir = filepath.Join(filepath.Dir(abs), c.DataDir)
	}
	if c.UIPort == 0 {
		c.UIPort = 9030
	}
	if c.JobTTLMinutes == 0 {
		c.JobTTLMinutes = 30
	}
	if len(c.ServerURL) == 0 && c.ServerPort > 0 {
		c.ServerURL = ServerList{fmt.Sprintf("http://127.0.0.1:%d", c.ServerPort)}
	}
	if len(c.ServerURL) == 0 {
		log.Fatal("config: ต้องใส่ server_url (ที่อยู่ตัวควบคุม) หรือ server_port (ถ้าเครื่องนี้เป็นตัวควบคุมเอง)")
	}
	return &c
}

func okRune(c rune) bool {
	return c == '_' || c == '-' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func sanitizeID(s string) string {
	var b strings.Builder
	for _, c := range s {
		if okRune(c) {
			b.WriteRune(c)
		} else {
			b.WriteByte('-')
		}
	}
	r := b.String()
	if len(r) > 40 {
		r = r[:40]
	}
	if r == "" {
		r = "PC"
	}
	return r
}

func short(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

// ---------- log เป็นไฟล์ txt ----------

func logPath(c *Config) string { return filepath.Join(c.DataDir, "printsys.log") }

func setupLog(c *Config) {
	os.MkdirAll(c.DataDir, 0o755)
	p := logPath(c)
	if fi, err := os.Stat(p); err == nil && fi.Size() > 2<<20 {
		os.Rename(p, p+".old") // เกิน 2MB เก็บของเก่าไว้ 1 ไฟล์ ที่เหลือทิ้ง
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err == nil {
		log.SetOutput(io.MultiWriter(os.Stderr, f))
	}
}

func tailLog(c *Config, n int) []string {
	f, err := os.Open(logPath(c))
	if err != nil {
		return []string{}
	}
	defer f.Close()
	fi, _ := f.Stat()
	off := fi.Size() - 16384
	if off < 0 {
		off = 0
	}
	f.Seek(off, io.SeekStart)
	b, _ := io.ReadAll(f)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if off > 0 && len(lines) > 0 {
		lines = lines[1:]
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
