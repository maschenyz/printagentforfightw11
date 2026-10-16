//go:build windows

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

type svcHandler struct{ start func() }

func (h *svcHandler) Execute(args []string, r <-chan svc.ChangeRequest, s chan<- svc.Status) (bool, uint32) {
	s <- svc.Status{State: svc.StartPending}
	go h.start() // ถ้าเริ่มไม่ได้ (config ผิด ฯลฯ) โปรเซสจะจบเอง แล้ว Service Manager สั่งเริ่มใหม่ตามที่ตั้งไว้
	s <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
	for c := range r {
		switch c.Cmd {
		case svc.Interrogate:
			s <- c.CurrentStatus
		case svc.Stop, svc.Shutdown:
			s <- svc.Status{State: svc.StopPending}
			return false, 0
		}
	}
	return false, 0
}

// runAsService คืน true ถ้าโปรเซสนี้ถูกเรียกโดย Service Manager (และทำงานจนถูกสั่งหยุดแล้ว)
func runAsService(start func()) bool {
	is, err := svc.IsWindowsService()
	if err != nil || !is {
		return false
	}
	if exe, err := os.Executable(); err == nil {
		os.Chdir(filepath.Dir(exe)) // service เริ่มจาก C:\Windows\System32
	}
	if err := svc.Run(svcName, &svcHandler{start: start}); err != nil {
		log.Fatalf("service: %v", err)
	}
	return true
}

func installService(cfgPath string) {
	exe, err := os.Executable()
	must(err)
	exe, err = filepath.Abs(exe)
	must(err)
	cfgAbs, err := filepath.Abs(cfgPath)
	must(err)
	if _, err := os.Stat(cfgAbs); err != nil {
		log.Fatalf("ไม่พบไฟล์ config: %s", cfgAbs)
	}
	m, err := mgr.Connect()
	if err != nil {
		log.Fatalf("เชื่อมต่อ Service Manager ไม่ได้ (ต้องรัน PowerShell แบบ Run as administrator): %v", err)
	}
	defer m.Disconnect()
	if s, err := m.OpenService(svcName); err == nil {
		s.Close()
		log.Fatalf("มี service %s อยู่แล้ว ใช้ -uninstall ถอดก่อน", svcName)
	}
	s, err := m.CreateService(svcName, exe, mgr.Config{
		DisplayName: "Print Agent",
		Description: "ระบบกลางจัดการเครื่องพิมพ์ (agent)",
		StartType:   mgr.StartAutomatic,
	}, "-config", cfgAbs)
	if err != nil {
		log.Fatalf("สร้าง service ไม่สำเร็จ: %v", err)
	}
	defer s.Close()
	// ถ้าโปรเซสล้ม ให้ Windows เริ่มใหม่ให้เอง
	s.SetRecoveryActions([]mgr.RecoveryAction{
		{Type: mgr.ServiceRestart, Delay: 5 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 30 * time.Second},
		{Type: mgr.ServiceRestart, Delay: time.Minute},
	}, 86400)
	if err := s.Start(); err != nil {
		log.Fatalf("ติดตั้งแล้วแต่เริ่มไม่ได้: %v", err)
	}
	fmt.Printf("ติดตั้งและเริ่ม service %s แล้ว\nconfig: %s\n", svcName, cfgAbs)
}

func uninstallService() {
	m, err := mgr.Connect()
	if err != nil {
		log.Fatalf("เชื่อมต่อ Service Manager ไม่ได้ (ต้องรันแบบ Run as administrator): %v", err)
	}
	defer m.Disconnect()
	s, err := m.OpenService(svcName)
	if err != nil {
		log.Fatalf("ไม่พบ service %s", svcName)
	}
	defer s.Close()
	s.Control(svc.Stop)
	for i := 0; i < 20; i++ {
		st, err := s.Query()
		if err != nil || st.State == svc.Stopped {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := s.Delete(); err != nil {
		log.Fatalf("ลบ service ไม่สำเร็จ: %v", err)
	}
	fmt.Printf("ถอด service %s แล้ว\n", svcName)
}
