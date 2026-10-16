//go:build windows

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

var (
	winspool   = syscall.NewLazyDLL("winspool.drv")
	pOpen      = winspool.NewProc("OpenPrinterW")
	pClose     = winspool.NewProc("ClosePrinter")
	pStartDoc  = winspool.NewProc("StartDocPrinterW")
	pEndDoc    = winspool.NewProc("EndDocPrinter")
	pStartPage = winspool.NewProc("StartPagePrinter")
	pEndPage   = winspool.NewProc("EndPagePrinter")
	pWrite     = winspool.NewProc("WritePrinter")
)

type docInfo1 struct {
	docName  *uint16
	outFile  *uint16
	datatype *uint16
}

// printRaw ส่ง byte ดิบเข้า spooler ของ Windows (datatype RAW) ไม่ผ่านการ render ของไดรเวอร์
func printRaw(printer, docName string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty job")
	}
	name, err := syscall.UTF16PtrFromString(printer)
	if err != nil {
		return err
	}
	var h syscall.Handle
	if r, _, e := pOpen.Call(uintptr(unsafe.Pointer(name)), uintptr(unsafe.Pointer(&h)), 0); r == 0 {
		return fmt.Errorf("OpenPrinter %q: %v", printer, e)
	}
	defer pClose.Call(uintptr(h))

	dn, _ := syscall.UTF16PtrFromString(docName)
	dt, _ := syscall.UTF16PtrFromString("RAW")
	di := docInfo1{docName: dn, datatype: dt}
	if r, _, e := pStartDoc.Call(uintptr(h), 1, uintptr(unsafe.Pointer(&di))); r == 0 {
		return fmt.Errorf("StartDocPrinter: %v", e)
	}
	defer pEndDoc.Call(uintptr(h))

	if r, _, e := pStartPage.Call(uintptr(h)); r == 0 {
		return fmt.Errorf("StartPagePrinter: %v", e)
	}
	defer pEndPage.Call(uintptr(h))

	for off := 0; off < len(data); {
		var written uint32
		r, _, e := pWrite.Call(uintptr(h), uintptr(unsafe.Pointer(&data[off])),
			uintptr(len(data)-off), uintptr(unsafe.Pointer(&written)))
		if r == 0 {
			return fmt.Errorf("WritePrinter: %v", e)
		}
		off += int(written)
	}
	return nil
}

// ---------- PowerShell helpers ----------
// ค่าที่มาจากแม่ (ชื่อเครื่องพิมพ์ ไดรเวอร์) ส่งผ่าน environment variable เท่านั้น
// ไม่นำไปต่อเป็นข้อความในสคริปต์ เพื่อกัน command injection

func psOut(script string, env ...string) (string, int, error) {
	cmd := exec.Command("powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	cmd.Env = append(os.Environ(), env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	out, err := cmd.Output()
	if ee, ok := err.(*exec.ExitError); ok {
		return string(out), ee.ExitCode(), nil
	}
	return string(out), 0, err
}

const listScript = `[Console]::OutputEncoding=[System.Text.Encoding]::UTF8
$ErrorActionPreference='SilentlyContinue'
$o = @(Get-Printer | Where-Object { $_.PortName -notlike 'HOSP_*' } | ForEach-Object {
  [pscustomobject]@{ name=$_.Name; driver=$_.DriverName; port=$_.PortName; status=[string]$_.PrinterStatus; jobs=[int]$_.JobCount } })
ConvertTo-Json -InputObject $o -Compress`

func listPrinters() ([]LocalPrinter, error) {
	out, code, err := psOut(listScript)
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, fmt.Errorf("Get-Printer exit %d", code)
	}
	out = strings.TrimPrefix(strings.TrimSpace(out), "\ufeff")
	list := []LocalPrinter{}
	if out == "" {
		return list, nil
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return nil, fmt.Errorf("parse printers: %v", err)
	}
	return list, nil
}

const ensureScript = `[Console]::OutputEncoding=[System.Text.Encoding]::UTF8
$ErrorActionPreference='Stop'
try {
  $name=$env:PS_NAME; $drv=$env:PS_DRIVER; $pn=$env:PS_PORTNAME; $port=[int]$env:PS_PORT
  if (-not (Get-PrinterDriver -Name $drv -ErrorAction SilentlyContinue)) { exit 3 }
  $p = Get-Printer -Name $name -ErrorAction SilentlyContinue
  if ($p -and $p.PortName -ne $pn) { exit 4 }
  if (-not (Get-PrinterPort -Name $pn -ErrorAction SilentlyContinue)) {
    Add-PrinterPort -Name $pn -PrinterHostAddress '127.0.0.1' -PortNumber $port
  }
  if (-not $p) { Add-Printer -Name $name -DriverName $drv -PortName $pn }
  exit 0
} catch { Write-Output $_.Exception.Message; exit 9 }`

// ensureVirtual สร้างเครื่องพิมพ์จำลอง คืน "ok" หรือข้อความอธิบายปัญหา
func ensureVirtual(name, driver string, port int) string {
	out, code, err := psOut(ensureScript,
		"PS_NAME="+name, "PS_DRIVER="+driver, "PS_PORTNAME="+portName(port), "PS_PORT="+strconv.Itoa(port))
	switch {
	case err != nil:
		return "เรียก PowerShell ไม่ได้: " + err.Error()
	case code == 0:
		return "ok"
	case code == 3:
		return "ไม่พบไดรเวอร์ \"" + driver + "\" ในเครื่องนี้ (ต้องติดตั้งไดรเวอร์ก่อน)"
	case code == 4:
		return "มีเครื่องพิมพ์ชื่อ \"" + name + "\" อยู่แล้วที่ไม่ใช่ของระบบ (ให้ admin เปลี่ยนชื่อที่แชร์)"
	}
	msg := short(strings.TrimSpace(out), 150)
	l := strings.ToLower(msg)
	if strings.Contains(l, "denied") || strings.Contains(l, "access") || strings.Contains(msg, "สิทธิ์") {
		return "ไม่มีสิทธิ์สร้างเครื่องพิมพ์ ต้องรัน agent ด้วย Run as administrator"
	}
	return "สร้างไม่สำเร็จ: " + msg
}

const removeScript = `$ErrorActionPreference='SilentlyContinue'
$p = Get-Printer -Name $env:PS_NAME
if ($p -and $p.PortName -eq $env:PS_PORTNAME) { Remove-Printer -Name $env:PS_NAME }
if (Get-PrinterPort -Name $env:PS_PORTNAME) { Remove-PrinterPort -Name $env:PS_PORTNAME }`

// removeVirtual ลบเฉพาะเครื่องพิมพ์ที่ผูกกับพอร์ต HOSP_* ของเรา
func removeVirtual(name, portName string) {
	psOut(removeScript, "PS_NAME="+name, "PS_PORTNAME="+portName)
}
