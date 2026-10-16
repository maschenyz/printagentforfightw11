//go:build !windows

package main

import "fmt"

func runAsService(start func()) bool { return false }
func installService(cfgPath string)  { fmt.Println("รองรับเฉพาะ Windows") }
func uninstallService()              { fmt.Println("รองรับเฉพาะ Windows") }
