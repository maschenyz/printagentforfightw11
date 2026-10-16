//go:build !windows

package main

import "fmt"

func printRaw(printer, docName string, data []byte) error { return fmt.Errorf("Windows only") }
func listPrinters() ([]LocalPrinter, error)               { return nil, fmt.Errorf("Windows only") }
func ensureVirtual(name, driver string, port int) string  { return "Windows only" }
func removeVirtual(name, portName string)                 {}
