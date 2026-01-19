// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	colorModeAlways = "always"
	colorModeAuto   = "auto"
	colorModeNever  = "never"
)

var ansiEscapeCode = regexp.MustCompile("\x1b\\[[0-9;]*m")

func normalizeColorMode(mode string) string {
	return strings.ToLower(strings.TrimSpace(mode))
}

func shouldUseColor() bool {
	switch normalizeColorMode(colorMode) {
	case colorModeAlways:
		return true
	case colorModeNever:
		return false
	case colorModeAuto:
		return isTerminal(os.Stdout) || isTerminal(os.Stderr)
	default:
		log.Warnf("unknown --color=%s, fallback to %s", colorMode, colorModeAlways)
		return true
	}
}

func configureLogging() {
	switch normalizeColorMode(colorMode) {
	case colorModeAlways:
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
	case colorModeNever:
		log.SetFormatter(&log.TextFormatter{DisableColors: true})
	case colorModeAuto:
		// Use logrus default auto behavior.
	default:
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
	}
}

func isTerminal(file *os.File) bool {
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func stripANSIEscapeCodes(input string) string {
	return ansiEscapeCode.ReplaceAllString(input, "")
}
