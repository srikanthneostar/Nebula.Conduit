package api

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type processRSSReader func() (uint64, error)
type memoryLimitReader func() (uint64, error)
type processCPUReader func() (float64, error)
type cpuCapacityReader func() (float64, error)
type timeReader func() time.Time

func newResourceMonitor(maxMemoryPercent, maxCPUPercent float64, checkInterval time.Duration) *ResourceMonitor {
	return &ResourceMonitor{
		maxMemoryPercent: maxMemoryPercent,
		maxCPUPercent:    maxCPUPercent,
		checkInterval:    checkInterval,
		now:              time.Now,
		readProcessRSS:   defaultProcessRSSBytes,
		readMemoryLimit:  defaultMemoryLimitBytes,
		readProcessCPU:   defaultProcessCPURawPercent,
		readCPUCapacity:  defaultCPUCapacity,
	}
}

func (rm *ResourceMonitor) ensureFreshStats() {
	now := rm.nowFunc()()

	rm.mu.RLock()
	needsRefresh := rm.lastMemoryCheck.IsZero() ||
		rm.lastCPUCheck.IsZero() ||
		now.Sub(rm.lastMemoryCheck) > rm.checkInterval ||
		now.Sub(rm.lastCPUCheck) > rm.checkInterval
	rm.mu.RUnlock()

	if needsRefresh {
		rm.updateResourceUsage()
	}
}

func (rm *ResourceMonitor) updateResourceUsage() {
	now := rm.nowFunc()()

	memoryUsage, memoryErr := rm.sampleMemoryUsagePercent()
	cpuUsage, cpuErr := rm.sampleCPUUsagePercent()

	rm.mu.Lock()
	if memoryErr == nil {
		rm.memoryUsage = memoryUsage
		rm.lastMemoryCheck = now
	}
	if cpuErr == nil {
		rm.cpuUsage = cpuUsage
		rm.lastCPUCheck = now
	}
	rm.mu.Unlock()

	if memoryErr != nil {
		log.Warn().Err(memoryErr).Msg("Failed to sample process memory usage")
	}
	if cpuErr != nil {
		log.Warn().Err(cpuErr).Msg("Failed to sample process CPU usage")
	}
}

func (rm *ResourceMonitor) sampleMemoryUsagePercent() (float64, error) {
	processRSS, err := rm.processRSSFunc()()
	if err != nil {
		return 0, err
	}

	memoryLimit, err := rm.memoryLimitFunc()()
	if err != nil {
		return 0, err
	}
	if memoryLimit == 0 {
		return 0, fmt.Errorf("memory limit must be greater than zero")
	}

	return (float64(processRSS) / float64(memoryLimit)) * 100, nil
}

func (rm *ResourceMonitor) sampleCPUUsagePercent() (float64, error) {
	rawCPUPercent, err := rm.processCPUFunc()()
	if err != nil {
		return 0, err
	}

	cpuCapacity, err := rm.cpuCapacityFunc()()
	if err != nil {
		return 0, err
	}
	if cpuCapacity <= 0 {
		return 0, fmt.Errorf("cpu capacity must be greater than zero")
	}

	return math.Max(0, rawCPUPercent/cpuCapacity), nil
}

func (rm *ResourceMonitor) nowFunc() timeReader {
	if rm.now != nil {
		return rm.now
	}
	return time.Now
}

func (rm *ResourceMonitor) processRSSFunc() processRSSReader {
	if rm.readProcessRSS != nil {
		return rm.readProcessRSS
	}
	return defaultProcessRSSBytes
}

func (rm *ResourceMonitor) memoryLimitFunc() memoryLimitReader {
	if rm.readMemoryLimit != nil {
		return rm.readMemoryLimit
	}
	return defaultMemoryLimitBytes
}

func (rm *ResourceMonitor) processCPUFunc() processCPUReader {
	if rm.readProcessCPU != nil {
		return rm.readProcessCPU
	}
	return defaultProcessCPURawPercent
}

func (rm *ResourceMonitor) cpuCapacityFunc() cpuCapacityReader {
	if rm.readCPUCapacity != nil {
		return rm.readCPUCapacity
	}
	return defaultCPUCapacity
}

func defaultProcessRSSBytes() (uint64, error) {
	switch runtime.GOOS {
	case "windows":
		value, err := runAndParseFloat("powershell", "-NoProfile", "-Command", fmt.Sprintf("(Get-Process -Id %d).WorkingSet64", os.Getpid()))
		if err != nil {
			return 0, err
		}
		return uint64(value), nil
	default:
		rssKB, err := runAndParseFloat("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid()))
		if err != nil {
			return 0, err
		}

		return uint64(rssKB * 1024), nil
	}
}

func defaultProcessCPURawPercent() (float64, error) {
	switch runtime.GOOS {
	case "windows":
		command := fmt.Sprintf("$p=(Get-Process -Id %d).CPU; Start-Sleep -Milliseconds 250; $q=(Get-Process -Id %d).CPU; (($q-$p)*100/0.25)", os.Getpid(), os.Getpid())
		return runAndParseFloat("powershell", "-NoProfile", "-Command", command)
	default:
		return runAndParseFloat("ps", "-o", "%cpu=", "-p", strconv.Itoa(os.Getpid()))
	}
}

func defaultMemoryLimitBytes() (uint64, error) {
	if limit, ok, err := detectCgroupMemoryLimit(); err != nil {
		return 0, err
	} else if ok {
		return limit, nil
	}

	switch runtime.GOOS {
	case "darwin":
		value, err := runAndParseFloat("sysctl", "-n", "hw.memsize")
		if err != nil {
			return 0, err
		}
		return uint64(value), nil
	case "linux":
		return detectLinuxHostMemoryBytes()
	case "windows":
		value, err := runAndParseFloat("powershell", "-NoProfile", "-Command", "((Get-CimInstance Win32_OperatingSystem).TotalVisibleMemorySize * 1024)")
		if err != nil {
			return 0, err
		}
		return uint64(value), nil
	default:
		return 0, fmt.Errorf("memory limit detection not implemented for %s", runtime.GOOS)
	}
}

func defaultCPUCapacity() (float64, error) {
	if capacity, ok, err := detectCgroupCPUCapacity(); err != nil {
		return 0, err
	} else if ok {
		return capacity, nil
	}

	return float64(runtime.NumCPU()), nil
}

func detectCgroupMemoryLimit() (uint64, bool, error) {
	paths := []string{
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	}

	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, false, err
		}

		value := strings.TrimSpace(string(content))
		if value == "" || value == "max" {
			continue
		}

		limit, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("parse cgroup memory limit from %s: %w", path, err)
		}
		if limit == 0 {
			continue
		}

		hostMemory, err := detectLinuxHostMemoryBytes()
		if err == nil && hostMemory > 0 && limit >= hostMemory {
			continue
		}

		return limit, true, nil
	}

	return 0, false, nil
}

func detectLinuxHostMemoryBytes() (uint64, error) {
	content, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}

	for _, line := range strings.Split(string(content), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}

		memKB, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse MemTotal: %w", err)
		}

		return memKB * 1024, nil
	}

	return 0, fmt.Errorf("MemTotal not found in /proc/meminfo")
}

func detectCgroupCPUCapacity() (float64, bool, error) {
	if quota, period, ok, err := detectCgroupV2CPUQuota(); err != nil {
		return 0, false, err
	} else if ok && quota > 0 && period > 0 {
		return math.Max(float64(quota)/float64(period), 1), true, nil
	}

	quotaPath := "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
	periodPath := "/sys/fs/cgroup/cpu/cpu.cfs_period_us"

	quota, err := readInt64FromFile(quotaPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return 0, false, err
		}
		return 0, false, nil
	}

	period, err := readInt64FromFile(periodPath)
	if err != nil {
		return 0, false, err
	}

	if quota <= 0 || period <= 0 {
		return 0, false, nil
	}

	return math.Max(float64(quota)/float64(period), 1), true, nil
}

func detectCgroupV2CPUQuota() (int64, int64, bool, error) {
	content, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}

	fields := strings.Fields(strings.TrimSpace(string(content)))
	if len(fields) < 2 || fields[0] == "max" {
		return 0, 0, false, nil
	}

	quota, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse cpu quota: %w", err)
	}

	period, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse cpu period: %w", err)
	}

	return quota, period, true, nil
}

func readInt64FromFile(path string) (int64, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(strings.TrimSpace(string(content)), 10, 64)
}

func runAndParseFloat(name string, args ...string) (float64, error) {
	cmd := exec.Command(name, args...)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	value := strings.TrimSpace(string(output))
	if value == "" {
		return 0, fmt.Errorf("%s returned empty output", name)
	}

	return strconv.ParseFloat(value, 64)
}
