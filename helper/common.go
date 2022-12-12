package helper

import (
	"bytes"
	"errors"
	"github.com/google/uuid"
	"net"
	"strings"
	"sync"
)

func GenerateNodeId() string {
	id, _ := uuid.NewUUID()
	return strings.Replace(id.String(), "-", "", -1)
}

func GetOutboundIP() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}

			return ip, nil
		}
	}

	return nil, errors.New("ip not found")
}

func IsPublicIP(IP net.IP) bool {
	if IP.IsLoopback() || IP.IsLinkLocalMulticast() || IP.IsLinkLocalUnicast() {
		return false
	}
	if ip4 := IP.To4(); ip4 != nil {
		switch true {
		case ip4[0] == 10:
			return false
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return false
		case ip4[0] == 192 && ip4[1] == 168:
			return false
		default:
			return true
		}
	}
	return false
}

func MergeChannels[T any](chs ...chan T) chan T {
	out := make(chan T)
	var wg sync.WaitGroup
	wg.Add(len(chs))
	for _, ch := range chs {
		go func(c <-chan T) {
			defer wg.Done()
			for v := range c {
				out <- v
			}
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func IsContains[T comparable](value T, arr []T) bool {
	for _, a := range arr {
		if a == value {
			return true
		}
	}
	return false
}

func FindDiff[T comparable](src []T, trg []T) []T {
	var found []T
	for _, s := range src {
		if !IsContains(s, trg) {
			found = append(found, s)
		}
	}
	return found
}

func HasSameElement[T comparable](src []T, trg []T) bool {
	for _, s := range src {
		if IsContains(s, trg) {
			return true
		}
	}
	return false
}

func HasAllElements[T comparable](src []T, trg []T) bool {
	if len(src) == 0 || len(trg) == 0 {
		return false
	}
	for _, s := range src {
		if !IsContains(s, trg) {
			return false
		}
	}
	return true
}

func RoundRobinSelection[T any](list []T) func() T {
	offset := 0
	return func() T { // round-robin write for fragments if topic write policy has UniquePerFragment
		selected := list[offset]
		offset = (offset + 1) % len(list)
		return selected
	}
}

func IsContainBytes(e []byte, s [][]byte) bool {
	for _, a := range s {
		if bytes.Compare(a, e) == 0 {
			return true
		}
	}
	return false
}
