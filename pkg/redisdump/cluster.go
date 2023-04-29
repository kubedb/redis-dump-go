package redisdump

import (
	"net"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func ParseRedisInfo(s string) (map[string]string, error) {
	lines := strings.Split(s, "\n")

	info := map[string]string{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		info[key] = val
	}
	return info, nil
}

type Node struct {
	Host  string
	Port  int
	Slots []Range
}

type Range struct {
	Start, End int
}

func GetMasterNodeAddresses(s string) ([]Node, error) {
	lines := strings.Split(s, "\n")

	var masters []Node
	for _, line := range lines {
		if strings.Contains(line, "master") {
			fields := strings.FieldsFunc(line, func(r rune) bool {
				return r == ' ' || r == '@'
			})

			host, port, err := net.SplitHostPort(fields[1])
			if err != nil {
				return nil, errors.Wrapf(err, "failed to split addr %s", fields[1])
			}
			p, err := strconv.Atoi(port)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse port in addr %s", fields[1])
			}
			m := Node{Host: host, Port: p}

			for i := len(fields) - 1; i >= 0; i-- {
				start, end, found := strings.Cut(fields[i], "-")
				if !found {
					break
				}
				s, err := strconv.Atoi(start)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to parse slot start for redis master %s with slots %s", m.Host, fields[i])
				}
				e, err := strconv.Atoi(end)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to parse slot end for redis master %s with slots %s", m.Host, fields[i])
				}
				m.Slots = append(m.Slots, Range{Start: s, End: e})
			}
			masters = append(masters, m)
		}
	}
	return masters, nil
}
