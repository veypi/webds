package libs

import (
	"context"
	"errors"
	"fmt"
	"github.com/sparrc/go-ping"
	"github.com/veypi/utils/log"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var cidrReg, _ = regexp.Compile("^(\\d+\\.\\d+\\.\\d+\\.\\d+)/(\\d+)$")

func handlePanic() {
	if e := recover(); e != nil {
		log.Error().Err(nil).Msgf("%v", e)
	}
}

func NewScanner(cidr string) (*scanner, error) {
	if len(cidrReg.FindAllStringSubmatch(cidr, -1)) == 0 {
		return nil, errors.New("invalid cidr: " + cidr)
	}
	if strings.HasPrefix(cidr, "169") {
		return nil, errors.New("un support cider range: " + cidr)
	}
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, err
	}
	var ips []string
	for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
		if ip4 := ip.To4(); ip4[3] != 0 {
			ips = append(ips, ip.String())
		}
	}
	s := &scanner{
		hosts:   ips,
		limiter: 10,
		timeout: time.Minute,
	}
	return s, nil
}

type scanner struct {
	hosts      []string
	limiter    int
	taskChan   chan string
	resultChan chan string
	ctx        context.Context
	cancel     context.CancelFunc
	timeout    time.Duration
}

func (s *scanner) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *scanner) SetLimiter(max int) {
	if max > 0 {
		s.limiter = max
	}
}

func (s *scanner) startScan(choice uint) {
	g := sync.WaitGroup{}
	for i := 0; i < s.limiter; i++ {
		g.Add(1)
		go func() {
			defer handlePanic()
			defer g.Done()
			for {
				select {
				case h := <-s.taskChan:
					if h == "" {
						return
					}
					if choice == 1 {
						if s.ping(h) {
							s.resultChan <- h
						}
					} else {
						if s.isOpen(h) {
							s.resultChan <- h
						}
					}
				case <-s.ctx.Done():
					return
				}
			}
		}()
	}
	g.Wait()
}

func (s *scanner) Scan() []string {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), s.timeout)
	defer s.cancel()
	s.taskChan = make(chan string, s.limiter*2)
	s.resultChan = make(chan string, s.limiter*2)
	results := make([]string, 0, 20)
	go func() {
		for _, h := range s.hosts {
			s.taskChan <- h
		}
		close(s.taskChan)
	}()
	go func() {
		for {
			select {
			case h := <-s.resultChan:
				if h == "" {
					return
				}
				results = append(results, h)
			case <-s.ctx.Done():
				return

			}
		}
	}()
	s.startScan(1)
	close(s.resultChan)
	return results
}

func (s *scanner) ScanPorts(ports ...uint) []string {
	strPorts := make([]string, len(ports))
	for i := range ports {
		strPorts[i] = fmt.Sprintf(":%d", ports[i])
	}
	aliveHosts := s.Scan()
	s.ctx, s.cancel = context.WithTimeout(context.Background(), s.timeout)
	defer s.cancel()
	s.taskChan = make(chan string, s.limiter*2)
	s.resultChan = make(chan string, s.limiter*2)

	go func() {
		for _, h := range aliveHosts {
			for _, p := range strPorts {
				s.taskChan <- h + p
			}
		}
		close(s.taskChan)
	}()
	results := make([]string, 0, 20)
	go func() {
		for {
			select {
			case h := <-s.resultChan:
				if h == "" {
					return
				}
				results = append(results, h)
			case <-s.ctx.Done():
				return

			}
		}
	}()
	s.startScan(0)
	close(s.resultChan)
	return results
}

func (s *scanner) ScanPortRange(min uint, max uint) []string {
	if min > max {
		min, max = max, min
	}
	if min < 1 || min > 65535 {
		min = 1
	}
	if max > 65535 || max < 1 {
		max = 65535
	}
	ports := make([]uint, max-min+1)
	for i := range ports {
		ports[i] = min + uint(i)
	}
	return s.ScanPorts(ports...)
}

func (s *scanner) ping(host string) bool {
	// TODO
	pinger, err := ping.NewPinger(host)
	if err != nil {
		return false
	}
	pinger.Count = 1
	pinger.Timeout = time.Millisecond * 10
	pinger.Run()                 // blocks until finished
	stats := pinger.Statistics() // get send/receive/rtt stats
	if stats.PacketsRecv > 0 {
		return true
	}
	return false
}

func (s *scanner) isOpen(host string) bool {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", host)
	if err != nil {
		return false
	}
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second/10)
	if err != nil {
		return false
	}

	defer conn.Close()

	return true
}
func GetLocalIps() []string {
	ips := make([]string, 0, 5)
	netInterfaces, err := net.Interfaces()
	if err != nil {
		log.Warn().Msg(err.Error())
		return nil
	}
	for _, inc := range netInterfaces {
		if inc.HardwareAddr.String() == "" {
			continue
		}
		addrs, err := inc.Addrs()
		if err != nil {
			log.Info().Msgf("get net addr failed: %s", err.Error())
			continue
		}
		for _, addr := range addrs {
			ip := addr.String()
			if !strings.Contains(ip, ":") && !strings.HasPrefix(ip, "127") {
				ips = append(ips, ip)
			}
		}
	}
	return ips
}

func freshConsole(format string, args ...interface{}) {
	_, _ = fmt.Fprint(os.Stdout, fmt.Sprintf(format+"\r", args...))
}

func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] == 255 {
			ip[j]++
		}
		if ip[j] > 0 {
			break
		}
	}
}
