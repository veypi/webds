package scan

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/veypi/utils/log"
	"net"
	"os"
	"sync"
	"text/tabwriter"
	"time"
)

func GetAllInterface() []net.Interface {
	var ifs []net.Interface
	var err error
	ifs, err = net.Interfaces()
	if err != nil {
		log.Warn().Msgf("无法获取本地网络信息:", err)
		return nil
	}
	res := make([]net.Interface, 0, 10)
	for _, it := range ifs {
		addr, _ := it.Addrs()
		for _, a := range addr {
			if ip, ok := a.(*net.IPNet); ok && !ip.IP.IsLoopback() {
				if ip.IP.To4() != nil {
					res = append(res, it)
					break
				}
			}
		}
	}
	return res
}

func ScanAllIP() []string {
	res := make([]string, 0, 10)
	ifs := GetAllInterface()
	for _, iface := range ifs {
		ips, err := Scan(&iface)
		if err != nil {
			log.Warn().Msg(err.Error())
			continue
		}
		res = append(res, ips...)
	}
	return res
}

// scan scans an individual interface's local network for machines using ARP requests/replies.
//
// scan loops forever, sending packets out regularly.  It returns an error if
// it's ever unable to write a packet.
func Scan(iface *net.Interface) ([]string, error) {
	// We just look for IPv4 addresses, so try to find if the interface has one.
	var addr *net.IPNet
	if addrs, err := iface.Addrs(); err != nil {
		return nil, err
	} else {
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				if ip4 := ipnet.IP.To4(); ip4 != nil {
					addr = &net.IPNet{
						IP:   ip4,
						Mask: ipnet.Mask[len(ipnet.Mask)-4:],
					}
					break
				}
			}
		}
	}
	// Sanity-check that the interface has a good address.
	if addr == nil {
		return nil, errors.New("no good IP network found")
	} else if addr.IP[0] == 127 {
		return nil, errors.New("skipping localhost")
	} else if addr.Mask[0] != 0xff || addr.Mask[1] != 0xff {
		return nil, errors.New("mask means network is too large")
	}
	log.Info().Msgf("Using network range %v for interface %v", addr, iface.Name)

	// Open up a pcap handle for packet reads/writes.
	handle, err := pcap.OpenLive(iface.Name, 65536, true, pcap.BlockForever)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	// Start up a goroutine to read in packet data.
	stop := make(chan string)
	go readARP(handle, iface, stop)
	defer close(stop)
	// Write our scan packets out to the handle.
	if err := writeARP(handle, iface, addr); err != nil {
		log.Info().Msgf("error writing packets on %v: %v", iface.Name, err)
		return nil, err
	}
	// We don't know exactly how long it'll take for packets to be
	// sent back to us, but 10 seconds should be more than enough
	// time ;)
	res := make([]string, 0, 10)
	for {
		ip := <-stop
		if ip != "" {
			res = append(res, ip)
		} else {
			return res, nil
		}
	}
}

func Port(ips []string, port uint, c chan string) {
	h := ""
	for _, i := range ips {
		h = fmt.Sprintf("%s:%d", i, port)
		if isOpen(h) {
			c <- h
		}
	}
}

func Ports(c chan string, ips []string, ports ...uint) {
	for _, p := range ports {
		Port(ips, p, c)
	}
}

func PortRange(ips []string, min uint, max uint, c chan string) {
	if min > max {
		min, max = max, min
	}
	if min < 1 || min > 65535 {
		min = 1
	}
	if max > 65535 || max < 1 {
		max = 65535
	}
	p := min
	g := sync.WaitGroup{}
	g.Add(int(max - min + 1))
	for {
		go func(p uint) {
			defer g.Done()
			defer func() {
				if e := recover(); e != nil {
					log.Error().Err(nil).Msgf("Scan Port error: %v", e)
				}
			}()
			Port(ips, p, c)
		}(p)
		if p == max {
			break
		}
		p++
	}
	g.Wait()
	close(c)
}

// readARP watches a handle for incoming ARP responses we might care about, and prints them.
//
// readARP loops until 'stop' is closed.
func readARP(handle *pcap.Handle, iface *net.Interface, stop chan string) {
	w := tabwriter.NewWriter(os.Stdout, 20, 20, 1, ' ', tabwriter.DiscardEmptyColumns)
	src := gopacket.NewPacketSource(handle, layers.LayerTypeEthernet)
	in := src.Packets()
	var t = time.After(time.Second * 2)
	for {
		var packet gopacket.Packet
		select {
		case <-t:
			stop <- ""
		case packet = <-in:
			if packet == nil {
				continue
			}
			arpLayer := packet.Layer(layers.LayerTypeARP)
			if arpLayer == nil {
				continue
			}
			arp := arpLayer.(*layers.ARP)
			if arp.Operation != layers.ARPReply || bytes.Equal(iface.HardwareAddr, arp.SourceHwAddress) {
				// This is a packet I sent.
				continue
			}
			// Note:  we might get some packets here that aren't responses to ones we've sent,
			// if for example someone else sends US an ARP request.  Doesn't much matter, though...
			// all information is good information :)

			stop <- net.IP(arp.SourceProtAddress).String()
			_ = w.Flush()
			t = time.After(time.Second * 2)
		}
	}
}

// writeARP writes an ARP request for each address on our local network to the
// pcap handle.
func writeARP(handle *pcap.Handle, iface *net.Interface, addr *net.IPNet) error {
	// Set up all the layers' fields we can.
	eth := layers.Ethernet{
		SrcMAC:       iface.HardwareAddr,
		DstMAC:       net.HardwareAddr{0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		EthernetType: layers.EthernetTypeARP,
	}
	arp := layers.ARP{
		AddrType:          layers.LinkTypeEthernet,
		Protocol:          layers.EthernetTypeIPv4,
		HwAddressSize:     6,
		ProtAddressSize:   4,
		Operation:         layers.ARPRequest,
		SourceHwAddress:   []byte(iface.HardwareAddr),
		SourceProtAddress: []byte(addr.IP),
		DstHwAddress:      []byte{0, 0, 0, 0, 0, 0},
	}
	// Set up buffer and options for serialization.
	buf := gopacket.NewSerializeBuffer()
	opts := gopacket.SerializeOptions{
		FixLengths:       true,
		ComputeChecksums: true,
	}
	// Send one packet for every address.
	for _, ip := range ips(addr) {
		arp.DstProtAddress = ip
		_ = gopacket.SerializeLayers(buf, opts, &eth, &arp)
		if err := handle.WritePacketData(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// ips is a simple and not very good method for getting all IPv4 addresses from a
// net.IPNet.  It returns all IPs it can over the channel it sends back, closing
// the channel when done.
func ips(n *net.IPNet) (out []net.IP) {
	num := binary.BigEndian.Uint32(n.IP)
	mask := binary.BigEndian.Uint32(n.Mask)
	num &= mask
	for mask < 0xffffffff {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], num)
		out = append(out, buf[:])
		mask++
		num++
	}
	return
}
func isOpen(host string) bool {
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
