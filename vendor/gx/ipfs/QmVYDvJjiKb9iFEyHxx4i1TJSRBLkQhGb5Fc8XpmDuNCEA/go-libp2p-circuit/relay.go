package relay

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	pb "gx/ipfs/QmVYDvJjiKb9iFEyHxx4i1TJSRBLkQhGb5Fc8XpmDuNCEA/go-libp2p-circuit/pb"

	ma "gx/ipfs/QmT4U94DnD8FRfqr21obWY32HLM5VExccPKMjQHofeYqr9/go-multiaddr"
	peer "gx/ipfs/QmTRhk7cgjUf2gfQ3p2M9KPECNZEW9XUrmHcFCgog4cPgB/go-libp2p-peer"
	pstore "gx/ipfs/QmTTJcDL3gsnGDALjh2fDGg1onGRUdVgNL2hU2WEZcVrMX/go-libp2p-peerstore"
	inet "gx/ipfs/QmXuRkCR7BNQa9uqfpTiFWsTQLzmTWYg91Ja1w95gnqb6u/go-libp2p-net"
	logging "gx/ipfs/QmZChCsSt8DctjceaL56Eibc29CVQq4dGKRXC5JRZ6Ppae/go-log"
	host "gx/ipfs/QmdJfsSbKSZnMkfZ1kpopiyB9i3Hd6cp8VKWZmtWPa7Moc/go-libp2p-host"
	tptu "gx/ipfs/QmeUjhpfGkrMtE6s7JjU2xLhfzprSDh16QUxGVr3wTrKSx/go-libp2p-transport-upgrader"
)

var log = logging.Logger("relay")

const ProtoID = "/libp2p/circuit/relay/0.1.0"

const maxMessageSize = 4096

var RelayAcceptTimeout = time.Minute
var HopConnectTimeout = 10 * time.Second

type Relay struct {
	host     host.Host
	upgrader *tptu.Upgrader
	ctx      context.Context
	self     peer.ID

	active    bool
	hop       bool
	discovery bool

	incoming chan *Conn

	relays map[peer.ID]struct{}
	mx     sync.Mutex

	liveHops map[peer.ID]map[peer.ID]int
	lhCount  uint64
	lhLk     sync.Mutex
}

type RelayOpt int

var (
	OptActive    = RelayOpt(0)
	OptHop       = RelayOpt(1)
	OptDiscovery = RelayOpt(2)
)

type RelayError struct {
	Code pb.CircuitRelay_Status
}

func (e RelayError) Error() string {
	return fmt.Sprintf("error opening relay circuit: %s (%d)", pb.CircuitRelay_Status_name[int32(e.Code)], e.Code)
}

func NewRelay(ctx context.Context, h host.Host, upgrader *tptu.Upgrader, opts ...RelayOpt) (*Relay, error) {
	r := &Relay{
		upgrader: upgrader,
		host:     h,
		ctx:      ctx,
		self:     h.ID(),
		incoming: make(chan *Conn),
		relays:   make(map[peer.ID]struct{}),
		liveHops: make(map[peer.ID]map[peer.ID]int),
	}

	for _, opt := range opts {
		switch opt {
		case OptActive:
			r.active = true
		case OptHop:
			r.hop = true
		case OptDiscovery:
			r.discovery = true
		default:
			return nil, fmt.Errorf("unrecognized option: %d", opt)
		}
	}

	h.SetStreamHandler(ProtoID, r.handleNewStream)

	if r.discovery {
		h.Network().Notify(r.Notifiee())
	}

	return r, nil
}

func (r *Relay) addLiveHop(from, to peer.ID) {
	r.lhLk.Lock()
	defer r.lhLk.Unlock()

	trg, ok := r.liveHops[from]
	if !ok {
		trg = make(map[peer.ID]int)
		r.liveHops[from] = trg
	}
	trg[to]++
	r.lhCount++
}

func (r *Relay) rmLiveHop(from, to peer.ID) {
	r.lhLk.Lock()
	defer r.lhLk.Unlock()

	trg, ok := r.liveHops[from]
	if !ok {
		return
	}
	var count int
	if count, ok = trg[to]; !ok {
		return
	}
	count--

	r.lhCount--
	if count <= 0 {
		delete(trg, to)
		if len(trg) == 0 {
			delete(r.liveHops, from)
		}
	} else {
		trg[to] = count
	}
}

func (r *Relay) GetActiveHops() uint64 {
	r.lhLk.Lock()
	defer r.lhLk.Unlock()

	return r.lhCount
}

func (r *Relay) DialPeer(ctx context.Context, relay pstore.PeerInfo, dest pstore.PeerInfo) (*Conn, error) {

	log.Debugf("dialing peer %s through relay %s", dest.ID, relay.ID)

	if len(relay.Addrs) > 0 {
		r.host.Peerstore().AddAddrs(relay.ID, relay.Addrs, pstore.TempAddrTTL)
	}

	s, err := r.host.NewStream(ctx, relay.ID, ProtoID)
	if err != nil {
		return nil, err
	}

	rd := newDelimitedReader(s, maxMessageSize)
	wr := newDelimitedWriter(s)

	var msg pb.CircuitRelay

	msg.Type = pb.CircuitRelay_HOP.Enum()
	msg.SrcPeer = peerInfoToPeer(r.host.Peerstore().PeerInfo(r.self))
	msg.DstPeer = peerInfoToPeer(dest)

	err = wr.WriteMsg(&msg)
	if err != nil {
		s.Reset()
		return nil, err
	}

	msg.Reset()

	err = rd.ReadMsg(&msg)
	if err != nil {
		s.Reset()
		return nil, err
	}

	if msg.GetType() != pb.CircuitRelay_STATUS {
		s.Reset()
		return nil, fmt.Errorf("unexpected relay response; not a status message (%d)", msg.GetType())
	}

	if msg.GetCode() != pb.CircuitRelay_SUCCESS {
		s.Reset()
		return nil, RelayError{msg.GetCode()}
	}

	return &Conn{Stream: s, remote: dest}, nil
}

func (r *Relay) Matches(addr ma.Multiaddr) bool {
	// TODO: Look at the prefix transport as well.
	_, err := addr.ValueForProtocol(P_CIRCUIT)
	return err == nil
}

func (r *Relay) CanHop(ctx context.Context, id peer.ID) (bool, error) {
	s, err := r.host.NewStream(ctx, id, ProtoID)
	if err != nil {
		return false, err
	}

	rd := newDelimitedReader(s, maxMessageSize)
	wr := newDelimitedWriter(s)

	var msg pb.CircuitRelay

	msg.Type = pb.CircuitRelay_CAN_HOP.Enum()

	if err := wr.WriteMsg(&msg); err != nil {
		s.Reset()
		return false, err
	}

	msg.Reset()

	if err := rd.ReadMsg(&msg); err != nil {
		s.Reset()
		return false, err
	}
	if err := inet.FullClose(s); err != nil {
		return false, err
	}

	if msg.GetType() != pb.CircuitRelay_STATUS {
		return false, fmt.Errorf("unexpected relay response; not a status message (%d)", msg.GetType())
	}

	return msg.GetCode() == pb.CircuitRelay_SUCCESS, nil
}

func (r *Relay) handleNewStream(s inet.Stream) {
	log.Infof("new relay stream from: %s", s.Conn().RemotePeer())

	rd := newDelimitedReader(s, maxMessageSize)

	var msg pb.CircuitRelay

	err := rd.ReadMsg(&msg)
	if err != nil {
		r.handleError(s, pb.CircuitRelay_MALFORMED_MESSAGE)
		return
	}

	switch msg.GetType() {
	case pb.CircuitRelay_HOP:
		r.handleHopStream(s, &msg)
	case pb.CircuitRelay_STOP:
		r.handleStopStream(s, &msg)
	case pb.CircuitRelay_CAN_HOP:
		r.handleCanHop(s, &msg)
	default:
		log.Warningf("unexpected relay handshake: %d", msg.GetType())
		r.handleError(s, pb.CircuitRelay_MALFORMED_MESSAGE)
	}
}

func (r *Relay) handleHopStream(s inet.Stream, msg *pb.CircuitRelay) {
	if !r.hop {
		r.handleError(s, pb.CircuitRelay_HOP_CANT_SPEAK_RELAY)
		return
	}

	src, err := peerToPeerInfo(msg.GetSrcPeer())
	if err != nil {
		r.handleError(s, pb.CircuitRelay_HOP_SRC_MULTIADDR_INVALID)
		return
	}

	if src.ID != s.Conn().RemotePeer() {
		r.handleError(s, pb.CircuitRelay_HOP_SRC_MULTIADDR_INVALID)
		return
	}

	dst, err := peerToPeerInfo(msg.GetDstPeer())
	if err != nil {
		r.handleError(s, pb.CircuitRelay_HOP_DST_MULTIADDR_INVALID)
		return
	}

	if dst.ID == r.self {
		r.handleError(s, pb.CircuitRelay_HOP_CANT_RELAY_TO_SELF)
		return
	}

	// open stream
	ctp := r.host.Network().ConnsToPeer(dst.ID)

	if len(ctp) == 0 && !r.active {
		r.handleError(s, pb.CircuitRelay_HOP_NO_CONN_TO_DST)
		return
	}

	if len(dst.Addrs) > 0 {
		r.host.Peerstore().AddAddrs(dst.ID, dst.Addrs, pstore.TempAddrTTL)
	}

	ctx, cancel := context.WithTimeout(r.ctx, HopConnectTimeout)
	defer cancel()

	bs, err := r.host.NewStream(ctx, dst.ID, ProtoID)
	if err != nil {
		log.Debugf("error opening relay stream to %s: %s", dst.ID.Pretty(), err.Error())
		r.handleError(s, pb.CircuitRelay_HOP_CANT_DIAL_DST)
		return
	}

	// stop handshake
	rd := newDelimitedReader(bs, maxMessageSize)
	wr := newDelimitedWriter(bs)

	msg.Type = pb.CircuitRelay_STOP.Enum()

	err = wr.WriteMsg(msg)
	if err != nil {
		log.Debugf("error writing stop handshake: %s", err.Error())
		bs.Reset()
		r.handleError(s, pb.CircuitRelay_HOP_CANT_OPEN_DST_STREAM)
		return
	}

	msg.Reset()

	err = rd.ReadMsg(msg)
	if err != nil {
		log.Debugf("error reading stop response: %s", err.Error())
		bs.Reset()
		r.handleError(s, pb.CircuitRelay_HOP_CANT_OPEN_DST_STREAM)
		return
	}

	if msg.GetType() != pb.CircuitRelay_STATUS {
		log.Debugf("unexpected relay stop response: not a status message (%d)", msg.GetType())
		bs.Reset()
		r.handleError(s, pb.CircuitRelay_HOP_CANT_OPEN_DST_STREAM)
		return
	}

	if msg.GetCode() != pb.CircuitRelay_SUCCESS {
		log.Debugf("relay stop failure: %d", msg.GetCode())
		bs.Reset()
		r.handleError(s, msg.GetCode())
		return
	}

	err = r.writeResponse(s, pb.CircuitRelay_SUCCESS)
	if err != nil {
		log.Debugf("error writing relay response: %s", err.Error())
		bs.Reset()
		s.Reset()
		return
	}

	// relay connection
	log.Infof("relaying connection between %s and %s", src.ID.Pretty(), dst.ID.Pretty())

	r.addLiveHop(src.ID, dst.ID)

	// Don't reset streams after finishing or the other side will get an
	// error, not an EOF.
	go func() {
		defer r.rmLiveHop(src.ID, dst.ID)

		count, err := io.Copy(s, bs)
		if err != nil {
			log.Debugf("relay copy error: %s", err)
			// Reset both.
			s.Reset()
			bs.Reset()
		} else {
			// propagate the close
			s.Close()
		}
		log.Debugf("relayed %d bytes from %s to %s", count, dst.ID.Pretty(), src.ID.Pretty())
	}()

	go func() {
		count, err := io.Copy(bs, s)
		if err != nil {
			log.Debugf("relay copy error: %s", err)
			// Reset both.
			bs.Reset()
			s.Reset()
		} else {
			// propagate the close
			bs.Close()
		}
		log.Debugf("relayed %d bytes from %s to %s", count, src.ID.Pretty(), dst.ID.Pretty())
	}()
}

func (r *Relay) handleStopStream(s inet.Stream, msg *pb.CircuitRelay) {
	src, err := peerToPeerInfo(msg.GetSrcPeer())
	if err != nil {
		r.handleError(s, pb.CircuitRelay_STOP_SRC_MULTIADDR_INVALID)
		return
	}

	dst, err := peerToPeerInfo(msg.GetDstPeer())
	if err != nil || dst.ID != r.self {
		r.handleError(s, pb.CircuitRelay_STOP_DST_MULTIADDR_INVALID)
		return
	}

	log.Infof("relay connection from: %s", src.ID)

	if len(src.Addrs) > 0 {
		r.host.Peerstore().AddAddrs(src.ID, src.Addrs, pstore.TempAddrTTL)
	}

	select {
	case r.incoming <- &Conn{Stream: s, remote: src}:
	case <-time.After(RelayAcceptTimeout):
		r.handleError(s, pb.CircuitRelay_STOP_RELAY_REFUSED)
	}
}

func (r *Relay) handleCanHop(s inet.Stream, msg *pb.CircuitRelay) {
	var err error

	if r.hop {
		err = r.writeResponse(s, pb.CircuitRelay_SUCCESS)
	} else {
		err = r.writeResponse(s, pb.CircuitRelay_HOP_CANT_SPEAK_RELAY)
	}

	if err != nil {
		s.Reset()
		log.Debugf("error writing relay response: %s", err.Error())
	} else {
		inet.FullClose(s)
	}
}

func (r *Relay) handleError(s inet.Stream, code pb.CircuitRelay_Status) {
	log.Warningf("relay error: %s (%d)", pb.CircuitRelay_Status_name[int32(code)], code)
	err := r.writeResponse(s, code)
	if err != nil {
		s.Reset()
		log.Debugf("error writing relay response: %s", err.Error())
	} else {
		inet.FullClose(s)
	}
}

func (r *Relay) writeResponse(s inet.Stream, code pb.CircuitRelay_Status) error {
	wr := newDelimitedWriter(s)

	var msg pb.CircuitRelay
	msg.Type = pb.CircuitRelay_STATUS.Enum()
	msg.Code = code.Enum()

	return wr.WriteMsg(&msg)
}
