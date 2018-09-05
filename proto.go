package foreverconn

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
	"log"
)

type Packet struct {
	Ack  uint64
	Data []byte
}

func WritePacket(w io.Writer, p Packet) error {
	err := binary.Write(w, binary.BigEndian, p.Ack)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, uint32(len(p.Data)))
	if err != nil {
		return err
	}

	_, err = w.Write(p.Data)
	if err != nil {
		return err
	}

	return nil
}

func ReadPacket(r io.Reader) (Packet, error) {
	var ack uint64
	var n uint32

	err := binary.Read(r, binary.BigEndian, &ack)
	if err != nil {
		return Packet{}, err
	}

	err = binary.Read(r, binary.BigEndian, &n)
	if err != nil {
		return Packet{}, err
	}

	buf := make([]byte, n, n)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return Packet{}, err
	}

	return Packet{Ack: ack, Data: buf}, nil
}

type streamState struct {
	CurrentReadStreamPos    uint64
	UnacknowledgedBufSize   uint64
	UnacknowledgedStreamPos uint64
	UnacknowledgedBytes     []byte
}

func SendRecvStreamState(rw io.ReadWriter, s streamState) (streamState, error) {

	var theirState streamState

	werr := make(chan error, 1)

	// Make offset exchange concurrently
	go func() {
		err := binary.Write(rw, binary.BigEndian, s.CurrentReadStreamPos)
		if err != nil {
			werr <- err
			return
		}
		err = binary.Write(rw, binary.BigEndian, s.UnacknowledgedBufSize)
		if err != nil {
			werr <- err
			return
		}
		err = binary.Write(rw, binary.BigEndian, s.UnacknowledgedStreamPos)
		if err != nil {
			werr <- err
			return
		}

		err = binary.Write(rw, binary.BigEndian, uint32(len(s.UnacknowledgedBytes)))
		if err != nil {
			werr <- err
			return
		}

		_, err = rw.Write(s.UnacknowledgedBytes)
		if err != nil {
			werr <- err
			return
		}

		werr <- nil
	}()

	err := binary.Read(rw, binary.BigEndian, &theirState.CurrentReadStreamPos)
	if err != nil {
		return streamState{}, err
	}

	err = binary.Read(rw, binary.BigEndian, &theirState.UnacknowledgedBufSize)
	if err != nil {
		return streamState{}, err
	}

	err = binary.Read(rw, binary.BigEndian, &theirState.UnacknowledgedStreamPos)
	if err != nil {
		return streamState{}, err
	}

	var unacknowledgedByteCount uint32

	err = binary.Read(rw, binary.BigEndian, &unacknowledgedByteCount)
	if err != nil {
		return streamState{}, err
	}

	unacknowledgedBytes := make([]byte, unacknowledgedByteCount, unacknowledgedByteCount)

	_, err = io.ReadFull(rw, unacknowledgedBytes)
	if err != nil {
		return streamState{}, err
	}
	theirState.UnacknowledgedBytes = unacknowledgedBytes

	err = <-werr
	if err != nil {
		return streamState{}, err
	}

	return theirState, nil
}

const READSZ = 4096

type persistentState struct {
	persistentIn         chan []byte
	persistentOut        chan []byte
	mutex                sync.Mutex
	currentReadStreamPos uint64
	lastSentAck          uint64
	ub                   *unacknowledgedBuffer
}

// XXX ring buffer would be better
func newUnacknowledgedBuffer(bufsz int) *unacknowledgedBuffer {
	// The minimum buffer is enough space for two in flight sends
	// while the ack for the first is on its way back.

	if bufsz/READSZ < 2 {
		bufsz = READSZ * 2
	}

	return &unacknowledgedBuffer{
		data: make([]byte, 0, bufsz),
	}
}

type unacknowledgedBuffer struct {
	data      []byte
	streamPos uint64

	waiter chan struct{}
}

func (ub *unacknowledgedBuffer) Add(b []byte) {
	if ub.Free() < uint64(len(b)) {
		panic("BUG")
	}

	ub.data = append(ub.data, b...)
	ub.streamPos += uint64(len(b))
}

func (ub *unacknowledgedBuffer) Size() uint64 {
	return uint64(cap(ub.data))
}

func (ub *unacknowledgedBuffer) Used() uint64 {
	return uint64(len(ub.data))
}

func (ub *unacknowledgedBuffer) Free() uint64 {
	return ub.Size() - ub.Used()
}

func (ub *unacknowledgedBuffer) Bytes() []byte {
	return ub.data
}

func (ub *unacknowledgedBuffer) StreamPos() uint64 {
	return ub.streamPos
}

func (ub *unacknowledgedBuffer) AckedTill(offset uint64) error {
	if offset > ub.streamPos {
		return errors.New("bad ack offset")
	}
	nToClear := ub.streamPos - offset
	nBuffered := uint64(len(ub.data))
	if nToClear > nBuffered {
		return errors.New("bad ack offset")
	}
	newSize := nBuffered - nToClear
	for i := uint64(0); i < nToClear; i++ {
		ub.data[i] = ub.data[i+nToClear]
	}
	ub.data = ub.data[0:newSize]

	if ub.Free() >= READSZ {
		if ub.waiter != nil {
			select {
			case ub.waiter <- struct{}{}:
			default:
			}
			ub.waiter = nil
		}
	}

	return nil
}

func (ub *unacknowledgedBuffer) AwaitReadSzFree() chan struct{} {
	// We only need/support a single waiter...
	ub.waiter = make(chan struct{}, 1)
	if ub.Free() >= READSZ {
		ub.waiter <- struct{}{}
	}
	return ub.waiter
}

func Proxy(persistent io.ReadWriteCloser, reconnect func() (io.ReadWriteCloser, error)) error {

	st := &persistentState{
		persistentIn:  make(chan []byte, 1),
		persistentOut: make(chan []byte, 1),
		ub:            newUnacknowledgedBuffer(1024 * 1024),
	}
	persistentErrors := make(chan error, 1)

	ctx, ctxCancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	once := &sync.Once{}

	cancel := func() {
		wg.Done()
		once.Do(func() {
			log.Print("persistent session shutting down")
			_ = persistent.Close()
			ctxCancel()
		})
	}

	wg.Add(1)
	go func() {
		defer cancel()
		
		for {
			buf := make([]byte, READSZ, READSZ)
			n, err := persistent.Read(buf)
			if err != nil {
				select {
				case persistentErrors <- err:
				default:
				}
				return
			}
			select {
			case st.persistentIn <- buf[:n]:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer cancel()

		for {
		select {
			case buf := <-st.persistentOut:
				_, err := persistent.Write(buf)
				if err != nil {
					select {
					case persistentErrors <- err:
					default:
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		transient, err := reconnect()
		if err != nil {
			log.Printf("transient reconnect failed: %s", err)
			time.Sleep(5 * time.Second)
			continue
		}

		proxy(ctx, st, transient)

		select {
		case err := <-persistentErrors:
			return err
		case <-ctx.Done():
			return nil
		}
	}

}

func proxy(ctx context.Context, st *persistentState, transient io.ReadWriteCloser) {

	packetOut := make(chan Packet, 1)
	packetIn := make(chan Packet, 1)

	ctx, ctxCancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	once := &sync.Once{}

	cancel := func() {
		wg.Done()
		once.Do(func() {
			log.Print("transient session shutting down")
			_ = transient.Close()
			ctxCancel()
		})
	}

	st.mutex.Lock()
	ss := streamState{
		CurrentReadStreamPos:    st.currentReadStreamPos,
		UnacknowledgedBufSize:   st.ub.Size(),
		UnacknowledgedStreamPos: st.ub.StreamPos(),
		UnacknowledgedBytes:     st.ub.Bytes(),
	}
	st.mutex.Unlock()

	theirState, err := SendRecvStreamState(transient, ss)
	if err != nil {
		log.Printf("syncing stream state failed: %s", err)
		return
	}

	log.Printf("got stream states : %#v %#v", ss, theirState)

	select {
	case st.persistentOut <- theirState.UnacknowledgedBytes:
		st.mutex.Lock()
		st.ub.AckedTill(st.ub.StreamPos() + st.ub.Used())
		st.mutex.Unlock()
	case <-ctx.Done():
		return
	}

	wg.Add(1)
	go func() {
		defer cancel()

		for {
			select {
			case p := <-packetOut:
				err := WritePacket(transient, p)
				if err != nil {
					log.Printf("transient io error: %s", err)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer cancel()

		for {
			p, err := ReadPacket(transient)
			if err != nil {
				log.Printf("transient io error: %s", err)
				return
			}
			select {
			case packetIn <- p:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer cancel()

		for {

			st.mutex.Lock()
			notifyReady := st.ub.AwaitReadSzFree()
			st.mutex.Unlock()

			log.Printf("awaiting read space in buffer")
			select {
			case <-notifyReady:
			case <-ctx.Done():
				return
			}
			log.Printf("enough read space in buffer")

			var toSend []byte

			log.Printf("waiting for next read")
			select {
			case toSend = <-st.persistentIn:
			case <-ctx.Done():
				return
			}
			log.Printf("read %d bytes", len(toSend))

			st.mutex.Lock()
			st.ub.Add(toSend)
			currentReadStreamPos := st.currentReadStreamPos
			st.lastSentAck = currentReadStreamPos
			st.mutex.Unlock()

			log.Printf("sending some data and an ack till offset %d", currentReadStreamPos)
			select {
			case packetOut <- Packet{Data: toSend, Ack: currentReadStreamPos}:
			case <-ctx.Done():
				return
			}

		}
	}()

	wg.Add(1)
	go func() {
		defer cancel()

		for {
			var p Packet

			select {
			case p = <-packetIn:
			case <-ctx.Done():
				return
			}

			st.mutex.Lock()
			err := st.ub.AckedTill(p.Ack)
			st.mutex.Unlock()
			if err != nil {
				return
			}

			select {
			case st.persistentOut <- p.Data:
			case <-ctx.Done():
				return
			}

			st.mutex.Lock()
			st.currentReadStreamPos += uint64(len(p.Data))
			readStreamPos := st.currentReadStreamPos
			lastSentAck := st.lastSentAck
			st.mutex.Unlock()

			if readStreamPos-lastSentAck >= (ss.UnacknowledgedBufSize / 2) {
				// The remote end is running out of buffer space.
				// Preemptively send him an ack, but only if
				// we aren't already sending one.
				// We don't want to block here to ensure we are
				// are always reading packets.
				select {
				case packetOut <- Packet{Ack: readStreamPos}:
				default:
				}
			}
		}
	}()
}
