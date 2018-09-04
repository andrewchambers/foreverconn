package foreverconn

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
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
	UnacknowledgedBufSize   uint64
	UnacknowledgedByteCount uint64
	UnacknowledgedStreamPos uint64
	CurrentReadStreamPos    uint64
}

func SendRecvStreamState(rw io.ReadWriter, s streamState) (streamState, error) {

	var theirState streamState

	werr := make(chan error, 1)

	// Make offset exchange concurrently
	go func() {
		err := binary.Write(rw, binary.BigEndian, s.UnacknowledgedBufSize)
		if err != nil {
			werr <- err
			return
		}
		err = binary.Write(rw, binary.BigEndian, s.UnacknowledgedByteCount)
		if err != nil {
			werr <- err
			return
		}
		err = binary.Write(rw, binary.BigEndian, s.UnacknowledgedStreamPos)
		if err != nil {
			werr <- err
			return
		}
		err = binary.Write(rw, binary.BigEndian, s.CurrentReadStreamPos)
		if err != nil {
			werr <- err
			return
		}
		werr <- nil
	}()

	err := binary.Read(rw, binary.BigEndian, &theirState.UnacknowledgedBufSize)
	if err != nil {
		return streamState{}, err
	}

	err := binary.Read(rw, binary.BigEndian, &theirState.UnacknowledgedByteCount)
	if err != nil {
		return streamState{}, err
	}

	err := binary.Read(rw, binary.BigEndian, &theirState.UnacknowledgedStreamPos)
	if err != nil {
		return streamState{}, err
	}

	err := binary.Read(rw, binary.BigEndian, &theirState.CurrentReadStreamPos)
	if err != nil {
		return streamState{}, err
	}

	err = <-werr
	if err != nil {
		return streamState{}, err
	}

	return theirState, nil
}

const IOUNIT = 4096

type persistentState struct {
	inIoUnits            chan []byte
	outIoUnits           chan []byte
	mutex                sync.Mutex
	currentReadStreamPos uint64
	lastSentAck          uint64
	ub                   *unacknowledgedBuffer
}

func newUnacknowledgedBuffer(bufsz int) *unacknowledgedBuffer {
	if bufsz < IOUNIT {
		panic("BUFSZ SMALLER THAN IOUNIT")
	}

	return &unacknowledgedBuffer{
		data: make([]byte, bufsz, 0),
	}
}

type unacknowledgedBuffer struct {
	data      []byte
	streamPos uint64
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

func (ub *unacknowledgedBuffer) StreamPos() uint64 {
	return ub.streamPos
}

func (ub *unacknowledgedBuffer) AckTill(offset uint64) error {
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
	return nil
}

func (ub *unacknowledgedBuffer) AwaitNFree(n uint64) chan struct{} {
	ch := make(chan struct{}, 1)
	panic("UNIMPLEMENTED")
}

func Proxy(persistent io.ReadWriteCloser, reconnect func() (io.ReadWriteCloser, error)) {

	st := &persistentState{
		inIoUnits:  make(chan []byte, 1),
		outIoUnits: make(chan []byte, 1),
		ub:         newUnacknowledgedBuffer(1024 * 1024),
	}
	persistentErrors := make(chan error, 1)

	ctx, ctxCancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	once := &sync.Once{}

	cancel := func() {
		wg.Done()
		once.Do(func() {
			_ = persistent.Close()
			ctxCancel()
		})
	}

	wg.Add(1)
	defer cancel()
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer cancel()

		buf := make([]byte, IOUNIT, IOUNIT)
		for {
			n, err := persistent.Read(buf)
			if err != nil {
				select {
				case persistentErrors <- err:
				default:
				}
			}
			select {
			case st.inIoUnits <- buf[:n]:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer cancel()

		select {
		case buf := <-st.outIoUnits:
			n, err := persistent.Write(buf)
			if err != nil {
				persistentErrors <- err
				return
			}
		case <-ctx.Done():
			return
		}
	}()

	for {
		transient, err := reconnect()
		if err != nil {
			// XXX parameter?.
			time.Sleep(5 * time.Second)
			continue
		}

		proxy(st, transient)

		select {
		case err := <-persistentErrors:
			return
		case <-ctx.Done():
			return
		}
	}
}

func proxy(ctx context.Context, st *persistentState, transient io.ReadWriteCloser) {

	packetOut := make(chan Packet, 1)
	packetIn := make(chan Packet, 1)

	ctx, ctxCancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	once := &sync.Once{}

	cancel := func() {
		wg.Done()
		once.Do(func() {
			_ = transient.Close()
			ctxCancel()
		})
	}

	wg.Add(1)
	defer cancel()
	defer wg.Wait()

	st.mutex.Lock()
	ss := streamState{
		UnacknowledgedBufSize:   st.ub.Size(),
		UnacknowledgedByteCount: st.ub.Used(),
		UnacknowledgedStreamPos: st.ub.StreamPos(),
		CurrentReadStreamPos:    st.currentReadStreamPos,
	}
	st.mutex.Unlock()

	theirState, err := SendRecvStreamState(transient, ss)
	if err != nil {
		return
	}

	// XXX TODO resend rerecv unacknowledged

	st.mutex.Lock()
	// XXX TODO recalculate stream positions
	// XXX
	st.mutex.Unlock()

	wg.Add(1)
	go func() {
		defer cancel()

		for {
			select {
			case p := <-packetOut:
				_, err := WritePacket(transient, p)
				if err != nil {
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
			notifyReady := st.ub.AwaitNFree(IOUNIT)
			st.mutex.Unlock()

			select {
			case <-notifyReady:
			case <-ctx.Done():
				return
			}

			var toSend []byte

			select {
			case toSend = <-st.inIoUnits:
			case <-ctx.Done():
				return
			}

			st.mutex.Lock()
			st.ub.Add(toSend)
			currentReadStreamPos := st.readStreamPos
			st.lastSentAck = currentReadStreamPos
			st.mutex.Unlock()

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
			err := st.ub.AckTill(p.Ack)
			st.mutex.Unlock()
			if err != nil {
				st <- p
				return
			}

			select {
			case st.outIoUnits <- p.Data:
			case <-ctx.Done():
				return
			}

			st.mutex.Lock()
			st.currentReadStreamPos += len(p.Data)
			readStreamPos := st.currentReadStreamPos
			lastSentAck := st.lastSentAck
			st.mutex.Unlock()

			if readStreamPos-lastSentAck > (ss.UnacknowledgedBufSize / 2) {
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

	wg.Wait()
}
