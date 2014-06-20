# -*- Mode: Python -*-

"""
Implements the AMQP protocol for Shrapnel.
"""

import struct
import coro
import spec
import rpc
import sys

from pprint import pprint as pp
is_a = isinstance

W = sys.stderr.write

__version__ = '0.5.6'

def hd(s):
    return ''.join('%02x ' % (ord(i)) for i in s)


class ProtocolError (Exception):
    pass
class UnexpectedClose (Exception):
    pass

class UnknownConsumerTag (ProtocolError):
    def __init__(self, frame, props, data):
        self.frame = frame
        self.properties = props
        self.data = data

class UnexpectedFrame (ProtocolError):
    def __init__(self, names, ftype, channel, frame):
        self.names = names
        self.ftype = ftype
        self.channel = channel
        self.frame = frame

def dump_ob (ob):
    W ('%s {\n' % (ob._name,))
    for name in ob.__slots__:
        W ('  %s = %r\n' % (name, getattr (ob, name)))
    W ('}\n')

# sentinel for consumer fifos
connection_closed = 'connection closed'
consumer_cancelled = 'consumer cancelled'

class client:

    """*auth*: is a tuple of (username, password) for the 'PLAIN' authentication mechanism.

       *host*: string - IP address of the server.

       *port*: int -  TCP port number

       *virtual_host*: string - specifies which 'virtual host' to connect to.

       *heartbeat*: int - whether to request heartbeat mode from the
        server.  A value of 0 indicates no heartbeat wanted.  Any
        other value specifies the number of seconds of idle time from
        either side before a heartbeat frame is sent.
    """

    version = [0,0,9,1]
    buffer_size = 4000
    properties = {
        'product':'AMQP/shrapnel',
        'version':__version__,
        'information':'https://github.com/samrushing/amqp-shrapnel',
        'capabilities': {
            'publisher_confirms':True,
            }
        }

    def __init__ (self, auth, host, port=5672, virtual_host='/', heartbeat=0, consumer_cancel_notify=None, frame_max=0):
        self.port = port
        self.host = host
        self.auth = auth
        self.virtual_host = virtual_host
        self.heartbeat = heartbeat
        self.frame_state = 0
        self.frames = coro.fifo()
        # collect body parts here.  heh.
        self.body = []
        self.next_content_consumer = None
        self.next_properties = None
        self.consumers = {}
        self.last_sent_before = coro.tsc_time.now_raw_posix_fsec()
        self.last_sent_after = coro.tsc_time.now_raw_posix_fsec()
        self.channels = {}
        self._exception_handler = None
        self._send_completion_handler = None
        self._recv_completion_handler = None
        self.consumer_cancel_notify = consumer_cancel_notify
        self.frame_max = frame_max
        # XXX implement read/write "channels" (coro).
        self._recv_loop_thread = None
        self._s_recv_sema = coro.semaphore(1)
        self._s_send_sema = coro.semaphore(1)

    def set_exception_handler(self, h):
        self._exception_handler = h

    def set_send_completion_handler(self, h):
        self._send_completion_handler = h

    def set_recv_completion_handler(self, h):
        self._recv_completion_handler = h

    # state diagram for connection objects:
    #
    # connection          = open-connection *use-connection close-connection
    # open-connection     = C:protocol-header
    #                       S:START C:START-OK
    #                       *challenge
    #                       S:TUNE C:TUNE-OK
    #                       C:OPEN S:OPEN-OK
    # challenge           = S:SECURE C:SECURE-OK
    # use-connection      = *channel
    # close-connection    = C:CLOSE S:CLOSE-OK
    #                     / S:CLOSE C:CLOSE-OK

    def go (self):
        "Connect to the server.  Spawns a new thread to monitor the connection."
        self.s = coro.tcp_sock()
        self.s.connect ((self.host, self.port))
        self._s_send_sema.acquire(1)
        try:
            self.s.send ('AMQP' + struct.pack ('>BBBB', *self.version))
        finally:
            self._s_send_sema.release(1)
        self.buffer = self.s.recv (self.buffer_size)
        if self.buffer.startswith ('AMQP'):
            # server rejection
            raise ProtocolError (
                "version mismatch: server wants %r" % (
                    struct.unpack ('>4B', self.buffer[4:8])
                    )
                )
        else:
            self._recv_loop_thread = coro.spawn (self.recv_loop)
            # pull the first frame off (should be connection.start)
            ftype, channel, frame = self.expect_frame (spec.FRAME_METHOD, 'connection.start')
            #W ('connection start\n')
            #dump_ob (frame)
            mechanisms = frame.mechanisms.split()
            self.server_properties = frame.server_properties

            if self.consumer_cancel_notify is None:
                # adjsut to the server properties
                if self.server_properties['capabilities'].get('consumer_cancel_notify'):
                    self.consumer_cancel_notify = True
                    self.properties['capabilities']['consumer_cancel_notify'] = True

            else:
                if self.consumer_cancel_notify:
                    if not self.server_properties['capabilities'].get('consumer_cancel_notify'):
                        raise ProtocolError ('server capabilities says NO to consumer_cancel_notify')
                    self.properties['capabilities']['consumer_cancel_notify'] = True


            if 'PLAIN' in mechanisms:
                response = '\x00%s\x00%s' % self.auth
            else:
                raise AuthenticationError ("no shared auth mechanism: %r" % (mechanisms,))
            reply = spec.connection.start_ok (
                # XXX put real stuff in here...
                client_properties=self.properties,
                response=response
                )
            self.send_frame (spec.FRAME_METHOD, 0, reply)
            # XXX
            # XXX according to the 'grammar' from the spec, we might get a 'challenge' frame here.
            # XXX
            ftype, channel, frame = self.expect_frame (spec.FRAME_METHOD, 'connection.tune')
            self.tune = frame
            if self.frame_max:
                self.tune.frame_max = min(self.tune.frame_max, self.frame_max)
            # I'm AMQP, and I approve this tune value.
            self.send_frame (
                spec.FRAME_METHOD, 0,
                spec.connection.tune_ok (frame.channel_max, frame.frame_max, self.heartbeat)
                )
            # ok, ready to 'open' the connection.
            self.send_frame (
                spec.FRAME_METHOD, 0,
                spec.connection.open (self.virtual_host)
                )
            ftype, channel, frame = self.expect_frame (spec.FRAME_METHOD, 'connection.open_ok')

    def expect_frame (self, ftype, *names):
        "read/expect a frame from the list *names*"
        while True:
            ftype, channel, frame = self.frames.pop()
            if frame._name not in names:
                if self._exception_handler:
                    self._exception_handler(UnexpectedFrame(names, ftype, channel, frame))
                    continue
                else:
                    raise UnexpectedFrame(names, ftype, channel, frame)
            else:
                if self._recv_completion_handler:
                    self._recv_completion_handler(self, ftype, channel, frame)
                return ftype, channel, frame

    def secs_since_send (self):
        return coro.tsc_time.now_raw_posix_fsec() - self.last_sent_after

    def recv_loop (self):
        try:
            try:
                while 1:
                    while len (self.buffer):
                        self.unpack_frame()
                    self._s_recv_sema.acquire(1)
                    try:
                        if self.heartbeat:
                            block = coro.with_timeout (self.heartbeat * 2, self.s.recv, self.buffer_size)
                        else:
                            block = self.s.recv (self.buffer_size)
                    finally:
                        self._s_recv_sema.release(1)
                    if not block:
                        break
                    else:
                        self.buffer += block
                    if self.heartbeat and self.secs_since_send() > self.heartbeat:
                        self.send_frame (spec.FRAME_HEARTBEAT, 0, '')
            except coro.TimeoutError:
                # two heartbeat periods have expired with no data, so we let the
                #   connection close.  XXX Should we bother trying to call connection.close()?
                pass
            except Exception, e:
                if self._exception_handler:
                    self._exception_handler(e)
        finally:
            self.notify_channels_of_close()

    def unpack_frame (self):
        # unpack the frame sitting in self.buffer
        ftype, chan, size = struct.unpack ('>BHL', self.buffer[:7])
        #W ('<<< frame: ftype=%r channel=%r size=%d\n' % (ftype, chan, size))
        if size + 8 <= len(self.buffer) and self.buffer[7+size] == '\xce':
            # we have the whole frame
            # <head> <payload> <end> ...
            # [++++++++++++++++++++++++]
            payload, self.buffer = self.buffer[7:7+size], self.buffer[8+size:]
        else:
            # we need to fetch more data
            # <head> <payload> <end>
            # [++++++++++][--------]
            self._s_recv_sema.acquire(1)
            try:
                payload = self.buffer[7:] + self.s.recv_exact (size - (len(self.buffer) - 7))
                # fetch the frame end separately
                if self.s.recv_exact (1) != '\xce':
                    raise ProtocolError ("missing frame end")
                else:
                    self.buffer = ''
            finally:
                self._s_recv_sema.release(1)
        # -------------------------------------------
        # we have the payload, what do we do with it?
        # -------------------------------------------
        #W ('<<< frame: ftype=%r channel=%r size=%d payload=%r\n' % (ftype, chan, size, payload))
        if ftype == spec.FRAME_METHOD:
            cm_id = struct.unpack ('>hh', payload[:4])
            ob = spec.method_map[cm_id]()
            ob.unpack (payload, 4)
            #W ('<<< ')
            #dump_ob (ob)
            # catch asynchronous stuff here and ship it out...
            if is_a (ob, spec.basic.deliver):
                ch = self.channels.get (chan, None)
                if ch is None:
                    W ('warning, dropping delivery for unknown channel #%d consumer_tag=%r\n' % (chan, ob.consumer_tag))
                else:
                    self.next_content_consumer = (ob, ch)
            elif self.properties['capabilities'].get('consumer_cancel_notify') and is_a (ob, spec.basic.cancel):
                # This is meant to be the last message for this consumer
                # http://www.rabbitmq.com/extensions.html#consumer-cancel-notify
                ch = self.channels.get (chan, None)
                if ch is None:
                    W ('warning, dropping basic.cancel for unknown channel #%d consumer_tag=%r\n' % (chan, ob.consumer_tag))
                else:
                    ch.notify_consumer_of_cancel(ob.consumer_tag)
            else:
                self.frames.push ((ftype, chan, ob))
        elif ftype == spec.FRAME_HEADER:
            cid, weight, size, flags = struct.unpack ('>hhqH', payload[:14])
            #W ('<<< HEADER: cid=%d weight=%d size=%d flags=%x payload=%r\n' % (cid, weight, size, flags, hd(payload)))
            #W ('<<< self.buffer=%r\n' % (hd(self.buffer),))
            if flags:
                self.next_properties = unpack_properties (flags, payload[14:])
            else:
                self.next_properties = {}
            self.remain = size
        elif ftype == spec.FRAME_BODY:
            #W ('<<< FRAME_BODY, len(payload)=%d\n' % (len(payload),))
            self.remain -= len (payload)
            self.body.append (payload)
            if self.remain == 0:
                if self.next_content_consumer is not None:
                    ob, ch = self.next_content_consumer
                    ch.accept_delivery (ob, self.next_properties, self.body)
                    self.next_content_consumer = None
                    self.next_properties = None
                else:
                    W ('dropped data: %r\n' % (self.body,))
                self.body = []
        elif ftype == spec.FRAME_HEARTBEAT:
            #W ('<<< FRAME_HEARTBEAT\n')
            pass
        else:
            self.close (505, "unexpected frame type: %r" % (ftype,))
            raise ProtocolError ("unhandled frame type: %r" % (ftype,))

    def send_frame (self, ftype, channel, ob):
        "send a frame of type *ftype* on this channel.  *ob* is a frame object built by the <spec> module"
        f = []
        #W ('ftype=%s\n' % (ftype,))
        if ftype == spec.FRAME_METHOD:
            #dump_ob(ob)
            payload = struct.pack ('>hh', *ob.id) + str(ob.pack())
        elif ftype in (spec.FRAME_HEADER, spec.FRAME_BODY, spec.FRAME_HEARTBEAT):
            payload = str(ob)
        else:
            raise ProtocolError ("unhandled frame type: %r" % (ftype,))
        frame = struct.pack ('>BHL', ftype, channel, len (payload)) + payload + chr(spec.FRAME_END)
        #W ('>>> send_frame: %s ...\n' % (hd(frame),))
        self.last_sent_before = coro.tsc_time.now_raw_posix_fsec()
        self._s_send_sema.acquire(1)
        try:
            self.s.send (frame)
        finally:
            self._s_send_sema.release(1)
        self.last_sent_after = coro.tsc_time.now_raw_posix_fsec()
        if self._send_completion_handler:
            self._send_completion_handler(self, ftype, channel)

    def close (self, reply_code=200, reply_text='normal shutdown', class_id=0, method_id=0):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#connection.close"
        # close any open channels first.
        try:
            self.notify_channels_of_close()
            self.send_frame (
                spec.FRAME_METHOD, 0,
                spec.connection.close (reply_code, reply_text, class_id, method_id)
                )
            ftype, channel, frame = self.expect_frame (spec.FRAME_METHOD, 'connection.close_ok')

        finally:
            try:
                self.s.close()
            except:
                pass

            self._recv_loop_thread.shutdown()
            self._recv_loop_thread.join()
            self._recv_loop_thread = None


    def channel (self, out_of_band=''):
        """Create a new channel on this connection.
        
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#connection.channel
        """
        chan = channel (self)
        chan.set_exception_handler(self._exception_handler)
        self.send_frame (spec.FRAME_METHOD, chan.num, spec.channel.open (out_of_band))
        ftype, chan_num, frame = self.expect_frame (spec.FRAME_METHOD, 'channel.open_ok')
        if chan_num != chan.num:
            raise ProtocolError('channel.open_ok returned '
                                'channel # %d, expected %d' %
                                (chan_num, chan.num))
        self.channels[chan.num] = chan
        return chan

    def forget_channel (self, num):
        del self.channels[num]

    def notify_channels_of_close (self):
        items = self.channels.items()
        for num, ch in items:
            ch.notify_of_close()

class channel:

    """*conn*: the connection object this channel resides on.

    The channel object presents the main interface to the user, exposing most of the 'methods'
      of AMQP, including the 'basic' and 'channel' methods.

    A connection may have multiple channels.
    
    """

    # state diagram for channel objects:
    #
    # channel             = open-channel *use-channel close-channel
    # open-channel        = C:OPEN S:OPEN-OK
    # use-channel         = C:FLOW S:FLOW-OK
    #                     / S:FLOW C:FLOW-OK
    #                     / functional-class
    # close-channel       = C:CLOSE S:CLOSE-OK
    #                     / S:CLOSE C:CLOSE-OK
    
    counter = 1
    ack_discards = True

    def __init__ (self, conn):
        self.conn = conn
        self.num = channel.counter
        self.confirm_mode = False
        self.consumers = {}
        channel.counter += 1
        self._exception_handler = None
        self._next_delivery_tag = 0
        self._publish_cv = coro.condition_variable()
        self._publish_thread = coro.spawn(self._publish_thread_loop)
        self._pending_published = {}

    def set_exception_handler(self, h):
        self._exception_handler = h

    def send_frame (self, ftype, frame):
        self.conn.send_frame (ftype, self.num, frame)

    # Q:, if a method is synchronous, does that mean it is sync w.r.t. this channel only?
    # in other words, might a frame for another channel come in before we get our reply?

    # leaving off all the 'ticket' args since they appear unused/undocumented...

    def exchange_declare (self, exchange=None, type='direct', passive=False,
                          durable=False, auto_delete=False, internal=False, nowait=False, arguments={}):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#exchange.declare"
        frame = spec.exchange.declare (0, exchange, type, passive, durable, auto_delete, internal, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        if not nowait:
            ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'exchange.declare_ok')
            if channel != self.num:
                raise ProtocolError('exchange.declare_ok returned '
                                    'channel # %d, expected %d' %
                                    (channel, self.num))
            return frame

    def queue_declare (self, queue='', passive=False, durable=False,
                       exclusive=False, auto_delete=False, nowait=False, arguments={}):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#queue.declare"
        frame = spec.queue.declare (0, queue, passive, durable, exclusive, auto_delete, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        if not nowait:
            ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'queue.declare_ok')
            if channel != self.num:
                raise ProtocolError('queue.declare_ok returned '
                                    'channel # %d, expected %d' %
                                    (channel, self.num))
            return frame

    def queue_delete (self, queue='', if_unused=False, if_empty=False, nowait=False):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#queue.delete"
        frame = spec.queue.delete (0, queue, if_unused, if_empty, nowait)
        self.send_frame (spec.FRAME_METHOD, frame)
        if not nowait:
            ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'queue.delete_ok')
            if channel != self.num:
                raise ProtocolError('queue.delete_ok returned '
                                    'channel # %d, expected %d' %
                                    (channel, self.num))
            return frame

    def queue_bind (self, queue='', exchange=None, routing_key='', nowait=False, arguments={}):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#queue.bind"
        frame = spec.queue.bind (0, queue, exchange, routing_key, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        if not nowait:
            ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'queue.bind_ok')
            if channel != self.num:
                raise ProtocolError('queue.bind_ok returned '
                                    'channel # %d, expected %d' %
                                    (channel, self.num))
            return frame

    def basic_consume (self, queue='', consumer_tag='', no_local=False,
                       no_ack=False, exclusive=False, arguments={}):
        """Start consuming messages from *queue*.

        Returns a new :class:consumer object which spawns a new thread to monitor incoming messages.

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume
        """
        # we do not allow 'nowait' since that precludes us from establishing a consumer fifo.
        frame = spec.basic.consume (0, queue, consumer_tag, no_local, no_ack, exclusive, False, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.consume_ok')
        if channel != self.num:
            raise ProtocolError('basic.consume_ok returned '
                                'channel # %d, expected %d' %
                                (channel, self.num))
        con0 = consumer (self, frame.consumer_tag)
        self.add_consumer (con0)
        return con0

    def basic_cancel (self, consumer_tag='', nowait=False):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.cancel"
        frame = spec.basic.cancel (consumer_tag, nowait)
        self.send_frame (spec.FRAME_METHOD, frame)
        if not nowait:
            ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.cancel_ok')
        self.forget_consumer (consumer_tag)

    def basic_get (self, queue='', no_ack=False):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get"
        frame = spec.basic.get (0, queue, no_ack)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.get_ok', 'basic.empty')
        if channel != self.num:
            raise ProtocolError('basic.get_ok returned '
                                'channel # %d, expected %d' %
                                (channel, self.num))
        return frame

    def _basic_publish (self, payload, exchange='', routing_key='', mandatory=False, immediate=False, properties=None):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish"
        frame = spec.basic.publish (0, exchange, routing_key, mandatory, immediate)
        self.send_frame (spec.FRAME_METHOD, frame)
        class_id = spec.basic.publish.id[0] # 60
        weight = 0
        size = len (payload)
        if properties:
            flags, pdata = pack_properties (properties)
        else:
            flags = 0
            pdata = ''
        #W ('basic_publish: properties=%r\n' % (unpack_properties (flags, pdata),))
        head = struct.pack ('>hhqH', class_id, weight, size, flags)
        self.send_frame (spec.FRAME_HEADER, head + pdata)
        chunk = self.conn.tune.frame_max
        for i in range (0, size, chunk):
            self.send_frame (spec.FRAME_BODY, payload[i:i+chunk])


    def _publish_thread_loop(self):
        while True:
            try:
                self._publish_cv.wait()
                while self._pending_published:
                    #W('_pending_published: %r\n' % self._pending_published)
                    ftype, channel, frame = self.conn.expect_frame(spec.FRAME_METHOD,
                                                                   'basic.ack')
                    #dump_ob(frame)
                    if frame.multiple:
                        p = [(dtag, s)
                             for dtag, s in sorted(self._pending_published.iteritems())
                             if dtag <= frame.delivery_tag]
                        for dtag, s in p:
                            del self._pending_published[dtag]
                            s.release()

                    else:
                        s = self._pending_published[frame.delivery_tag]
                        del self._pending_published[frame.delivery_tag]
                        s.release()

            except coro.Shutdown:
                break


    def basic_publish (self,
                       payload,
                       exchange='',
                       routing_key='',
                       mandatory=False,
                       immediate=False,
                       properties=None):

        if self.confirm_mode:
            self._next_delivery_tag += 1
            dtag = self._next_delivery_tag
            assert dtag not in self._pending_published
            s = coro.inverted_semaphore(1)
            self._pending_published[dtag] = s
            #W('basic_publish pending >>> %d\n' % dtag);
            self._basic_publish(payload,
                                exchange,
                                routing_key,
                                mandatory,
                                immediate,
                                properties)
            self._publish_cv.wake_one()
            s.block_till_zero()
            #W('basic_publish pending <<< %d\n' % dtag);
        else:
            self._basic_publish(payload,
                                exchange,
                                routing_key,
                                mandatory,
                                immediate,
                                properties)


    def basic_ack (self, delivery_tag=0, multiple=False):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.ack"
        frame = spec.basic.ack (delivery_tag, multiple)
        self.send_frame (spec.FRAME_METHOD, frame)

    def get_ack (self):
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.ack')
        return frame

    def close (self, reply_code=0, reply_text='normal shutdown', class_id=0, method_id=0):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#channel.close"
        frame = spec.channel.close (reply_code, reply_text, class_id, method_id)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'channel.close_ok')
        self.conn.forget_channel (self.num)
        return frame

    def accept_delivery (self, frame, properties, data):
        probe = self.consumers.get (frame.consumer_tag, None)
        if probe is None:
            if self.ack_discards:
                self.basic_ack (frame.delivery_tag)
            if self._exception_handler:
                self._exception_handler(UnknownConsumerTag(frame, properties, data))
            else:
                W('unknown consumer tag: %s\n' % frame.consumer_tag)
        else:
            probe.push ((frame, properties, data))

    def make_default_consumer (self):
        "create a consumer to catch unexpected deliveries"
        con0 = consumer (self.num, '')
        return self.conn.make_default_consumer (con0)

    def add_consumer (self, con):
        self.consumers[con.tag] = con

    def forget_consumer (self, tag):
        if tag in self.consumers:
            del self.consumers[tag]

    def notify_consumers_of_close (self):
        for _, con in self.consumers.iteritems():
            con.close()

    def notify_consumer_of_cancel(self, tag):
        if tag in self.consumers:
            self.consumers[tag].set_cancelled()
            self.forget_consumer(tag)

    def notify_of_close (self):
        self._publish_thread.shutdown()
        self.notify_consumers_of_close()

    # rabbit mq extension
    def confirm_select (self, nowait=False):
        "http://www.rabbitmq.com/amqp-0-9-1-reference.html#confirm.select"
        try:
            if self.conn.server_properties['capabilities']['publisher_confirms'] != True:
                raise ProtocolError ("server capabilities says NO to publisher_confirms")
        except KeyError:
                raise ProtocolError ("server capabilities says NO to publisher_confirms")
        else:
            frame = spec.confirm.select (nowait)
            self.send_frame (spec.FRAME_METHOD, frame)
            if not nowait:
                ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'confirm.select_ok')
            self.confirm_mode = True
            return frame

def pack_properties (props):
    sbp = spec.basic.properties
    r = []
    flags = 0
    items = sbp.bit_map.items()
    items.sort()
    items.reverse()
    for bit, name in items:
        if props.has_key (name):
            _, unpack, pack = sbp.name_map[name]
            r.append (str(pack (props[name])))
            flags |= 1<<bit
    return flags, ''.join (r)

def unpack_properties (flags, data):
    sbp = spec.basic.properties
    r = {}
    pos = 0
    # these must be unpacked from highest bit to lowest [really?]
    items = sbp.bit_map.items()
    items.sort()
    items.reverse()
    for bit, name in items:
        if flags & 1<<bit:
            _, unpack, pack = sbp.name_map[name]
            r[name], pos = unpack (data, pos)
    return r

class AMQP_Consumer_Closed (Exception):
    pass

# Signal of the need to re-create consumer.
class AMQP_Consumer_Cancelled (Exception):
    pass

class consumer:

    """The consumer object manages the consumption of messages triggered by a call to basic.consume.

    *channel*: the channel object delivering the messages.

    *tag*: the unique consumer tag associated with the call to basic.consume
    """

    def __init__ (self, channel, tag):
        self.channel = channel
        self.tag = tag
        self.fifo = coro.fifo()
        self.closed = False

    def close (self):
        "close this consumer channel"
        self.fifo.push (connection_closed)
        self.closed = True
        self.channel.forget_consumer (self)

    def set_cancelled(self):
        self.fifo.push(consumer_cancelled)

    def cancel (self):
        "cancel the basic.consume() call that created this consumer"
        self.closed = True
        self.channel.basic_cancel (self.tag)

    def push (self, value):
        self.fifo.push (value)

    def pop (self, ack=True):
        "pop a new value from this consumer.  Will block if no value is available."
        if self.closed:
            raise AMQP_Consumer_Closed
        else:
            probe = self.fifo.pop()
            if probe == connection_closed:
                self.closed = True
                raise AMQP_Consumer_Closed
            elif probe == consumer_cancelled:
                raise AMQP_Consumer_Cancelled
            else:
                frame, properties, data = probe
                if ack:
                    self.channel.basic_ack (frame.delivery_tag)
                return frame, properties, ''.join (data)
