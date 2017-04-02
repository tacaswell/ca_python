#!/usr/bin/env python
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
EPICS Channel Access Protocol
https://github.com/friedrich-schotte/ca_python

Author: Friedrich Schotte
Date created: 4/26/2009
Date last modified: 4/1/2017
Python Version: 2.7

Based on: 'Channel Access Protocol Specification', version 4.11
http://epics.cosylab.com/cosyjava/JCA-Common/Documentation/CAproto.html
"""
__authors__ = ["Friedrich Schotte"]
__credits__ = []
__license__ = "GPLv3+"
__version__ = "2.0.3" # efficient background thread
__status__ = "Prototype"

import socket
from logging import debug

timeout = 1.0 # s
DEBUG = False # Generate diagnostics messages?
monitor_always = False # run server communication alsways in background

class PV_info:
    """State information for each process variable"""
    def __init__(self):
        from time import time
        t = time()
        self.connection_requested = t # first time a PV was asked for
        self.last_connection_requested = t # last time a PV was asked for
        self.connection_initiated = 0 # time a CA connection for PV was initiated
        self.servers_queried = [] # for address resolution
        self.addr = None # IP address and port number of IOC
        self.channel_ID = None # client-provided reference number for PV
        self.channel_SID = None # server-provided reference number for PV
        self.data_type = None # DOUBLE,INT,STRING,...
        self.data_count = None # 1 if a scalar, >1 if an array
        self.access_bits = None # premissions bit map (bit 0: read, 1: write)
        self.IOID = 0 # last used read/write transaction reference number
        self.subscription_ID = None # locally assiged reference number for server updates
        self.response_time = 0 # timestamp of last reply from server
        self.data = None # value in CA representation (big-edian binary data)
        self.last_updated = 0 # timestamp of data, time update event received
        self.write_data = None # if put in progres, new value in CA representation
        self.write_requested = 0 # time WRITE_NOTIFY command sent
        self.write_sent = 0 # time WRITE_NOTIFY command sent
        self.write_confirmed = 0 # time WRITE_NOTIFY reply received
        self.callbacks = [] # for "camonitor"
        self.writers = [] # for "camonitor"

    def __str__(self):
        s = "PV_info:"
        for attr in dir(self):
            if not "__" in attr:
                s += "\n    %s = %r" % (attr,getattr(self,attr))
        return s
    
PVs = {} # Unique list of active process variables

class connection_info:
    "Per CA server (IOC) state information"
    socket = None
    access_bits = None
    
connections = {} # list of known CA servers (IOCs)

# Used for IOC disocvery broadcasts
UDP_socket = None

# Protocol version 4.11:
major_version = 4
minor_version = 11
# CA server port = 5056 + major version * 2
# CA repeater port = 5056 + major version * 2 + 1
port = 5056 + major_version * 2

# CA Message command codes:
VERSION = 0
EVENT_ADD = 1
EVENT_CANCEL = 2
WRITE = 4
SEARCH = 6
NOT_FOUND = 14
READ_NOTIFY = 15
WRITE_NOTIFY = 19
CLIENT_NAME = 20
HOST_NAME = 21
CREATE_CHAN = 18
ACCESS_RIGHTS = 22

commands = {
    "VERSION": 0,
    "EVENT_ADD": 1,
    "EVENT_CANCEL": 2,
    "WRITE": 4,
    "SEARCH": 6,
    "NOT_FOUND": 14,
    "READ_NOTIFY": 15,
    "WRITE_NOTIFY": 19,
    "CLIENT_NAME": 20,
    "HOST_NAME": 21,
    "CREATE_CHAN": 18,
    "ACCESS_RIGHTS": 22,
}

# CA Message data type codes:
STRING = 0
INT = 1
SHORT = 1
FLOAT = 2
ENUM = 3
CHAR = 4
LONG = 5
DOUBLE = 6
NO_ACCESS = 7

types = {
    "STRING": 0,
    "INT": 1,
    "SHORT": 1,
    "FLOAT": 2,
    "ENUM": 3,
    "CHAR": 4,
    "LONG": 5,
    "DOUBLE": 6,
    "NO_ACCESS": 7,
}

# CA Message monitor mask bits
VALUE = 0x01 # Value change events are reported.
LOG   = 0x02 # Log events are reported (different dead band than VALUE)
ALARM = 0x04 # Alarm events are reported

class PV (object):
    """EPICS Process Variable or
    a collections of process variable with common prefix"""
    def __init__(self,name):
        """name: PREFIX:Record.Field or PREFIX:Record or RPEFIX:"""
        self.name = name

    def get_value(self): return caget(self.name)
    def set_value(self,value): caput(self.name,value)
    value = property(get_value,set_value)

    def get_info(self): return cainfo(self.name,printit=False)
    info = property(get_info)

    def __getattr__(self,name):
        """If this PV object is a record of process variables, retreive
        a process vairable within this record."""
        # Called for attributes other than "value" or "info".
        # E.g. temperature_controller = PV("NIH:TEMP")
        # print temperature_controller.feedback_loop.P.value
        if self.name.endswith(":"): pv = PV(self.name+name)
        else: pv = PV(self.name+"."+name)
        object.__setattr__(self,name,pv)
        return pv

    def __repr__(self): return "PV(%r)" % self.name

    def add_callback(self,callback):
        """Have the routine 'callback' be called every the time value
        of the PV changes.
        callback: function that takes three parameters:
        PV_name, value, char_value"""
        camonitor(self.name,callback=callback)

    def clear_callbacks(self,callback):
        """Undo 'add_callback'."""
        camonitor_clear(self.name)


class Record(object):
    """A collections of process variables with common prefix"""
    __prefix__ = ""
    
    def __init__(self,prefix=""):
        """prefix: common beginning for all process variables within the record.
        e.g. 'NIH:TEMP'"""
        self.__prefix__ = prefix

    def __getattr__(self,name):
        """Called when 'x.name' is evaluated."""
        ## __getattr__ is only invoked if the attribute wasn't found the usual ways.
        ##debug("Record.__getattribute__(%r)" % name)
        # __members__ is used for auto completion, browsing and "dir".
        if name == "__members__": return self.__PV_names__()
        if name == "name" or name == "__name__": return self.__prefix__
        if (name.startswith("__") and name.endswith("__")):
            return object.__getattribute__(self,name)
        full_name = self.__prefix__+"."+name
        value = caget(full_name)
        ##debug("Record: caget(%r) = %r" % (full_name,value))
        # The value being "<record>" indicates that this
        # is a record of PVs, not a PV.
        if isinstance(value,str) and value.startswith("<record"):
            return Record(full_name)
        return value

    def __setattr__(self,name,value):
        """Called when 'x.name = value' is evaluated."""
        ##debug("Record.__setattribute__(%r,%r)" % (name,value))
        if (name.startswith("__") and name.endswith("__")):
            object.__setattr__(self,name,value)
            return
        if name in self.__dict__ or name in self.__class__.__dict__:
            object.__setattr__(self,name,value)
            return
        ##debug("Record: caput(%r,%r)" % (self.__prefix__+"."+name,value))
        caput(self.__prefix__+"."+name,value)

    def __PV_names__(self):
        """A list of PV names in the record."""
        value = caget(self.__prefix__)
        if isinstance(value,str) and value.startswith("<record"):
            return value[9:-1].split(", ")
        return []

    def __repr__(self): return "Record(%r)" % self.__prefix__    
    
def caget(PV_name,timeout=None,wait=None):
    """Retreive the current value of a process variable
    timeout: time in seconds, overrides default timeout of 1.0 s
    wait:
      True: always wait for a timeout to pass before giving up 
      False: return None if the value is not readily availabe.
      Default: Wait for a timeout to pass before giving up only the first time
    """
    from time import time
    if timeout is None: timeout = globals()["timeout"]
    if wait == False: timeout = 0
    
    camonitor_background()
    if not PV_name in PVs: PVs[PV_name] = PV_info(); process_replies(update=True)
    process_replies()
    pv = PVs[PV_name]
    while pv.data is None and time() - pv.connection_requested < timeout:
        process_replies()
    
    v = value(pv.data_type,pv.data_count,pv.data) if pv.data else None

    return v

def caput(PV_name,value,wait=False,timeout=60):
    """Modify the value of a process variable
    If wait=True the call returns only after the server has confirmed
    that is has finished processing the write request or the timeout
    has expired."""
    from time import time
    if timeout is None: timeout = globals()["timeout"]
    
    if not PV_name in PVs: PVs[PV_name] = PV_info()
    
    pv = PVs[PV_name]
    pv.write_data = value
    pv.write_requested = write_requested = time()
    pv.write_confirmed = 0
    write_sent = pv.write_sent
    process_replies(update=True)
    
    while pv.write_sent == write_sent and time() - write_requested < timeout:
        process_replies()
    if wait:
        while not pv.write_confirmed and time() - write_requested < timeout:
            process_replies()

    camonitor_background()

def cawait(PV_name,timeout=None):
    """Wait for the server to send an update event for the PV."""
    if timeout == None: timeout = globals()["timeout"]

    from time import time
    t0 = time()

    if not PV_name in PVs: PVs[PV_name] = PV_info(); process_replies(update=True)
    pv = PVs[PV_name]

    # If the PV has changed in the past 70 ms, let it count as 'changed now'.
    ##debug("pv.last_updated - t0 = %r" % (pv.last_updated - t0))
    if pv.last_updated - t0 > -0.070: return

    process_replies()
    last_updated = pv.last_updated
    while pv.last_updated == last_updated and time()-t0 < timeout:
        process_replies()

def camonitor(PV_name,writer=None,callback=None):
    """Call a function every time a PV changes value.
    writer: function that will be passed a formatted string:
    "<PB_name> <date> <time> <value>"
    E.g. "14IDB:SAMPLEZ.RBV 2013-11-02 18:25:13.555540 4.3290"
    f=file("PV.log","w"); camonitor("14IDB:SAMPLEZ.RBV",f.write)
    callback: function that will be passed three arguments:
    the PV name, its new value, and its new value as string.
    E.g. def callback(PV_name,value,char_value):
    def callback(pvname,value,char_value): print pvname,value,char_value
    """
    if not PV_name in PVs: PVs[PV_name] = PV_info(); process_replies(update=True)

    pv = PVs[PV_name]
    if callback is None and writer is None:
        # By default, if not argument are given, just print update messages.
        import sys
        writer = sys.stdout.write
        
    if callback is not None:
        if not callback in pv.callbacks: pv.callbacks += [callback]
    if writer is not None:
        if not writer in pv.writers: pv.writers += [writer]

    camonitor_background()

def camonitor_clear(PV_name):
    """Undo "camonitor" """
    for PV_name in PVs: 
        pv = PVs[PV_name]
        pv.callbacks = []
        pv.writers = []

camonitor_thread_ID = None

def camonitor_background():
    """Handle IOC communication in background"""
    global camonitor_thread_ID
    if camonitor_thread_ID is None:
        from thread import start_new_thread
        camonitor_thread_ID = start_new_thread (camonitor_thread,())

def camonitor_thread():
    """Perform montitoring to triggger call of registered callback
    routines."""
    while (camonitors() or (monitor_always and PVs)) and process_replies:
       process_replies(1.0)
    global camonitor_thread_ID
    camonitor_thread_ID = None

def camonitors():
    """List of active callback routines"""
    camonitors = []
    for PV_name in PVs.keys(): 
        pv = PVs[PV_name]
        camonitors += pv.callbacks+pv.writers
    return camonitors 

def socketpair(family=socket.AF_INET,type=socket.SOCK_STREAM,proto=0):
    """Create a pair of connected socket objects using TCP/IP protocol.
    This is a replacement for the socket library's 'socketpair' function,
    which is not portalbe to Windows.
    """
    from socket import socket,error
    global listen_socket
    listen_socket = socket(family,type,proto)
    port = 1024
    while port < 16535:
        try: listen_socket.bind(("127.0.0.1",port)); break
        except error: port += 1
    listen_socket.listen(1)
    s1 = socket(family,type,proto)
    s1.connect(("127.0.0.1",port))
    s2,addr = listen_socket.accept()
    return s1,s2

# Used to wake up the CA background (server) thread
request_sockets = socketpair()

def PV_server_discover(PV_name):
    """Send UDP broadcast to find the server hosting a PV
    PV_name: string"""
    from time import time
    if not PV_name in PVs: PVs[PV_name] = PV_info()
    pv = PVs[PV_name]

    global UDP_socket
    if UDP_socket == None:
        from socket import socket,SOCK_DGRAM,SOL_SOCKET,SO_BROADCAST
        UDP_socket = socket(type=SOCK_DGRAM)
        UDP_socket.setsockopt(SOL_SOCKET,SO_BROADCAST,1)

    if pv.addr is None:
        pv.connection_initiated = time()
        reply_flag = 5 # Do not reply
        if pv.channel_ID == None: pv.channel_ID = new_channel_ID()
        request = message(SEARCH,0,reply_flag,minor_version,pv.channel_ID,
            pv.channel_ID,PV_name+"\0")
        for addr in broadcast_addresses():
            sendto(UDP_socket,(addr,port),request)
            pv.servers_queried += [addr]
        # updates PV.addr, then calls "PV_connect"

def PV_connect(PV_name):
    """Translate PV name from string to server-specific channel ID.
    PV_name: string"""
    PV_server_connect(PV_name) # make sure ocnnection to server is established.
    if PV_name in PVs:
        pv = PVs[PV_name]
        if pv.addr and pv.addr in connections and pv.channel_SID is None:
            # Directly connect to the server hosting the PV.
            s = connections[pv.addr].socket
            if pv.channel_ID == None: pv.channel_ID = new_channel_ID()
            send(s,message(CREATE_CHAN,0,0,0,pv.channel_ID,minor_version,
                PV_name+"\0"))
            # updates pv.channel_SID, then calls "PV_subscribe"

def PV_server_connect(PV_name):
    """Establish a TCP connection to the server hosting a PV.
    PV_name: string"""
    from socket import socket,gethostname,error,timeout as socket_timeout
    from getpass import getuser
    if PV_name in PVs:
        pv = PVs[PV_name]
        if pv.addr is not None and pv.addr not in connections:
            addr,cport = pv.addr
            s = socket()
            s.settimeout(timeout)
            try: s.connect((addr,cport))
            except error,msg:
                if DEBUG: debug("%s:%r: %r" % (addr,cport,msg))
                return
            except socket_timeout:
                if DEBUG: debug("%s: timeout" % (addr))
                return
            connections[addr,cport] = connection_info()
            connections[addr,cport].socket = s
            send(s,message(VERSION,0,10,minor_version,0,0)) # 10 = priority
            send(s,message(CLIENT_NAME,0,0,0,0,0,getuser()))
            send(s,message(HOST_NAME,0,0,0,0,0,gethostname()))

def PV_subscribe(PV_name):
    """Ask the server to be notified about when the value of a PV changes.
    PV_name: string"""
    from struct import pack
    if PV_name in PVs:
        pv = PVs[PV_name]
        if pv.subscription_ID is None and pv.channel_SID is not None \
            and pv.addr in connections:
            s = connections[pv.addr].socket
            pv.subscription_ID = new_subscription_ID()
            send(s,message(EVENT_ADD,16,pv.data_type,pv.data_count,pv.channel_SID,
                pv.subscription_ID,pack(">fffHxx",0.0,0.0,0.0,VALUE|LOG|ALARM))) 

from thread import allocate_lock
lock = allocate_lock()

def process_replies(timeout = 0.001,update=False):
    """Interpret any packets comming from the IOC waiting in the system's
    receive queue.
    If timeout > 0 wait for more packets to arrive for the specified number
    of seconds.
    update: make sure pending connection and write processes are
    handled
    """
    if lock.acquire(False):
        import socket
        from select import select,error as select_error
        from struct import unpack

        process_pending_connection_requests()
        process_pending_write_requests()

        while True:
            # Use 'select' to check which sockets have data pending in the input
            # queue.
            sockets = []
            if request_sockets[1]: sockets += [request_sockets[1]]
            if UDP_socket: sockets += [UDP_socket]
            for connection in connections.values(): sockets += [connection.socket]
            try: ready_to_read,x,in_error = select(sockets,[],sockets,timeout)
            except select_error: continue # 'Interrupted system call'

            if request_sockets[1] in ready_to_read:
                # This indicates that a wakeup from "select" had been triggred.
                request_sockets[1].recv(2048)
                if DEBUG: debug("Wake up call")
                global wake_up_in_progress
                wake_up_in_progress = False
                process_pending_connection_requests()
                process_pending_write_requests()

            if UDP_socket in ready_to_read:
                try: messages,addr = UDP_socket.recvfrom(2048)
                except socket.error: messages = ""
                # Several replies may be concantenated. Break them up.
                while len(messages) > 0:
                    # The minimum message size is 16 bytes. If the 'payload size'
                    # field has value > 0, the total size if 16+'payload size'.
                    payload_size, = unpack(">H",messages[2:4])
                    message = messages[0:16+payload_size]
                    messages = messages[16+payload_size:]
                    if DEBUG: debug("Recv upd:%s:%s %s" % (addr[0],addr[1],message_info
                        (message)))
                    process_message(addr,message)
            if UDP_socket in in_error:
                if DEBUG: debug("UDP error")

            for addr in connections.keys():
                connection = connections[addr]
                s = connection.socket
                if s in in_error:
                    if DEBUG: debug("Lost connection to server %s:%s" % addr)
                    reset_PVs(addr)
                    del connections[addr]
                    continue
                if s in ready_to_read:
                    # Several replies may be concatenated. Read one at a time.
                    # The minimum message size is 16 bytes.
                    try: message = s.recv(16)
                    except socket.error:
                        if DEBUG: debug("Recv: lost connection to server %s:%s" % addr)
                        reset_PVs(addr)
                        del connections[addr]
                        continue
                    if len(message) == 0:
                        if DEBUG: debug("Server %s:%s closed connection" % addr)
                        reset_PVs(addr)
                        del connections[addr]
                        break
                    # If the 'payload size' field has value > 0, 'payload size'
                    # more bytes are part of the message.
                    payload_size, = unpack(">H",message[2:4])
                    if payload_size > 0:
                        try: message += s.recv(payload_size)
                        except socket.timeout:
                            if DEBUG: debug("Recv timed out")
                    if DEBUG: debug("Recv %s:%s %s" % (addr[0],addr[1],
                        message_info(message)))
                    process_message(addr,message)
                
            process_pending_connection_requests()
            process_pending_write_requests()
            
            if len(ready_to_read) == 0 and len(in_error) == 0: break # select timed out
        lock.release()
    else: # already in progress
        if update: wake_up()
        from time import sleep
        sleep(timeout)

wake_up_in_progress = False
wake_up_lock = allocate_lock()

def wake_up():
    """Make sure 'process_replies' handles pending connection and write requests"""
    with wake_up_lock:
        global wake_up_in_progress
        if not wake_up_in_progress:
            wake_up_in_progress = True
            request_sockets[0].send(".")

def process_pending_connection_requests():
    """Check list of PVs unconnected PVs and conntect them."""
    from time import time
    for name in PVs.keys():
        pv = PVs[name]
        # Does PV need to be connected?
        ##if time() - pv.last_connection_requested > timeout: continue 
        # Is PV already connected?
        if pv.subscription_ID != None: continue
        # Is connection already in progress?
        if time() - pv.connection_initiated < timeout: continue
        # To Do: retry after timeout
        if DEBUG: debug("Processing connection request for PV %r" % name)
        PV_server_discover(name)

def process_pending_write_requests():
    """Check list of PVs for pending write requests and execute them when possible."""
    from time import time
    for name in PVs.keys():
        pv = PVs[name]
        if pv.write_data == None: continue # nothing to do
        if pv.addr == None: continue # need to postpone
        if pv.channel_SID == None: continue # need to postpone
        if pv.data_type == None: continue # need to postpone

        if DEBUG: debug("Processing write request for PV %r" % name)
        s = connections[pv.addr].socket
        pv.IOID = pv.IOID + 1
        pv.write_confirmed = 0
        
        data = network_data(pv.write_data,pv.data_type)
        count = data_count(pv.write_data,pv.data_type)
        send(s,message(WRITE_NOTIFY,0,pv.data_type,count,
            pv.channel_SID,pv.IOID,data))
        pv.write_sent = time()
        pv.write_data = None

def process_message(addr,message):
    """Interpret a CA protocol datagram"""
    from struct import unpack
    from time import time

    header = message[0:16]
    payload = message[16:]
    if len(header) < 16:
        if DEBUG: debug("process_message: invalid header %r" % header)
        return
    command,payload_size,data_type,data_count,parameter1,parameter2 = \
        unpack(">HHHHII",header)

    if command == SEARCH: # Reply to a SEARCH request.
        port_number = data_type
        channel_SID = parameter1 # 'temporary server ID': 0xFFFFFFFF
        channel_ID = parameter2
        if DEBUG: debug("SEARCH port_number=%r, channel_ID=%r, channel_SID=%r" %
            (port_number,channel_ID,channel_SID))
        for name in PVs.keys():
            if PVs[name].channel_ID == channel_ID:
                # Ignore duplicate replies.
                if PVs[name].addr != None:
                    if DEBUG: debug("Ignoring duplicate SEARCH reply for %r from "
                        "%r:%r" % (name,addr[0],addr[1]))
                    continue
                PVs[name].addr = (addr[0],port_number)
                if DEBUG: debug("PVs[%r].addr = %r" % (name,addr))
                PVs[name].response_time = time()
                PV_connect(name)
    elif command == CREATE_CHAN: # Reply to a 'Create Channel' request.
        channel_ID = parameter1
        channel_SID = parameter2
        if DEBUG: debug("CREATE_CHAN channel_ID=%r, channel_SID=%r" %
            (channel_ID,channel_SID))
        for name in PVs.keys():
            if PVs[name].channel_ID == channel_ID:
                if PVs[name].channel_SID != None:
                    if DEBUG: debug("Ignoring duplicate CREATE_CHAN reply for %r from "
                        "%r:%r" % (name,addr[0],addr[1]))
                    continue
                PVs[name].addr = addr
                if DEBUG: debug("PVs[%r].addr = %r" % (name,addr))
                PVs[name].channel_SID = channel_SID
                if DEBUG: debug("PVs[%r].channel_SID = %r" % (name,channel_SID))
                PVs[name].data_type = data_type
                if DEBUG: debug("PVs[%r].data_type = %r" % (name,data_type))
                PVs[name].data_count = data_count
                if DEBUG: debug("PVs[%r].data_count = %r" % (name,data_count))
                PVs[name].response_time = time()
                PV_subscribe(name)
    elif command == ACCESS_RIGHTS:
        # Reply to the CLIENT_NAME/HOST_NAME greeting.
        channel_ID = parameter1
        access_bits = parameter2
        if DEBUG: debug("ACCESS_RIGHTS channel_ID %r, %s" % (channel_ID,access_bits))
        for name in PVs.keys():
            if PVs[name].channel_ID == channel_ID:
                PVs[name].access_bits = access_bits
                if DEBUG: debug("PVs[%r].access_bits = %r" % (name,access_bits))
                PVs[name].response_time = time()
    elif command == READ_NOTIFY:
        # Reply to a synchronous read request (never used).
        # Channel Access Protocol Specification, section 6.15.2, says: 
        # parameter 1: channel_SID, parameter 2: IOID
        # However, I always get: parameter 1 = 1, parameter 2 = 1.
        channel_SID = parameter1
        IOID = parameter2
        val = value(data_type,data_count,payload)
        if DEBUG: debug("READ_NOTIFY channel_SID=%r, IOID=%r, value=%r" %
            (channel_SID,IOID,val))
        for name in PVs.keys():
            if PVs[name].channel_SID == channel_SID:
                if DEBUG: debug("PVs[%r].data = %r" % (name,payload))
                PVs[name].data = payload
                PVs[name].data_type = data_type
                PVs[name].data_count = data_count
                PVs[name].response_time = time()
    elif command == EVENT_ADD: # Asynchronous notification that PV changed.
        status_code = parameter1
        subscription_ID = parameter2
        val = value(data_type,data_count,payload)
        if DEBUG: debug("EVENT_ADD status_code=%r, subscription_ID=%r, value=%r" % (status_code,
            subscription_ID,val))
        for name in PVs.keys():
            if PVs[name].subscription_ID == subscription_ID and \
                PVs[name].addr == addr:
                update = True if PVs[name].data is not None else False
                PVs[name].data_type = data_type
                PVs[name].data_count = data_count
                if DEBUG: debug("PVs[%r].data = %r" % (name,payload))
                t = time()
                if PVs[name].data != None: PVs[name].last_updated = t
                PVs[name].data = payload
                PVs[name].response_time = t
                # Call any callback routines for this PV.
                pv = PVs[name]
                if len(pv.callbacks) > 0 or len(pv.writers) > 0:
                    if DEBUG: debug("%s has callbacks" % name)
                    new_value = value(pv.data_type,pv.data_count,pv.data)
                    char_value = "%r" % new_value
                    if DEBUG: debug("%s = %s" % (name,char_value))
                    # Run the callback function in a separate thread to avoid
                    # deadlock in case the function calls "caput" or "caget".
                    from thread import start_new_thread
                    for function in pv.callbacks:
                        if DEBUG: debug("%s: calling %s" % (name,object_name(function)))
                        start_new_thread(function,(name,new_value,char_value))
                    from datetime import datetime
                    message = "%s %s %s\n" % (name,datetime.fromtimestamp(t),
                        char_value)
                    for function in pv.writers:
                        if DEBUG: debug("%s: calling %s" % (name,object_name(function)))
                        function(message)
    elif command == EVENT_CANCEL: # Asynchronous notification that PV not longer exists.
        channel_SID = parameter1
        subscription_ID = parameter2
        if DEBUG: debug("EVENT_CANCEL channel_SID=%r, subscription_ID=%r" %
            (channel_SID,subscription_ID))
        for name in PVs.keys():
            if PVs[name].subscription_ID == subscription_ID and \
                PVs[name].addr == addr:
                del PVs[name]
    elif command == WRITE_NOTIFY: # Confirmation of a sucessful write.
        status = parameter1
        IOID = parameter2
        if DEBUG: debug("WRITE_NOTIFY status_code=%r, IOID=%r" % (status,IOID))
        for name in PVs.keys():
            if PVs[name].IOID == IOID and \
                PVs[name].addr == addr:
                t = time()
                if DEBUG: debug("PVs[%r].write_confirmed = %r" % (name,t))
                PVs[name].write_confirmed = t
                PVs[name].response_time = t
    elif command == NOT_FOUND:
        channel_ID = parameter1
        PV_name = "unknown"
        for name in PVs.keys():
            if PVs[name].channel_ID == channel_ID: PV_name = name
        if DEBUG: debug("NOT_FOUND: %r" % PV_name)
    else:
        if DEBUG: debug("%r: unknown command code" % command)

def object_name(object):
    """Convert Python object to string"""
    if hasattr(object,"__name__"): return object.__name__
    else: return repr(object)


def new_channel_ID():
    """Return a unique integer to be used as 'Channel ID' for a PV.
    A Channel ID is a client-provided integer number, which the CA server (IOC)
    includes as reference when replying to 'create channel' requests."""
    IDs = [pv.channel_ID for pv in PVs.values()]
    ID = 1
    while ID in IDs: ID += 1
    return ID
 
def new_subscription_ID():
    """Return a unique integer to be used as 'Subscription ID' for a PV.
    A subscription ID is a client-provided integer number, which  the CA server
    (IOC) includes as reference number when sending update events."""
    IDs = [pv.subscription_ID for pv in PVs.values()]
    ID = 1
    while ID in IDs: ID += 1
    return ID

def reset_PVs(addr):
    """If the connection to the server 'addr' is lost, clear outdate PV state
    info."""
    for name in PVs.keys(): PVs[name] = PV_info()

def message(command=0,payload_size=0,data_type=0,data_count=0,
        parameter1=0,parameter2=0,payload=""):
    """Assemble a Channel Access message datagram for network transmission"""
    assert data_type is not None
    assert data_count is not None
    assert parameter1 is not None
    assert parameter2 is not None
    
    from math import ceil
    from struct import pack

    if payload_size == 0 and len(payload) > 0:
        # Pad to multiple of 8.
        payload_size = int(ceil(len(payload)/8.)*8)
        
    while len(payload) < payload_size: payload += "\0"

    # 16-byte header consisting of four 16-bit integers
    # and two 32-bit integers in big-edian byte order.
    header = pack(">HHHHII",command,payload_size,data_type,data_count,
        parameter1,parameter2)    
    message = header + payload
    return message

def message_info(message):
    """Text representation of the CA message datagram"""
    from struct import unpack
    header = message[0:16]
    payload = message[16:]
    if len(header) < 16: return "invalid message %r" % header
    command,payload_size,data_type,data_count,parameter1,parameter2 = \
        unpack(">HHHHII",header)
    s = str(command)
    if command in commands.values():
        s += "("+commands.keys()[commands.values().index(command)]+")"
    s += ","+str(payload_size)
    s += ","+str(data_type)
    if data_type in types.values():
        s += "("+types.keys()[types.values().index(data_type)]+")"
    s += ","+str(data_count)
    s += ", %r, %r" % (parameter1,parameter2)
    if payload:
        s += ", %r" % payload
        if command in (EVENT_ADD,WRITE,READ_NOTIFY,WRITE_NOTIFY):
            s += "(%r)" % (value(data_type,data_count,payload),)
    return s     

def send(socket,message):
    """Transmit a Channel Access message to an IOC via TCP"""
    from socket import error as socket_error
    addr,port = socket.getpeername()
    if DEBUG: debug("Send %s:%s %s" % (addr,port,message_info(message)))
    try: socket.sendall(message)
    except socket_error,error:
        if DEBUG: debug("Send failed: %r" % error)

def sendto(socket,addr,message):
    """Transmit a Channel Access message to an IOC via UDP"""
    from socket import error as socket_error
    if DEBUG: debug("Send UDP %s:%s %s" % (addr[0],addr[1],message_info(message)))
    try: socket.sendto(message,addr)
    except socket_error,error:
        if DEBUG: debug("Sendto %r failed: %r" % (addr,error))

def value(data_type,data_count,payload):
    """Convert received network binary data to a Python data type"""
    if payload == None: return None
    from struct import unpack
    if data_type == STRING:
        # Null-terminated string.
        # data_count is the number of null-terminated strings (characters)
        value = payload.split("\0")[0:data_count]
        if len(value) == 1: value = value[0]
    elif data_type == SHORT:
        payload = payload.ljust(2*data_count,"\0")
        value = list(unpack(">%dh"%data_count,payload[0:2*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type == FLOAT:
        payload = payload.ljust(4*data_count,"\0")
        value = list(unpack(">%df"%data_count,payload[0:4*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type == ENUM:
        payload = payload.ljust(2*data_count,"\0")
        value = list(unpack(">%dh"%data_count,payload[0:2*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type == CHAR:
        payload = payload.ljust(data_count,"\0")
        value = list(unpack("%db"%data_count,payload[0:data_count]))
        if len(value) == 1: value = value[0]
    elif data_type == LONG:
        payload = payload.ljust(4*data_count,"\0")
        value = list(unpack(">%di"%data_count,payload[0:4*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type == DOUBLE:
        payload = payload.ljust(8*data_count,"\0")
        value = list(unpack(">%dd"%data_count,payload[0:8*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type == None: value = payload
    else:
        if DEBUG: debug("unsupported data type %r" % data_type)
        value = payload
    return value

def data_count(value,data_type):
    """If value is an array return the number of elements, else return 1.
    In CA, a string counts as a single element."""
    # If the data type is STRING the data count is the number of NULL-
    # terminated strings, if the data type if CHAR the data count is the
    # number is characters in the string, including any NULL characters
    # inside and at the end.
    if issubclass(type(value),basestring): return 1
    if hasattr(value,"__len__"): return len(value)
    return 1

def network_data(value,data_type):
    "Convert a Python data type to binary data for network transmission"
    from struct import pack
    from numpy import int8,int16,int32,float32,float64
    
    payload = ""
    if data_type == STRING:
        payload = str(value)
        # EPICS requires that strings are NULL-terminated.
        if not payload.endswith("\0"): payload += "\0"
    elif data_type == SHORT:
        if hasattr(value,"__len__"):
            for v in value: payload += pack(">h",to(v,int16))
        else: payload = pack(">h",to(value,int16))
    elif data_type == FLOAT:
        if hasattr(value,"__len__"):
            for v in value: payload += pack(">f",to(v,float32))
        else: payload = pack(">f",to(value,float32))
    elif data_type == ENUM:
        if hasattr(value,"__len__"):
            for v in value: payload += pack(">h",to(v,int16))
        else: payload = pack(">h",to(value,int16))
    elif data_type == CHAR:
        if hasattr(value,"__len__"):
            for v in value: payload += pack(">b",to(v,int8))
        else: payload = pack(">b",to(value,int8))
    elif data_type == LONG:
        if hasattr(value,"__len__"):
            for v in value: payload += pack(">i",to(v,int32))
        else: payload = pack(">i",to(value,int32))
    elif data_type == DOUBLE:
        if hasattr(value,"__len__"):
            for v in value: payload += pack(">d",to(v,float64))
        else: payload = pack(">d",to(value,float64))
    else:
        if DEBUG: debug("network_data: unsupported data type %r" % data_type)
        payload = str(value)

    return payload

def to(value,dtype):
    """Force conversion to int data type. If failed return 0:
    dtype: int8, int32, int64"""
    isfloat = "float" in str(dtype)
    try: return dtype(value)
    except: return 0 if not isfloat else 0.0

def broadcast_addresses():
    """A list if IP adresses to use for name resolution broadcasts"""
    from os import environ
    if "EPICS_CA_AUTO_ADDR_LIST" in environ and \
       environ["EPICS_CA_AUTO_ADDR_LIST"] == "NO": return []

    # You can override the automatic selection of broadcast
    # addresses by setting the variable 'broadcast_address'.
    if "broadcast_address" in globals() and broadcast_address:
        return [broadcast_address]

    from socket import inet_aton,inet_ntoa,error
    from struct import pack,unpack
    addresses = []
    for address in network_interfaces():
        try: num_address = inet_aton(address)
        except: continue # E.g. IPv6 address
        if not address in addresses: addresses += [address]
        ipaddr, = unpack(">I",num_address)
        ipaddr |= 0x000000FF
        address = inet_ntoa(pack(">I",ipaddr))
        if not address in addresses: addresses += [address]
    # This is a hack (was in an hurry).
    addresses += ["172.21.46.255"] # LCLS XPP ICS subnet
    return addresses

def network_interfaces():
    """A list of IP adresses of the local network interfaces,
    as strings in numerical dot notation"""
    from socket import getaddrinfo,gethostname
    addresses = [local_ip_address()]
    for addrinfo in getaddrinfo(None,0)+getaddrinfo(gethostname(),0):
        address = addrinfo[4][0]
        if not address in addresses: addresses += [address]
    return addresses

def local_ip_address():
    """IP address of the local network interface as string in dot notation"""
    # Unfortunately, Python has no platform-indepdent function to find
    # the IP address of the local machine.
    # As a work-around let us pretend we want to send a UDP datagram to a
    # non existing external IP address.
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try: s.connect(("129.166.233.186",1024))
    except socket.error: return "127.0.0.1" # Network is unreachable
    # This code does not geneate any network traffic, because UDP is not
    # a connection-orientation protocol.
    # Now, Python can tell us what would be thet "source address" of the packets
    # if we would sent a packet (but we won't actally sent a packet).
    address,port = s.getsockname()
    return address

def cainfo(PV_name="all",printit=True,update=True):
    "Print status info string"
    from socket import gethostbyaddr,herror
    from datetime import datetime
    from time import time

    if PV_name == "all":
        for name in PVs: cainfo(name,update=False)
    else:
        if update: caget(PV_name)

        s = PV_name+"\n"
        if PV_name in PVs: pv = PVs[PV_name]
        else: pv = PV_info()

        fmt = "    %-14s %.60s\n"

        if pv.channel_SID: val = "connected"
        else: val = "not connected"
        if pv.subscription_ID: val += ", receiving notifications"
        if pv.connection_requested and not pv.subscription_ID:
            val += ", pending for %.0f s" % (time() - pv.connection_requested)
        s += fmt % ("State:",val)
        
        if pv.addr:
            val = pv.addr[0]
            # Try to translate numeric IP address to host name.
            try: val = gethostbyaddr(val)[0]
            except herror: pass
            val += ":%s" % pv.addr[1]
        else: val = "N/A"
        s += fmt % ("Host:",val)

        if pv.access_bits != None:
            val = ""
            if pv.access_bits & 1: val += "read/"
            if pv.access_bits & 2: val += "write/"
            val = val.strip("/")
            if val == "": val = "none"
        else: val = "N/A"
        s += fmt % ("Access:",val)
        
        if pv.data_type != None:
            val = repr(pv.data_type)
            for t in types:
                if types[t] == pv.data_type: val = t
        else: val = "N/A"
        s += fmt % ("Data type:",val)

        if pv.data_count != None: val = str(pv.data_count)
        else: val = "N/A"
        s += fmt % ("Element count:",val)

        if pv.data != None: val = repr(value(pv.data_type,pv.data_count,pv.data))
        else: val = "N/A"
        s += fmt % ("Value:",val)

        if pv.last_updated != 0:
            t = pv.last_updated
            val = "%s (%s)" % (t,datetime.fromtimestamp(t))
            s += fmt % ("Last changed:",val)

        if pv.response_time != 0:
            t = pv.response_time
            val = "%s (%s)" % (t,datetime.fromtimestamp(t))
            s += fmt % ("Time stamp:",val)

        if printit: print(s)
        else: return s

def PV_status():
    """print status info"""
    for name in PVs:
        s = "%s: " % name
        pv = PVs[name]
        for attr in dir(pv):
            if not "__" in attr: s += "%s = %r, " % (attr,getattr(pv,attr))
        s = s.strip(", ")
        print(s)

def logfile():
    """File name for diagnostics messages."""
    from tempfile import gettempdir
    return gettempdir()+"/CA.log"

if __name__ == "__main__": # for testing
    from pdb import pm
    from time import time
    import logging
    logging.basicConfig(level=logging.DEBUG,format="%(asctime)s: %(message)s",
        filename=logfile())
    ##DEBUG = True
    PV_name = "14IDA:DAC1_4.VAL"
    print('DEBUG = %r' % DEBUG)
    print('monitor_always = %r' % monitor_always)
    print('caget(%r)' % PV_name)
    ##print('t=time(); x=caget(%r); time()-t,x' % PV_name)
    print('caput(%r,4.8372)' % PV_name)
    ##print('t=time(); caput(%r,4.8372); time()-t' % PV_name)
    print('camonitor(%r)' % PV_name)
    print('camonitor_clear(%r)' % PV_name)
    print('cawait(%r,20)' % PV_name)
