"""
Support mudule for EPICS Input/Output Controllers (IOCs)
Implements the server side of the Channel Access (CA) protocol, version 4.11.

Author: Friedrich Schotte
Date created: 2009-10-31
Date last modified: 2019-06-02

based on: 'Channel Access Protocol Specification', version 4.11
http://epics.cosylab.com/cosyjava/JCA-Common/Documentation/CAproto.html

Object-Oriented Interface 1

PV class object: recommended for application that export a single
process variable.

def getT(): return float(serial_port.query("SET:TEMP?"))
def setT(T): serial_port.write("SET:TEMP %s" % T)
pv = PV("14IDB:TemperatureController.T",get=getT,set=setT)

Object-Oriented Interface 2

Use "register" object to export properties of a Python class object as
EPICS PVs.

class Temperature(object):
    def get_value(self): return float(serial_port.query("SET:TEMP?"))
    def set_value(self,value): serial_port.write("SET:TEMP %s" % value)
    value = property(get_value,set_value)
T = Temperature()

register_object(T,prefix="14IDB:TemperatureController.")

Procedural Interface

casput ("14IDB:MyInstrument.VAL",1.234)
Creates a process variable named "14IDB:MyInstrument.VAL"
Subsequent calls to with difference value cause update events to besent to
connected clients.

casget ("14IDB:MyInstrument.VAL")
Reads back the current value of the "14IDB:MyInstrument.VAL", which may have
been modified be a client since the last casput.

casmonitor("14IDB:MyInstrument.VAL",callback=procedure)
The function "procedure" is called when a client modifies a the process
variable with three arguments:
- the name of the process variable
- the new value
- the new value as string
"""
from logging import debug,info,warn,error

__version__ = "1.5.1" # EPICS 3.16 compatibility: Read request have data_count=0

DEBUG = False # Generate debug messages?

registered_objects = []

def register_object(object,name=""):
    """Export object as PV under the given name"""
    global registered_objects
    start_server()
    unregister_object(name=name)
    registered_objects += [(object,name)]

casregister = CAServer_register = register_object # alias names

def unregister_object(object=None,name=None):
    """Undo 'register_object'"""
    global registered_objects
    if name is None:
        for (o,n) in registered_objects:
            if o is object: name = n
    if name is not None:    
        for PV_name in PVs.keys():
            if PV_name.startswith(name): delete_PV(PV_name)
    if object is not None:
        registered_objects = [(o,n) for (o,n) in registered_objects if not o is object]
    if name is not None:
        registered_objects = [(o,n) for (o,n) in registered_objects if not n == name]

registered_properties = {}

def register_property(object,property_name,PV_name):
    """Export object as PV under the given name"""
    global registered_properties
    start_server()
    unregister_property(PV_name=PV_name)
    registered_properties[PV_name] = (object,property_name)

def unregister_property(object=None,property_name=None,PV_name=None):
    """Undo 'register_object'"""
    global registered_properties
    if object is not None and property_name is not None and PV_name is not None:
        if PV_name in registered_properties:
            if registered_properties[PV_name] == (object,property_name):
                del registered_properties[PV_name]
    elif PV_name is not None:
        if PV_name in registered_properties: del registered_properties[PV_name]
    elif object is not None and property_name is not None:
        for key in registered_properties.keys():
            if registered_properties[key] == (object,property_name):
                del registered_properties[key]

def casdel(name):
    """Undo 'casput'"""
    for PV_name in PVs.keys():
        if PV_name.startswith(name): delete_PV(PV_name)

class PV(object):
    """Process Variable.
    Override the 'set_value' and 'get_value' methods in subclasses"""
    instances = []
    
    def __init__(self,name):
        """name: common prefix for all process variables, e.g.
        '14IDB:MyInstrument.'"""
        self.__name__ = name
        self.instances += [self]
        start_server()

    def get_value(self): return getattr(self,"__value__",None)
    def set_value(self,value): self.__value__ = value
    value = property(get_value,set_value)

    def get_connected(self): return PV_connected(self.__name__)
    connected = property(get_connected)

    def __setattr__(self,attr,value):
        """Called when x.attr = value is executed."""
        ##if DEBUG: debug("PV.__setattr__(%r,%r)" % (attr,value))
        object.__setattr__(self,attr,value)
        if attr == "value":
            notify_subscribers_if_changed(self.__name__,value)

    def __getattr__(self,attr):
        """Called when x.attr is evaluated."""
        ##if DEBUG: debug("PV.__getattr__(%r)" % attr)
        value = object.__getattr__(self,attr)
        if attr == "value":
            notify_subscribers_if_changed(self.__name__,value)
        return value


def casput(PV_name,value,update=True):
    """Create a new process variable with thte given name,
    or update an existing one.
    update: send an updaate to the clients even if the value has not changed.
    """
    if DEBUG: debug("casput(%r,%r)" % (PV_name,value))
    start_server()
    if not PV_name in PVs.keys(): PVs[PV_name] = PV_info()
    PV = PVs[PV_name]
    if PV_value(PV_name) != value or update: PV_set_value(PV_name,value)
CAServer_put = casput

def casget(PV_name):
    """Current value of a process variable"""
    start_server()
    return PV_value(PV_name)
CAServer_get = casget

def casmonitor(PV_name,writer=None,callback=None):
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
    start_server()
    if not PV_name in PVs.keys(): PVs[PV_name] = PV_info()
    PV = PVs[PV_name]
    if callback is None and writer is None:
        # By default, if not argument are given, just print update messages.
        import sys
        writer = sys.stdout.write
        
    if callback is not None:
        if not callback in PV.callbacks: PV.callbacks += [callback]
    if writer is not None:
        if not writer in PV.writers: PV.writers += [writer]
CAServer_monitor = casmonitor

class PV_info:
    """State information for each process variable"""
    def __init__(self):
        self.value = None # current value in Python format
        self.subscribers = {} # subscriber_info objects, indexed by (address,port)
        self.channel_SID = None # server-assigned session identity number
        self.last_updated = 0 # timestamp of value
        self.callbacks = []  # for "casmonitor"
        self.writers = []  # for "casmonitor"

PVs = {} # Active process variables, indexed by name

class subscriber_info:
    """State information for each active connection to a process variable"""
    def __init__(self,subscription_ID=None,data_type=None,data_count=None):
        """subscription_ID: client-assigned number for EVENT_ADD updates""" 
        self.subscription_ID = subscription_ID        
        self.data_type = data_type # DOUBLE,LONG,STRING,...
        self.data_count = data_count # 1 if a scalar, >1 if an array

cache = {} # values of PVs
cache_timeout = 1.0
class cache_entry():
    def __init__(self,value,time):
        self.value = value
        self.time = time
    def __repr__(self): return "(%r,%s)" % (self.value,date_string(self.time))

def PV_exists(PV_name):
    """Has a process variable with the given name been defined?"""
    ##return PV_name in PVs.keys() or PV_value(PV_name) is not None
    return PV_value(PV_name) is not None

def PV_value(PV_name,cached=True):
    """The value of a process variable as Python data type.
    If the process variable has not been define return None."""
    from time import time
    if cached and PV_name in cache:
        if time() <= cache[PV_name].time + cache_timeout:
            ##if DEBUG: debug("%s in cache" % PV_name)
            return cache[PV_name].value
        ##if DEBUG: debug("%s expired from cache" % PV_name)
    value = PV_current_value(PV_name)
    cache[PV_name] = cache_entry(value,time())
    return value

def PV_current_value(PV_name):
    """The current value of a process variable as Python data type.
    If the process variable has not been define return None."""
    from time import time
    t0 = time()
    value = PV_value_or_object(PV_name)
    # Is value is an object, use the PV name instead.
    if isobject(value): value = "<record: %s>" % ", ".join(members(value))
    ##if DEBUG: debug("%s: current value %r (%.3f s)" % (PV_name,value,time()-t0))
    return value

def PV_value_or_object(PV_name):
    """The current value of a process variable as Python data type.
    If the process variable has not been define return None."""
    for object,name in registered_objects:
        if PV_name.startswith(name):
            attribute = PV_name[len(name):]
            ##try: return eval("object"+attribute+".value")
            ##except: pass
            try: return eval("object"+attribute)
            except: pass
    if PV_name in registered_properties:
        object,property_name = registered_properties[PV_name]
        try: return getattr(object,property_name)
        except Exception,msg:
            error("%s: %r.%s: %s" % (PV_name,object,property_name,msg))
    record = object_instance(PV_name)
    if record: return getattr(record,object_property(PV_name))
    if PV_name in PVs.keys(): return PVs[PV_name].value
    return None

def isobject(x):
    """Is x a class object?"""
    if hasattr(x,"__len__"): return False # array
    if hasattr(x,"__dict__"): return True
    return False

def members(x):
    """x: class object
    Return value: list of  strings"""
    function = type(lambda: 0)
    members = []
    for name in dir(x):
        if name.startswith("__") and name.endswith("__"): continue
        ##if type(getattr(x,name)) == function: continue
        members += [name]
    return members

def PV_set_value(PV_name,value):
    """Modify the local value of a process variable
    (The value retreived by 'PV_value')"""
    if DEBUG: debug("set %s = %r" % (PV_name,value))
    # Convert value to the correct data type.
    # The value of a PV might be passed as string when the PV type is acually
    # DOUBLE.
    value = convert(PV_name,value)
    if DEBUG: debug("converted %r to %r" % (PV_name,value))
    for object,name in registered_objects:
        if PV_name.startswith(name+"."):
            attribute = PV_name[len(name+"."):]
            PV_object_name = "object."+attribute
            try: PV_object = eval(PV_object_name)
            except Exception,exception:
                if DEBUG: debug("%s: %s" % (PV_object_name,exception))
                continue
            if hasattr(PV_object,"value"): 
                code = "object.%s.value = %r" % (attribute,value)
                from numpy import nan,inf # needed for exec
                try:
                    exec(code)
                    if DEBUG: debug("Tried %s: OK" % code.replace("object",name))
                    continue
                except Exception,exception:
                    if DEBUG: debug("Tried %s: failed: %s" % (code,exception))
            else:
                if not ("." in attribute or "[" in attribute):
                    try:
                        setattr(object,attribute,value)
                        if DEBUG: debug("Tried setattr(%s,%s,%r): OK" %
                            (name,attribute,value))
                    except Exception,exception:
                        if DEBUG: debug("Tried setattr(%s,%s,%r): %s" %
                            (name,attribute,value,exception))
                else:
                    code = "object.%s = %r" % (attribute,value)
                    from numpy import nan,inf # needed for exec
                    try:
                        exec(code)
                        if DEBUG: debug("Tried %s: OK" % code.replace("object",name))
                        continue
                    except Exception,exception:
                        if DEBUG: debug("Tried %s: failed: %s" % (code,exception))
    if PV_name in registered_properties:
        object,property_name = registered_properties[PV_name]
        try: setattr(object,property_name,value)
        except Exception,msg:
            error("%s: %r.%s = %r: %s",(PV_name,object,property_name,value,msg))
    record = object_instance(PV_name)
    if record:
        setattr(record,object_property(PV_name),value)
    if not PV_name in PVs.keys(): PVs[PV_name] = PV_info()
    PV = PVs[PV_name]
    PV.value = value
    from time import time
    PV.last_updated = time()
    cache[PV_name] = cache_entry(value,PV.last_updated)
    notify_subscribers(PV_name)

def call_callbacks(PV_name):
    """Call any callback routines for this PV."""
    if not PV_name in PVs.keys(): return
    PV = PVs[PV_name]
    if len(PV.callbacks) > 0:
        char_value = "%r" % PV.value
        # Run the callback function in a separate thread to avoid
        # deadlock in case the function calls "caput" or "caget".
        from threading import Thread
        for function in PV.callbacks:
            if DEBUG: debug("%s: calling '%s'" % (PV_name,object_name(function)))
            task = Thread(target=function,args=(PV_name,PV.value,char_value),
                name="callback %s" % function)
            task.daemon = True
            task.start()
    if len(PV.writers) > 0:
        from datetime import datetime
        message = "%s %s %r\n" % (PV_name,
            datetime.fromtimestamp(PV.last_updated),PV.value)
        for function in PV.writers:
            if DEBUG: debug("%s: calling '%s'" % (PV_name,object_name(function)))
            function(message)
    notify_subscribers(PV_name) 

def PV_subscribers(PV_name):
    """IP address/ports of clients are connected to a process variable.
    Return value: list of (string,integer) tuples"""
    if not PV_name in PVs.keys(): return []
    PV = PVs[PV_name]
    return PV.subscribers.keys()

def PV_nsubscribers(PV_name):
    """How many clients are connected to a process variable?"""
    return len(PV_subscribers(PV_name))

def PV_connected(PV_name):
    """Is there a client currenlty subscribing to this process variable?"""
    return len(PV_subscribers(PV_name)) > 0

def notify_subscribers_if_changed(PV_name,new_value):
    """Send update events to all client monitoring the given process variable
    if the new value is different than the current value"""
    ##if DEBUG: debug("notify_subscribers_if_changed(%r,%r)" % (PV_name,new_value))
    if not PV_name in PVs.keys(): return
    PV = PVs[PV_name]
    if new_value is None:
        if DEBUG: info("CA: notify_subscribers_if_changed: %s = %s" % (PV_name,new_value))
        ##delete_PV(PV_name)
        return
    ##if DEBUG: debug("PV %r, old value %r, new value %r: equal? %r" %
    ##      (PV_name,PV.value,new_value,nan_equal(new_value,PV.value)))
    if nan_equal(new_value,PV.value): return
    new_value = PV_data(new_value)
    ##if DEBUG: debug("Updating PV %r, old value %r, new value %r" %
    ##      (PV_name,PV.value,new_value))
    PV.value = new_value
    from time import time
    PV.last_updated = time()
    for address in PV.subscribers.keys():
        # Notify connected clients that process variable has changed.
        subscriber = PV.subscribers[address]
        # Make sure client is interested in receiving update notifications.
        if subscriber.subscription_ID == None: continue
        # Make sure client is still connected.
        if not address in connections: continue
        connection = connections[address]
        status_code = 1 # Normal successful completion
        data = network_data(new_value,subscriber.data_type)
        subscriber.data_count = CA_count(new_value)
        send(connection,message("EVENT_ADD",0,subscriber.data_type,
            subscriber.data_count,status_code,subscriber.subscription_ID,
            data))

def notify_subscribers(PV_name):
    """Send update events to all client monitoring the given process variable"""
    if not PV_name in PVs.keys(): return
    PV = PVs[PV_name]
    value = PV_value(PV_name)
    if value is None:
        if DEBUG: info("CA: notify_subscribers: %s=%r" % (PV_name,value))
        ##delete_PV(PV_name)
        return
    for address in PV.subscribers.keys():
        if not address in PV.subscribers: continue
        # Notify connected clients that process variable has changed.
        subscriber = PV.subscribers[address]
        # Make sure client is interested in receiving update notifications.
        if subscriber.subscription_ID == None: continue
        # Make sure client is still connected.
        if not address in connections: continue
        connection = connections[address]
        status_code = 1 # Normal successful completion
        data = network_data(value,subscriber.data_type)
        subscriber.data_count = CA_count(value)
        send(connection,message("EVENT_ADD",0,subscriber.data_type,
            subscriber.data_count,status_code,subscriber.subscription_ID,
            data))

def delete_PV(PV_name):
    """Call if PV no longer exists"""
    disconnect_PV(PV_name)
    if DEBUG: info("CAServer: deleting PV %r" % PV_name)
    del PVs[PV_name]    

def disconnect_PV(PV_name):
    """Notify subscribers that PV no longer exists."""
    ##if DEBUG: debug("notify_subscribers_if_changed(%r,%r)" % (PV_name,new_value))
    if not PV_name in PVs.keys(): return
    PV = PVs[PV_name]
    for address in PV.subscribers.keys():
        # Notify connected clients that process variable has changed.
        subscriber = PV.subscribers[address]
        # Make sure client is interested in receiving update notifications.
        if subscriber.subscription_ID == None: continue
        # Make sure client is still connected.
        if not address in connections: continue
        connection = connections[address]
        status_code = 1 # Normal successful completion
        send(connection,message("EVENT_CANCEL",0,subscriber.data_type,
            subscriber.data_count,PV.channel_SID,subscriber.subscription_ID))
    PV.subscribers = []

def PV_data(value):
    """If value is an array or a list, the current content of the array,
    rather a reference to the array"""
    if isinstance(value,basestring): return value
    try: value = list(value[:])
    except: pass
    return value

def PV_names():
    """List of all currently defined process variables."""
    PV_names = []
    for pv in PV.instances: PV_names += [pv.__name__]
    return PV_names

def connected_PVs():
    """All currently active process variables, with clients connected to them.
    Return value: ist of strings"""
    return [PV_name for PV_name in PVs.keys() if PV_connected(PV_name)]

def update_all_PVs():
    """Send update events to all connected clients for the PVs which have
    changed since the last update."""
    for PV_name in connected_PVs():
        notify_subscribers_if_changed(PV_name,PV_value(PV_name,cached=False))

update_interval = 1.0 # Waiting time between PV updates in seconds.

def update_all_PVs_loop():
    """Keep polling actively subscribed PVs for changes and send update events
    to connected clients."""
    from time import sleep
    while True:
        sleep(update_interval)
        update_all_PVs()

def properties(object):
    "list of property names of a given class object"
    names = []
    for name in dir(object):
        if name.startswith("__") and name.endswith("__"): continue
        x = getattr(object,name)
        if callable(x): continue
        names += [name]
    return names

def object_instance(PV_name):
    """The PV class object hosting a given process variable.
    If not found the return value is None."""
    for pv in PV.instances:
        if pv.__name__ == PV_name: return pv

def object_property(PV_name):
    """The name of the property of an PV class object, hosting a given
    process variable. If not found, the return value is None."""
    for pv in PV.instances:
        if pv.__name__ == PV_name: return "value"

# CA Message command codes:

commands = {
    "VERSION": 0,
    "EVENT_ADD": 1,
    "EVENT_CANCEL": 2,
    "READ": 3,
    "WRITE": 4,
    "SNAPSHOT": 5,
    "SEARCH": 6,
    "BUILD": 7,
    "EVENTS_OFF": 8,
    "EVENTS_ON": 9,
    "READ_SYNC": 10,
    "ERROR": 11,
    "CLEAR_CHANNEL": 12,
    "RSRV_IS_UP": 13,
    "NOT_FOUND": 14,
    "READ_NOTIFY": 15,
    "READ_BUILD": 16,
    "CREATE_CHAN": 18,
    "WRITE_NOTIFY": 19,
    "CLIENT_NAME": 20,
    "HOST_NAME": 21,
    "ACCESS_RIGHTS": 22,
    "ECHO": 23,
    "SIGNAL": 25,
    "CREATE_CH_FAIL": 26,
    "SERVER_DISCONN": 27,
}

def command_name(command_code):
    """'VERSION', 'EVENT_ADD',.... """
    if not command_code in commands.values(): return str(command_code)
    return commands.keys()[commands.values().index(command_code)]

# CA Payload Data Types:

types = {
    "STRING": 0,
    "SHORT": 1,
    "FLOAT": 2,
    "ENUM": 3,
    "CHAR": 4,
    "LONG": 5,
    "DOUBLE": 6,
    "STS_STRING": 7,
    "STS_SHORT": 8,
    "STS_FLOAT": 9,
    "STS_ENUM": 10,
    "STS_CHAR": 11,
    "STS_LONG": 12,
    "STS_DOUBLE": 13,
    "TIME_STRING": 14,
    "TIME_SHORT": 15,
    "TIME_FLOAT": 16,
    "TIME_ENUM": 17,
    "TIME_CHAR": 18,
    "TIME_LONG": 19,
    "TIME_DOUBLE": 20,
    "GR_STRING": 21,
    "GR_SHORT": 22,
    "GR_FLOAT": 23,
    "GR_ENUM": 24,
    "GR_CHAR": 25,
    "GR_LONG": 26,
    "GR_DOUBLE": 27,
    "CTRL_STRING": 28,
    "CTRL_SHORT": 29,
    "CTRL_FLOAT": 30,
    "CTRL_ENUM": 31,
    "CTRL_CHAR": 32,
    "CTRL_LONG": 33,
    "CTRL_DOUBLE": 34,
}

def type_name(data_type):
    """Channel Access data type as string. data_type: integer number"""
    if not data_type in types.values(): return str(data_type)
    return types.keys()[types.values().index(data_type)]

# Return status codes
status_codes = {
    "NORMAL":          0,
    "MAXIOC":          1,
    "UKNHOST":         2,
    "UKNSERV":         3,
    "SOCK":            4,
    "CONN":            5,
    "ALLOCMEM":        6,
    "UKNCHAN":         7,
    "UKNFIELD":        8,
    "TOLARGE":         9,
    "TIMEOUT":         10,
    "NOSUPPORT":       11,
    "STRTOBIG":        12,
    "DISCONNCHID":     13,
    "BADTYPE":         14,
    "CHIDNOTFND":      15,
    "CHIDRETRY":       16,
    "INTERNAL":        17,
    "DBLCLFAIL":       18,
    "GETFAIL":         19,
    "PUTFAIL":         20,
    "ADDFAIL":         21,
    "BADCOUNT":        22,
    "BADSTR":          23,
    "DISCONN":         24,
    "DBLCHNL":         25,
    "EVDISALLOW":      26,
    "BUILDGET":        27,
    "NEEDSFP":         28,
    "OVEVFAIL":        29,
    "BADMONID":        30,
    "NEWADDR":         31,
    "NEWCONN":         32,
    "NOCACTX":         33,
    "DEFUNCT":         34,
    "EMPTYSTR":        35,
    "NOREPEATER":      36,
    "NOCHANMSG":       37,
    "DLCKREST":        38,
    "SERVBEHIND":      39,
    "NOCAST":          40,
    "BADMASK":         41,
    "IODONE":          42,
    "IOINPROGRESS":    43,
    "BADSYNCGRP":      44,
    "PUTCBINPROG":     45,
    "NORDACCESS":      46,
    "NOWTACCESS":      47,
    "ANACHRONISM":     48,
    "NOSEARCHADDR":    49,
    "NOCONVERT":       50,
    "BADCHID":         51,
    "BADFUNCPTR":      52,
    "ISATTACHED":      53,
    "UNAVAILINSERV":   54,
    "CHANDESTROY":     55,
    "BADPRIORITY":     56,
    "NOTTHREADED":     57,
    "16KARRAYCLIENT":  58,
    "CONNSEQTMO":      59,
    "UNRESPTMO":       60,
}

severities = {
    "WARNING": 0, 
    "SUCCESS": 1,
    "ERROR":   2,
    "INFO":    3,
    "SEVERE":  4,
    "FATAL":   6,
}

# Protocol version 4.11:
major_version = 4
minor_version = 11
# CA server port = 5056 + major version * 2 = 5064
# CA repeater port = 5056 + major version * 2 + 1  = 5065
TCP_port_number = 5064 # fixed
UDP_port_number = 5064 # default, may be different if multiple servers running
server_started = False

def start_server():
    global server_started
    if server_started: return
    server_started = True

    UDP_server = UDPServer(("",UDP_port_number),UDPHandler)
    from threading import Thread
    task = Thread(target=UDP_server.serve_forever,name="UDP_server.serve_forever")
    task.daemon = True
    task.start()
    
    # Multiple CA servers may run on the same machine listening at the same UDP
    # port number 5064.
    # However, only the first server started can use TCP port 5064, the others
    # have to use different port numbers (5065,5066,...).
    global TCP_port_number
    while True:
        from socket import error as socket_error
        try:
            TCP_server = ThreadingTCPServer(("",TCP_port_number),TCPHandler)
            break
        except socket_error: TCP_port_number += 1
    if DEBUG: debug("server version %s, listening on TCP/UDP port %d." % (__version__,TCP_port_number))
    task = Thread(target=TCP_server.serve_forever,name="TCP_server.serve_forever")
    task.daemon = True
    task.start()

    # Keep polling actively subscribed PVs and sending updates to connected
    # clients.
    task = Thread(target=update_all_PVs_loop,name="update_all_PVs_loop")
    task.daemon = True
    task.start()

import SocketServer

class UDPServer(SocketServer.UDPServer,SocketServer.ThreadingMixIn):
    """UPD server with customized socket options"""
    # No long timeout for restarting the server ("port in use")
    allow_reuse_address = True
    # Ctrl-C will cleanly kill all spawned threads
    daemon_threads = True
    def server_bind(self):
        """Called by constructor to bind the socket."""
        import socket
        if self.allow_reuse_address:
            # Without using the option SO_REUSEADDR, only one process can
            # listen on a given UPD port number (error 'Address already in use').
            # Also, without this option, one would have to wait 60 seconds
            # after the server terminates, before another process can bind to
            # the same port number, the time the socket remains in
            # CLOSED_WAIT" state.
            self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        if hasattr(socket,"SO_REUSEPORT"):
            # SO_REUSEPORT allows completely duplicate bindings by multiple
            # processes if they all set SO_REUSEPORT before binding the port.
            # This option permits multiple instances of a program to each
            # receive UDP/IP multicast or broadcast datagrams destined for the
            # bound port.
            # This option is needed for Mac OS X. On Linux, it is sufficient to
            # set SO_REUSEADDR.
            self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
        self.socket.bind(self.server_address)
        self.server_address = self.socket.getsockname()

class ThreadingTCPServer(SocketServer.ThreadingTCPServer,SocketServer.ThreadingMixIn):
    # No long timeout for restarting the server ("port in use")
    import os
    if os.name == "nt": allow_reuse_address = False # Windows
    else: allow_reuse_address = True # Linux and Mac OS X
    # Ctrl-C will cleanly kill all spawned threads
    daemon_threads = True

class UDPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        addr = "%s:%d" % self.client_address
        from socket import error as socket_error
        messages = self.request[0]
        # Several replies may be concantenated. Break them up.
        while len(messages) > 0:
            # The minimum message size is 16 bytes. If the 'payload size'
            # field has value > 0, the total size if 16+'payload size'.
            from struct import unpack
            payload_size, = unpack(">H",messages[2:4])
            message = messages[0:16+payload_size]
            messages = messages[16+payload_size:]
            ##if DEBUG: debug("%s: UDP packet received: %s\n" % (addr,message_info(message)))
            reply = process_message(self.client_address,message)
            if reply:
                if DEBUG: debug("%s: returning reply %r" % (addr,message_info(reply)))
                self.request[1].sendto(reply,self.client_address)

connections = {} # list of active client TCP connections

class TCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        addr = "%s:%d" % self.client_address
        if DEBUG: debug("%s: accepted connection" % addr)
        # Update list of active client connections.
        connections[self.client_address] = self.request
        while 1:
            import socket
            # Several messages may be concatenated. Read one at a time.
            # The minimum message size is 16 bytes.
            try: message = self.request.recv(16)
            except socket.error:
                if DEBUG: debug("%s: lost connection\n" % addr)
                break
            if len(message) == 0:
                if DEBUG: debug("%s: client disconnected" % addr)
                break
            if len(message) < 4:
                if DEBUG: debug("excepted 4 received %d bytes" % len(message))
                break
            # If the 'payload size' field has value > 0, 'payload size'
            # more bytes are part of the message.
            from struct import unpack
            payload_size, = unpack(">H",message[2:4])
            if payload_size > 0:
                try: message += self.request.recv(payload_size)
                except socket.error:
                    if DEBUG: debug("%s: lost connection\n" % addr)
            if DEBUG: debug("%s: received: %s\n" % (addr,message_info(message)))
            reply = process_message(self.client_address,message)
            if reply:
                if DEBUG: debug("%s: returning reply %r" % (addr,message_info(reply)))
                try: self.request.sendall(reply)
                except socket.error:
                    if DEBUG: debug("%s: lost connection\n" % addr)
        # Update list of active client connections.
        for PV in PVs.values():
            if self.client_address in PV.subscribers:
                del PV.subscribers[self.client_address]
        del connections[self.client_address]
        if DEBUG: debug("%s: closing connection" % addr)
        self.request.close()

def process_message(address,request):
    """Interpret a CA protocol datagram"""
    from struct import unpack
    from time import time
    
    header = request[0:16].ljust(16,'\0')
    payload = request[16:]
    command_code,payload_size,data_type,data_count,parameter1,parameter2 = \
        unpack(">HHHHII",header)

    command = command_name(command_code)
    if command == "SEARCH":
        # Client wants to knoww wether this server hosts a specific
        # process variable
        reply_flag = data_type
        minor_version = data_count
        channel_CID = parameter1 # client allocated ID for this transaction.
        channel_name = payload.rstrip("\0")
        if PV_exists(channel_name):
            if DEBUG: debug("SEARCH,reply_flag=%r,minor_ver=%r,channel_CID=%r,channel_name=%r\n"
                % (reply_flag,minor_version,channel_CID,channel_name))
            return message("SEARCH",8,TCP_port_number,0,0xffffffff,channel_CID,
                network_data(minor_version,types["SHORT"]))
        # Reply flag: whether failed search response should be returned.
        # 10 = do reply, 5 = do not reply
        if reply_flag == 10:
            return message("NOT_FOUND",0,reply_flag,minor_version,channel_CID,
                channel_CID)
    elif command == "VERSION":
        # Client 'greeting' after opening a TCP connection, part 1
        # There is no response to this command.
        pass
        ##if DEBUG: debug("VERSION\n")
    elif command == "CLIENT_NAME":
        # Client 'greeting' after opening a TCP connection, part 2
        # There is no response to this command. 
        if DEBUG: debug("CLIENT_NAME\n")
    elif command == "HOST_NAME":
        # Client 'greeting' after opening a TCP connection, part 3
        # There is no response to this command. 
        if DEBUG: debug("HOST_NAME\n")
    elif command == "CREATE_CHAN":
        # Client requests "session identity" for a process variable passed by
        # name. 
        channel_CID = parameter1
        minor_version = parameter2
        channel_name = payload.rstrip("\0")
        if DEBUG: debug("CREATE_CHAN channel_CID=%r, minor_version=%r" %
            (channel_CID,minor_version))
        if not PV_exists(channel_name): return
        if not channel_name in PVs.keys(): PVs[channel_name] = PV_info()
        PV = PVs[channel_name]
        val = PV_value(channel_name)
        data_type = CA_type(val)
        data_count = CA_count(val)
        if PV.channel_SID == None:
            channel_SIDs = [pv.channel_SID for pv in PVs.values()]
            PV.channel_SID = 1
            while PV.channel_SID in channel_SIDs: PV.channel_SID += 1
        reply = message("CREATE_CHAN",0,data_type,data_count,channel_CID,
            PV.channel_SID)
        access_rights = 3 # Read and write
        reply += message("ACCESS_RIGHTS",0,0,0,channel_CID,access_rights)
        return reply
    elif command == "READ_NOTIFY":
        # Client wants know the current value if a process variable,
        # referenced by server ID, without receiving update events.
        # Channel Access Protocol Specification, section 6.15.2, says: 
        # parameter 1: channel_SID, parameter 2: IOID
        # However, I always get: parameter 1 = 1, parameter 2 = 1.
        # Thus, I assume: 
        # parameter 1: status_code, parameter 2: IOID
        # status_code = 1 indicates normal successful completion
        channel_SID = parameter1
        IOID = parameter2
        if DEBUG: debug("READ_NOTIFY data_type=%r,data_count=%r,channel_SID=%r,IOID=%r"
            % (data_type,data_count,channel_SID,IOID))
        for PV_name in PVs.keys():
            PV = PVs[PV_name]
            if PV.channel_SID == channel_SID:
                status_code = 1 # Normal successful completion
                val = PV_value(PV_name)
                data_count = CA_count(val)
                data = network_data(val,data_type)
                reply = message("READ_NOTIFY",0,data_type,data_count,status_code,
                    IOID,data)
                return reply
    elif command == "EVENT_ADD":
        # Client wants to receive update events for a given process variable.
        channel_SID = parameter1
        subscription_ID = parameter2
        low_val,high_val,to_val,mask = unpack(">fffH",payload[0:14])
        if DEBUG: debug("EVENT_ADD {data_type:%s, data_count:%r, "\
            "channel_SID:%r, subscription_ID:%r}, "\
            "payload={low_val:%r, high_val:%r, to_val:%r, mask:%r}"
            % (type_name(data_type),data_count,channel_SID,subscription_ID,
            low_val,high_val,to_val,mask))
        for PV_name in PVs.keys():
            PV = PVs[PV_name]
            if PV.channel_SID == channel_SID:
                PV.subscribers[address] = \
                    subscriber_info(subscription_ID,data_type,data_count)
                subscriber = PV.subscribers[address]
                status_code = 1 # Normal successful completion
                val = PV_value(PV_name)
                data_count = CA_count(val)
                data = network_data(val,data_type)
                return message("EVENT_ADD",0,data_type,data_count,
                    status_code,subscription_ID,data)
    elif command == "WRITE_NOTIFY":
        # Client wants to modify a process variable.
        # This requests needs to be confirmed by a WRITE_NOTIFY reply when
        # complete.
        channel_SID = parameter1
        IOID = parameter2
        new_value = value(data_type,data_count,payload)
        if DEBUG: debug("WRITE_NOTIFY data_type=%r, data_count=%r, channel_SID=%r, "\
            "IOID=%r, value=%r\n" %
            (data_type,data_count,channel_SID,IOID,new_value))
        for PV_name in PVs.keys():
            PV = PVs[PV_name]
            if PV.channel_SID == channel_SID:
                if DEBUG: debug("Changing %r to %r\n" % (PV_name,new_value))
                PV_set_value(PV_name,new_value)
                call_callbacks(PV_name)
                status_code = 1 # Normal successful completion
                reply = message("WRITE_NOTIFY",0,data_type,data_count,
                    status_code,IOID)
                return reply
    elif command == "ACCESS_RIGHTS": # not a client request (server only)
        channel_ID = parameter1
        access_bits = parameter2
        if DEBUG: debug("ACCESS_RIGHTS channel_ID=%r, access_bits=%s (ignored)\n" %
            (channel_ID,access_bits))
    elif command == "WRITE":
        # Client wants to modify a process variable.
        # Unlike WRITE_NOTIFY, there is no response to this command. 
        channel_SID = parameter1
        IOID = parameter2
        new_value = value(data_type,data_count,payload)
        if DEBUG: debug("WRITE data_type=%r, data_count=%r, channel_SID=%r, "\
            "IOID=%r, value=%r\n" %
            (data_type,data_count,channel_SID,IOID,new_value))
        for PV_name in PVs.keys():
            PV = PVs[PV_name]
            if PV.channel_SID == channel_SID:
                if DEBUG: debug("Changing %r to %r\n" % (PV_name,new_value))
                PV_set_value(PV_name,new_value)
                call_callbacks(PV_name)
    elif command == "ECHO":
        # Client wants to be sure that server is still alive and reachable.
        return message("ECHO",0,0,0,0,0)
    elif command == 'EVENT_CANCEL':
        # Opposite of EVENT_ADD.
        # Client no longer wants to receive update events.
        channel_SID = parameter1
        subscription_ID = parameter2
        if DEBUG: debug("EVENT_CANCEL {data_type:%s,data_count:%r, "\
            "channel_SID:%r,subscription_ID:%r},"
            % (type_name(data_type),data_count,channel_SID,subscription_ID))
        for PV_name in PVs.keys():
            PV = PVs[PV_name]
            if PV.channel_SID == channel_SID:
                if address in PV.subscribers and \
                    PV.subscribers[address].subscription_ID == subscription_ID:
                    del PV.subscribers[address]
                    if DEBUG: debug("Cancelled updates for %r %r" % (PV_name,address))
    elif command == 'CLEAR_CHANNEL':
        # Opposite of CREATE_CHAN. Client indicates it will not use a certain
        # client ID for a PV any longer.
        channel_SID = parameter1
        channel_CID = parameter2
        if DEBUG: debug("CLEAR_CHANNEL channel_SID=%r, channel_CID=%r" %
            (channel_SID,channel_CID))
        # Nothing to do, because there is no status information associated
        # with a channel CID. There are no resources allocated per-channel CID.
        return message('CLEAR_CHANNEL',0,0,0,channel_SID,channel_CID)
    else:
        if DEBUG: debug("command %r: not supported (yet)\n" % command)

def object_name(object):
    """Convert Python object to string"""
    if hasattr(object,"__name__"): return object.__name__
    else: return repr(object)

def message(command=0,payload_size=0,data_type=0,data_count=0,
        parameter1=0,parameter2=0,payload=""):
    """Assemble a Channel Access message datagram for network transmission"""
    if type(command) == str: command = commands[command]
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
    if len(message) < 16: return "invalid message %r" % message
    header = message[0:16]
    payload = message[16:]
    command_code,payload_size,data_type,data_count,parameter1,parameter2 = \
        unpack(">HHHHII",header)
    s = str(command_code)
    command = command_name(command_code)
    if command: s += "("+command+")"
    s += ","+str(payload_size)
    s += ","+str(data_type)
    if data_type in types.values():
        s += "("+types.keys()[types.values().index(data_type)]+")"
    s += ","+str(data_count)
    s += ", %r, %r" % (parameter1,parameter2)
    if payload:
        s += ", %r" % payload
        if command in ("EVENT_ADD","WRITE","WRITE_NOTIFY","READ_NOTIFY"):
            s += "("
            header = header_info(data_type,payload)
            if header: s += header+"; "
            s += repr(value(data_type,data_count,payload))
            s += ")"
    return s     

def send(socket,message):
    """Return a reply to a client using TCP/IP"""
    from socket import error as socket_error
    try: addr = "%s:%d" % socket.getpeername()
    except socket_error: addr = "?"
    if DEBUG: debug("Send %s %s\n" % (addr,message_info(message)))
    ##socket.setblocking(0)
    try: socket.sendall(message)
    except socket_error,error:
        if DEBUG: debug("Send failed %r\n" % error)

def value(data_type,data_count,payload):
    """Convert network binary data to a Python data type
    data_type: integer data type code"""
    if payload == None: return None
    from struct import unpack
    data_type = type_name(data_type)
    
    header_size = 0
    if data_type.startswith("STS_"):
        header_size = 2+2 # status,severity
        # Add alignment padding to header.
        if data_type.endswith("CHAR"):    header_size += 1       
        elif data_type.endswith("DOUBLE"):header_size += 4
    elif data_type.startswith("TIME_"):
        header_size = 12
        # Add alignment padding to header.
        if data_type.endswith("SHORT"):   header_size += 2
        elif data_type.endswith("ENUM"):  header_size += 2
        elif data_type.endswith("CHAR"):  header_size += 3
        elif data_type.endswith("DOUBLE"):header_size += 4
    elif data_type.startswith("GR_"):
        header_size = 2+2 # status,severity
        if data_type.endswith("STRING"):  pass     
        elif data_type.endswith("SHORT"): header_size += 8+6*2 # unit,limits    
        elif data_type.endswith("FLOAT"): header_size += 2+2+8+6*4 # precision,pad,unit,limits   
        elif data_type.endswith("ENUM"):  header_size += 2+16*26 # nstrings,strings      
        elif data_type.endswith("CHAR"):  header_size += 8+6*1+1 # unit,limits,pad       
        elif data_type.endswith("LONG"):  header_size += 8+6*4 # unit,limits
        elif data_type.endswith("DOUBLE"):header_size += 2+2+8+6*8 # precision,pad,unit,limits
        else:
            if DEBUG: debug("value: data type %r not supported\n" % data_type)
    elif data_type.startswith("CTRL_"):
        header_size = 2+2 # status,severity
        if data_type.endswith("STRING"):  pass     
        elif data_type.endswith("SHORT"): header_size += 8+8*2 # unit,limits    
        elif data_type.endswith("FLOAT"): header_size += 2+2+8+8*4 # precision,pad,unit,limits   
        elif data_type.endswith("ENUM"):  header_size += 2+16*26 # nstrings,strings      
        elif data_type.endswith("CHAR"):  header_size += 8+8*1+1 # unit,limits,pad       
        elif data_type.endswith("LONG"):  header_size += 8+8*4 # unit,limits
        elif data_type.endswith("DOUBLE"):header_size += 2+2+8+8*8 # precision,pad,unit,limits
        else:
            if DEBUG: debug("value: data type %r not supported\n" % data_type)

    payload = payload[header_size:] # strip off header

    if data_type.endswith("STRING"):
        # Null-terminated string.
        # data_count is the number of null-terminated strings (characters)
        value = payload.split("\0")[0:data_count]
        if len(value) == 1: value = value[0]
    elif data_type.endswith("SHORT"):
        if data_count > len(payload)/2: data_count = max(len(payload)/2,1)
        payload = payload.ljust(2*data_count,"\0")
        value = list(unpack(">%dh"%data_count,payload[0:2*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type.endswith("FLOAT"):
        if data_count > len(payload)/4: data_count = max(len(payload)/4,1)
        payload = payload.ljust(4*data_count,"\0")
        value = list(unpack(">%df"%data_count,payload[0:4*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type.endswith("ENUM"):
        if data_count > len(payload)/2: data_count = max(len(payload)/2,1)
        payload = payload.ljust(2*data_count,"\0")
        value = list(unpack(">%dh"%data_count,payload[0:2*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type.endswith("CHAR"):
        if data_count > len(payload)/1: data_count = max(len(payload)/1,1)
        payload = payload.ljust(1*data_count,"\0")
        value = list(unpack("%db"%data_count,payload[0:1*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type.endswith("LONG"):
        if data_count > len(payload)/4: data_count = max(len(payload)/4,1)
        payload = payload.ljust(4*data_count,"\0")
        value = list(unpack(">%di"%data_count,payload[0:4*data_count]))
        if len(value) == 1: value = value[0]
    elif data_type.endswith("DOUBLE"):
        if data_count > len(payload)/8: data_count = max(len(payload)/8,1)
        payload = payload.ljust(8*data_count,"\0")
        value = list(unpack(">%dd"%data_count,payload[0:8*data_count]))
        if len(value) == 1: value = value[0]
    else:
        if DEBUG: debug("value: unsupported data type %r\n" % data_type)
        value = payload

    return value

def header_info(data_type,payload):
    """Report additional non-payload in network binary data.
    These can be status, time, grapic or control structures"""
    # Structures are defined in db_access.h.
    if payload == None: return ""
    from struct import unpack
    data_type = type_name(data_type)
    
    if data_type.startswith("STS_"):
        status,severity = unpack(">HH",payload[0:4])
        # Expecting status = 0 (normal), severity = 1 (success)
        return "{status:%d,severity:%d}" % (status,severity)
    elif data_type.startswith("TIME_"):
        status,severity = unpack(">HH",payload[0:4])
        # The time stamp is represented as two uint32 values. The first is the
        # number of seconds passed since 1 Jan 1990 00:00 GMT. The second is the
        # number of nanoseconds within the second.
        seconds,nanoseconds = unpack(">II",payload[4:12])
        from time import mktime,strftime,gmtime
        offset = mktime((1990,1,1,0,0,0,0,0,0))-mktime((1970,1,1,0,0,0,0,0,0))
        t = seconds+nanoseconds*1e-9 + offset
        timestamp = strftime("%Y-%m-%d %H:%M:%S GMT",gmtime(t))
        return "{status:%d,severity:%d, timestamp:%s}" % \
            (status,severity,timestamp)
    elif data_type.startswith("GR_"):
        status,severity = unpack(">HH",payload[0:4])
        info = "status:%d,severity:%d, " % (status,severity)
        if data_type.endswith("STRING"): pass
        elif data_type.endswith("SHORT"):
            unit = payload[8:16].rstrip("\0")
            limits = unpack("6h",payload[16:16+6*2])
            info += "unit=%r,limits=%r" % (unit,limits)            
        elif data_type.endswith("FLOAT"):
            precision, = unpack(">h",payload[4:6])
            unit = payload[8:16].rstrip("\0")
            limits = unpack(">6f",payload[16:16+6*4])
            info += "precision=%r,unit=%r,limits=%r" % (precision,unit,limits)
        elif data_type.endswith("ENUM"):
            nstrings, = unpack(">h",payload[4:6])
            strings = payload[6:6+16*26]
            info += "nstrings=%r" % nstrings
        elif data_type.endswith("CHAR"):
            unit = payload[8:16].rstrip("\0")
            limits = unpack("6b",payload[16:16+6*1])
            info += "unit=%r,limits=%r" % (unit,limits)
        elif data_type.endswith("LONG"):
            unit = payload[8:16].rstrip("\0")
            limits = unpack("6i",payload[16:16+6*4])
            info += "unit=%r,limits=%r" % (unit,limits)
        elif data_type.endswith("DOUBLE"):
            precision, = unpack(">h",payload[4:6])
            unit = payload[8:16].rstrip("\0")
            limits = unpack(">6d",payload[16:16+6*8])
            info += "precision=%r,unit=%r,limits=%r" % (precision,unit,limits)
        else: info += "?"
        info = info.restrip(", ")
        return "{"+info+"}"
    elif data_type.startswith("CTRL_"):
        status,severity = unpack(">HH",payload[0:4])
        info = "status:%d,severity:%d, " % (status,severity)
        if data_type.endswith("STRING"): pass
        elif data_type.endswith("SHORT"):
            unit = payload[8:16].rstrip("\0")
            limits = unpack("8h",payload[16:16+8*2])
            info += "unit=%r,limits=%r" % (unit,limits)            
        elif data_type.endswith("FLOAT"):
            precision, = unpack(">h",payload[4:6])
            unit = payload[8:16].rstrip("\0")
            limits = unpack(">8f",payload[16:16+8*4])
            info += "precision=%r,unit=%r,limits=%r" % (precision,unit,limits)
        elif data_type.endswith("ENUM"):
            nstrings, = unpack(">h",payload[4:6])
            strings = payload[6:6+16*26]
            info += "nstrings=%r" % nstrings
        elif data_type.endswith("CHAR"):
            unit = payload[8:16].rstrip("\0")
            limits = unpack("8b",payload[16:16+8*1])
            info += "unit=%r,limits=%r" % (unit,limits)
        elif data_type.endswith("LONG"):
            unit = payload[8:16].rstrip("\0")
            limits = unpack("8i",payload[16:16+8*4])
            info += "unit=%r,limits=%r" % (unit,limits)
        elif data_type.endswith("DOUBLE"):
            precision, = unpack(">h",payload[4:6])
            unit = payload[8:16].rstrip("\0")
            limits = unpack(">8d",payload[16:16+8*8])
            info += "precision=%r,unit=%r,limits=%r" % (precision,unit,limits)
        else: info += "?"
        info = info.rstrip(", ")
        return "{"+info+"}"
    return ""

def convert(PV_name,value):
    """Convert value to the correct data type for the given process variable"""
    # The value of a PV might be passed as string when the PV type is acually
    # DOUBLE.
    current_value = PV_value(PV_name)
    if current_value is None: new_value = value
    elif not isarray(current_value):
        dtype = type(current_value)
        try: new_value = dtype(value)
        except Exception,message:
            if DEBUG: debug("convert: %r from %r to %r failed: %r" %
                (PV_name,value,dtype,message))
            new_value = dtype()
    else:
        if not isarray(value): value = [value]
        # Convert each array element.
        if len(current_value) > 0: dtype = type(current_value[0])
        else: dtype = float
        try: new_value = [dtype(x) for x in value]
        except Exception,message:
            if DEBUG: debug("convert: %r from %r to %r failed: %r" %
                (PV_name,value,dtype,message))
            new_value = [dtype()]*len(value)
    if new_value != value:
        if DEBUG: debug("converting %r from %r to %r" % (PV_name,value,new_value))
    return new_value

def CA_type(value):
    """Channel Access data type for a Python variable as integer type code"""
    if issubclass(type(value),basestring): return types["STRING"]
    if hasattr(value,"dtype"): # value is an array
        from numpy import int8,int16,int32,float32,int64,float64
        if value.dtype == int16: return types["SHORT"]
        if value.dtype == float32: return types["FLOAT"]
        if value.dtype == int8: return types["CHAR"]
        if value.dtype == int32: return types["LONG"]
        if value.dtype == int64: return types["LONG"]
        if value.dtype == float64: return types["DOUBLE"]
        if value.dtype == bool: return types["CHAR"]
        return types["STRING"]
    # If a list if given, use the first element to determine the type.
    if isarray(value):
        if len(value)>0: value = value[0]
        else: return types["DOUBLE"]
    if type(value) == int: return types["LONG"]
    if type(value) == float: return types["DOUBLE"]
    if type(value) == bool: return types["SHORT"]
    return types["STRING"]

def CA_count(value):
    """If value is an array return the number of elements, else return 1.
    In CA, a string counts as a single element."""
    if issubclass(type(value),basestring): return 1
    if isarray(value): return len(value)
    return 1

def network_data(value,data_type):
    """Convert a Python data type to binary data for network transmission
    data_type: integer number for CA payload data type (0 = STRING, 1 = SHORT)
    """
    from struct import pack
    data_type = type_name(data_type)
    payload = ""

    precision = 8 # Number of digits displayed in MEDM screen
    
    if data_type.startswith("STS_"):
        status = 0 # 0 = normal
        severity = 1 # 1 = success
        payload += pack(">HH",status,severity)
        # Add alignment padding to the header.
        if data_type.endswith("CHAR"):     payload += '\0'       
        elif data_type.endswith("DOUBLE"): payload += '\0'*4
    elif data_type.startswith("TIME_"):
        # Add time header
        from time import mktime,time
        status = 0 # 0 = normal
        severity = 1 # 1 = sucess
        # The time stamp is represented as two uint32 values. The first is the
        # number of seconds passed since 1 Jan 1990 00:00 GMT. The second is the
        # number of nanoseconds within the second.
        offset = mktime((1990,1,1,0,0,0,0,0,0))-mktime((1970,1,1,0,0,0,0,0,0))
        timestamp = time()-offset
        seconds = int(timestamp)
        nanoseconds = int((timestamp%1)*1e9)
        payload += pack(">HHII",status,severity,seconds,nanoseconds)
        # Add alignment padding to the header.
        if data_type.endswith("SHORT"):    payload += '\0'*2
        elif data_type.endswith("ENUM"):   payload += '\0'*2
        elif data_type.endswith("CHAR"):   payload += '\0'*3
        elif data_type.endswith("DOUBLE"): payload += '\0'*4
    elif data_type.startswith("GR_"):
        status = 0 # 0 = normal
        severity = 1 # 1 = success
        payload += pack(">HH",status,severity)
        if data_type.endswith("STRING"): pass     
        elif data_type.endswith("SHORT"):
            payload += '\0'*(8+6*2) # unit,limits    
        elif data_type.endswith("FLOAT"):
            payload += pack(">h",precision)
            payload += '\0'*(2+8+6*4) # pad,unit,limits
        elif data_type.endswith("ENUM"):
            payload += '\0'*(2+16*26) # number of strings,strings 
        elif data_type.endswith("CHAR"):
            payload += '\0'*(8+6*1+1) # unit,limits,pad      
        elif data_type.endswith("LONG"):
            payload += '\0'*(8+6*4) # unit,limits  
        elif data_type.endswith("DOUBLE"):
            payload += pack(">h",precision)
            payload += '\0'*(2+8+6*8) # pad,unit,limits
        else:
            if DEBUG: debug("network_data: data type %r not supported\n" % data_type)
    elif data_type.startswith("CTRL_"):
        status = 0 # 0 = normal
        severity = 1 # 1 = success
        payload += pack(">HH",status,severity)
        if data_type.endswith("STRING"): pass     
        elif data_type.endswith("SHORT"):
            payload += '\0'*(8+8*2) # unit,limits    
        elif data_type.endswith("FLOAT"):
            payload += pack(">h",precision)
            payload += '\0'*(2+8+8*4) # pad,unit,limits
        elif data_type.endswith("ENUM"):
            payload += '\0'*(2+16*26) # number of strings,strings 
        elif data_type.endswith("CHAR"):
            payload += '\0'*(8+8*1+1) # unit,limits,pad      
        elif data_type.endswith("LONG"):
            payload += '\0'*(8+8*4) # unit,limits  
        elif data_type.endswith("DOUBLE"):
            payload += pack(">h",precision)
            payload += '\0'*(2+8+8*8) # pad,unit,limits
        else:
            if DEBUG: debug("network_data: data type %r not supported\n" % data_type)

    from numpy import int8,int16,int32,float32,float64

    if data_type.endswith("STRING"):
        if isarray(value):
            # Null-terminated strings.
            payload += "\0".join([str(v) for v in value])
        else: payload += str(value)
    elif data_type.endswith("SHORT"):
        if isarray(value):
            for v in value: payload += pack(">h",to(v,int16))
        else: payload += pack(">h",to(value,int16))
    elif data_type.endswith("FLOAT"):
        if isarray(value):
            for v in value: payload += pack(">f",to(v,float32))
        else: payload += pack(">f",to(value,float32))
    elif data_type.endswith("ENUM"):
        if isarray(value):
            for v in value: payload += pack(">h",to(v,int16))
        else: payload += pack(">h",to(value,int16))
    elif data_type.endswith("CHAR"):
        if isarray(value):
            for v in value: payload += pack("b",to(v,int8))
        else: payload += pack("b",to(value,int8))
    elif data_type.endswith("LONG"):
        if isarray(value):
            for v in value: payload += pack(">i",to(v,int32))
        else: payload += pack(">i",to(value,int32))
    elif data_type.endswith("DOUBLE"):
        if isarray(value):
            for v in value: payload += pack(">d",to(v,float64))
        else: payload += pack(">d",to(value,float64))
    else:
        if DEBUG: debug("network_data: unsupported data type %r\n" % data_type)
        payload += str(value)

    return payload

def to(value,dtype):
    """Force conversion to int data type. If failed return 0:
    dtype: int8, int32, int64"""
    isfloat = "float" in str(dtype)
    try: return dtype(value)
    except: return 0 if not isfloat else 0.0
    
def isarray(value):
    "Is the value a container, like tuple, list or numpy array?"
    if issubclass(type(value),basestring): return False
    if hasattr(value,"__len__"): return True
    else: return False

def date_string(seconds=None):
    """Date and time as formatted ASCCI text, precise to 1 ms"""
    if seconds is None:
        from time import time
        seconds = time()
    from datetime import datetime
    timestamp = str(datetime.fromtimestamp(seconds))
    return timestamp[:-3] # omit microsconds

def modulename():
    """Name of this Python module, without directory and extension,
    as used for 'import'"""
    from inspect import getmodulename,getfile
    return getmodulename(getfile(modulename))

def nan_equal(a,b):
    """Do a and b have the same value?
    Return value: True or False"""
    ##if DEBUG: debug("nan_equal(%r,%r)?" % (a,b))
    a,b = PV_data(a),PV_data(b)
    from numpy import isnan
    try:
        if isnan(a) and isnan(b): return True
    except: pass
    # Work-around for the following problem:
    # if a == b: ...
    # ValueError: The truth value of an array with more than one element is
    # ambiguous. Use a.any() or a.all()
    if not isinstance(a,basestring) and not isinstance(b,basestring) and \
        hasattr(a,"__len__") and hasattr(b,"__len__") and len(a) == len(b):
        return all([nan_equal(x,y) for (x,y) in zip(a,b)])
    return bool(a == b)

def nan_equal_2(a,b):
    """Are two arrays containing nan identical, assuming nan == nan?"""
    from numpy import asarray
    from numpy.testing import assert_equal
    a,b = asarray(a),asarray(b)
    try: assert_equal(a,b)
    except: value = False
    value = True
    if DEBUG: debug("nan_equal(%r,%r): %r" % (a,b,value))
    return value

def logfile(): return "" # for backward compatibility


if __name__ == "__main__": # for testing
    from pdb import pm
    import logging
    from tempfile import gettempdir
    logfile = gettempdir()+"/CAServer.log"
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s: %(levelname)s %(message)s",
        ##filename=logfile,
    )
    DEBUG = True

    PV_name = "TEST:TEST.VAL"
    casput(PV_name,"test")
    print('from epics import *; caget(%r)' % PV_name)
    print('from epics import *; camonitor(%r)' % PV_name)
