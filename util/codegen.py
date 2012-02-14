# -*- Mode: Python -*-

# XXX have this emit cython instead

import simplejson as json
import keyword

def frob (s):
    s = s.replace ('-','_')
    if keyword.iskeyword (s):
        return '_' + s
    else:
        return s

def gen (ipath, opath):
    ofile = open (opath, 'wb')
    W = ofile.write
    d = json.load (open (ipath, 'rb'))

    domains = {}
    # get the mapping of 'domains' to types...
    for [k, v] in d['domains']:
        domains[k] = v

    def get_arg_type (arg):
        if arg.has_key ('domain'):
            arg_type = domains[arg['domain']]
        else:
            arg_type = arg['type']
        return arg_type

    def emit_methods (arguments):
        # init
        formals = []
        for arg in arguments:
            name = frob (arg['name'])
            if arg.has_key ('default-value'):
                formals.append ('%s=%r' % (name, arg['default-value']))
            else:
                # this seems wrong, but sometimes the spec puts an undefaulted arg
                #   in the middle of the argument list - the alternative would be to
                #   reorder the args, probably not a good idea.
                formals.append ('%s=None' % (name,))
        W ('        def __init__ (self, %s):\n' % (', '.join (formals)))
        for arg in arguments:
            name = frob (arg['name'])
            W ('            self.%s = %s\n' % (name, name))
        if not arguments:
            W ('            pass\n')
        # unpacker
        W ('        def unpack (self, data, pos):\n')
        if not arguments:
            W ('            pass\n')
        bit = 0
        was_bit = False
        for arg in arguments:
            arg_type = get_arg_type (arg)
            if arg_type == 'bit':
                unpack = '(ord(data[pos]) & (1<<%d)), pos' % (bit,)
                bit += 1
                was_bit = True
            else:
                if was_bit or bit == 8:
                    W ('            pos += 1 # post-bit-octet advance\n')
                unpack = 'unpack_%s (data, pos)' % (arg_type,)
                was_bit = False
            W ('            self.%s, pos = %s\n' % (frob (arg['name']), unpack))
        if was_bit:
            W ('            pos += 1 # post-bit-octet advance\n')
        # packer
        W ('        def pack (self):\n')
        W ('            r = []\n')
        has_bits = False
        for arg in arguments:
            arg_type = get_arg_type (arg)
            if arg_type == 'bit':
                has_bits = True
        if has_bits:
            W ('            byte = 0\n')
        bit = 0
        was_bit = False
        for arg in arguments:
            name = frob (arg['name'])
            arg_type = get_arg_type (arg)
            if arg_type == 'bit':
                W ('            if self.%s: byte |= (1<<%d)\n' % (name, bit,))
                bit += 1
                was_bit = True
            else:
                if was_bit or bit == 8:
                    W ('            r.append (chr (byte))\n')
                    W ('            byte = 0\n')
                W ('            r.append (pack_%s (self.%s))\n' % (arg_type, name))
                was_bit = False
        if was_bit:
            W ('            r.append (chr (byte))\n')
        W ('            return "".join (r)\n')

    W ('# -*- Mode: Python -*-\n\n')
    W ('# generated by %s from %s\n' % (sys.argv[0], ipath))
    W ('from wire import *\n\n')
    for c in d['constants']:
        W ('%s = %d\n' % (frob (c['name']), c['value']))
    W ('\n\n')
    W ('method_map = {}\n')
    for c in d['classes']:
        W ('class %s (object):\n' % (frob (c['name']),))
        # This whole 'basic.properties' thing looks like a kludge.
        #   so I'll treat it like a kludge. 8^)
        if c.has_key ('properties') and c['properties']:
            props = c['properties']
            W ('    class properties (object):\n')
            W ('        bit_map = {\n')
            for i in range (len (props)):
                bit = 15-i
                W ('            %d : "%s",\n' % (bit, props[i]['name']))
            W ('        }\n')
            W ('        name_map = {\n')
            for i in range (len (props)):
                ptype = props[i]['type']
                bit = 15-i
                W ('            "%s" : (%d, unpack_%s, pack_%s),\n' % (props[i]['name'], bit, ptype, ptype))
            W ('        }\n')
            W ('        n_properties = %d\n' % (len(props)))
        for m in c['methods']:
            W ('    class %s (object):\n' % (frob (m['name'])))
            W ("        _name = '%s.%s'\n" % (frob (c['name']), frob (m['name'])))
            W ('        id = (%d, %d)\n' % (c['id'], m['id']))
            W ('        __slots__ = ['),
            W (', '.join (["'%s'" % frob (arg['name']) for arg in m['arguments']]))
            W (']\n')
            emit_methods (m['arguments'])
        W ('\n')
        for m in c['methods']:
            W ('method_map[(%d,%d)] = %s.%s\n' % (
                c['id'],
                m['id'],
                frob (c['name']),
                frob (m['name']),
                ))
        W ('\n')

if __name__ == '__main__':
    import sys
    gen (sys.argv[1], 'spec.py')
