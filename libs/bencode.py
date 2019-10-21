# The contents of this file are subject to the BitTorrent Open Source License
# Version 1.1 (the License).  You may not copy or use this file, in either
# source code or executable form, except in compliance with the License.  You
# may obtain a copy of the License at http://www.bittorrent.com/license/.
#
# Software distributed under the License is distributed on an AS IS basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied.  See the License
# for the specific language governing rights and limitations under the
# License.

# Written by Petru Paler
import six


def b(i):
    return six.int2byte(i)


def decode_int(x, f):
    f += 1
    new_f = x.index(b'e', f)
    n = int(x[f:new_f])
    if b(x[f]) == b'-':
        if x[f + 1] == b'0':
            raise ValueError
    elif b(x[f]) == b'0' and new_f != f + 1:
        raise ValueError
    return n, new_f + 1


def decode_string(x, f):
    colon = x.index(b':', f)
    n = int(x[f:colon].decode())
    if b(x[f]) == b'0' and colon != f + 1:
        raise ValueError
    colon += 1
    return x[colon:colon + n], colon + n


def decode_list(x, f):
    r, f = [], f + 1
    while b(x[f]) != b'e':
        v, f = decode_func[b(x[f])](x, f)
        r.append(v)
    return r, f + 1


def decode_dict(x, f):
    r, f = {}, f + 1
    while b(x[f]) != b'e':
        k, f = decode_string(x, f)
        r[k], f = decode_func[b(x[f])](x, f)
    return r, f + 1


decode_func = {
    b'l': decode_list, b'd': decode_dict, b'i': decode_int, b'0': decode_string,
    b'1': decode_string, b'2': decode_string, b'3': decode_string, b'4': decode_string,
    b'5': decode_string, b'6': decode_string, b'7': decode_string, b'8': decode_string,
    b'9': decode_string
}


def bdecode(x):
    try:
        r, l = decode_func[b(x[0])](x, 0)
    except (IndexError, KeyError, ValueError):
        raise Exception("not a valid bencoded string")
    # if l != len(x):
    #    raise Exception("invalid bencoded value (data after valid prefix)")
    return r


class Bencached(object):
    __slots__ = ['bencoded']

    def __init__(self, s):
        self.bencoded = s


def encode_bencached(x, r):
    r.append(x.bencoded)


def encode_int(x, r):
    r.extend(('i', str(x), 'e'))


def encode_bool(x, r):
    if x:
        encode_int(1, r)
    else:
        encode_int(0, r)


def encode_string(x, r):
    r.extend((str(len(x)), ':', x))


def encode_bytes(x, r):
    r.extend((str(len(x)), ':', x))


def encode_list(x, r):
    r.append('l')
    for i in x:
        encode_func[type(i)](i, r)
    r.append('e')


def encode_dict(x, r):
    r.append('d')
    ilist = x.items()
    ilist = sorted(ilist)
    for k, v in ilist:
        r.extend((str(len(k)), ':', k))
        encode_func[type(v)](v, r)
    r.append('e')


encode_func = {
    Bencached: encode_bencached,
    int: encode_int,
    str: encode_string,
    bytes: encode_bytes,
    list: encode_list,
    tuple: encode_list,
    dict: encode_dict,
    bool: encode_bool
}


def bencode(x):
    r = []
    encode_func[type(x)](x, r)
    return b''.join([i if isinstance(i, bytes) else six.b(i) for i in r])
