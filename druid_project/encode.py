def encode(b):
    b=ord(b)
    if ord('a')<=b<=ord('z'):
        return hex(ord('a')+(b-ord('a')+5)%26)
    elif ord('A')<=b<=ord('Z'):
        return hex(ord('A')+(b-ord('A')+5)%26)
    else:
        return hex(b)

def encode_str(s):
    res = [encode(x) for x in s]
    return "".join(res)

M = "VENI."

def ECB(m):
    print(encode_str(m))

counter = 0x45
def CTR(m,counter):
    res = ""
    for i in range(len(m)):
        s = int(encode(chr(counter+i)), 0)
        res+=hex(ord(m[i])^s)
    print(res)
CTR(M,counter)

IV = 0x08
def CBC(m,IV):
    res = ""
    c = IV
    for i in range(len(m)):
        tmp = ord(m[i])^c
        c = encode(chr(tmp))
        res += c
        c = int(c,0)
    print(res)
CBC(M,IV)

IV = 0x66
def OFB(m,IV):
    res = ""
    for i in range(len(m)):
        o = encode(chr(IV))
        res += hex(ord(m[i])^int(o, 0))
        IV = int(o,0)
    print(res)
OFB(M,IV)


