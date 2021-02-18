import base64
import hashlib
from Crypto.Cipher import AES

class AESCrypto(object):
    def __init__(self, algorithm, password):
        self.algorithm = filter(lambda x: not x.isdigit(), algorithm).lower()
        self.bits = int(filter(str.isdigit, algorithm))
        self.bs = 16
        if not self.algorithm == 'aes':
            raise Exception('Only AES crypto is supported')
        if not self.bits % 8 == 0:
            raise Exception('Bits of crypto must be a multiply of 8.')
        self.bytes = self.bits / 8
        self.password = password
        self.generateKeyAndIv()

    def generateKeyAndIv(self):
        last = ''
        allBytes = ''
        maxBytes = self.bytes + self.bs
        while len(allBytes) < maxBytes:
            last = hashlib.md5(last + self.password).digest()
            allBytes += last
        self.key = allBytes[:self.bytes]
        self.iv = allBytes[self.bytes:maxBytes]

    def encrypt(self, raw, outputEncoding):
        outputEncoding = outputEncoding.lower()
        raw = self._pad(raw)
        cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
        encrypted = cipher.encrypt(raw)
        if outputEncoding == 'hex':
            return encrypted.encode('hex')
        elif outputEncoding == 'base64':
            return base64.b64encode(encrypted)
        else:
            raise Exception('Encoding is not supported.')

    def decrypt(self, data, inputEncoding):
        inputEncoding = inputEncoding.lower()
        if inputEncoding == 'hex':
            data = ''.join(map(chr, bytearray.fromhex(data)))
        elif inputEncoding == 'base64':
            data = base64.b64decode(data)
        cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
        return self._unpad(cipher.decrypt(data))

    def _pad(self, data):
        padding = self.bs - len(data) % self.bs
        return data + padding * chr(padding)

    @staticmethod
    def _unpad(data):
        return data[0:-ord(data[-1])]