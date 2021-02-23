import base64
from Crypto.Cipher import AES

class AESCrypto:
    class InvalidBlockSizeError(Exception):
        """Raised for invalid block sizes"""
        pass

    def __init__(self, key):
        self.key = key
        self.iv = bytes(key[0:24], 'utf-8')
        print(self.key)
        print(key[0:24])

    def __pad(self, text):
        text_length = len(text)
        amount_to_pad = AES.block_size - (text_length % AES.block_size)
        if amount_to_pad == 0:
            amount_to_pad = AES.block_size
        pad = chr(amount_to_pad)
        return text + pad * amount_to_pad

    def __unpad(self, text):
        pad = ord(text[-1])
        return text[:-pad]

    def encrypt( self, raw ):
        raw = self.__pad(raw)
        cipher = AES.new(self.key, AES.MODE_ECB, self.iv)
        return base64.b64encode(cipher.encrypt(raw)) 

    def decrypt( self, enc ):
        enc = base64.b64decode(enc)
        cipher = AES.new(self.key, AES.MODE_ECB, self.iv )
        return self.__unpad(cipher.decrypt(enc).decode("utf-8"))