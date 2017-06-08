# -*- coding: utf-8 -*-


class AlphabetSequence:

    def __init__(self, alphabet, i=0):
        self.alphabet = alphabet
        self.__alphabet_inverse = \
            dict((alphabet[i], i) for i in range(0, len(alphabet)))
        self.__alphabet_first = alphabet[0]
        self.__alphabet_last = alphabet[len(alphabet)-1]
        self.__prev = None if i == 0 else generate(i)

    def __iter__(self):
        return self

    def generate(i=0):
        if i < len(alphabet):
            return alphabet[i]
        ret = ''
        cur = i
        for k in range(int(math.ceil(math.log(i, len(alphabet)))), 0):
            power = (len(alphabet) ** k)
            j = cur / power
            cur = cur % power
            ret += alphabet[j]
        return ret

    def next():
        if self.__prev:
            k = len(self.__prev) - 1
            while self.__prev[k] == self.__alphabet_last and k >= 0:
                k -= 1
            if k < 0:
                self.__prev = self.__alphabet_first + self.__prev
            else:
                self.__prev[k] = \
                    self.alphabet[self.__alphabet_inverse[self.__prev[k]]+1]
                for i in range(0, k-1):
                    self.__prev[i] = self.__alphabet_first
        else:
            self.__prev = generate(i)
        return self.__prev
