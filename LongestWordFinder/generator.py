import argparse
from random import randint

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-c', help='count of words', required=True, type=int)
parser.add_argument('-p', help='period of malformed', required=True, type=int)
parser.add_argument('-o', help='output file', required=True)

args = parser.parse_args()


def translate(word):
    symbols = (u"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
               u"абцдифжхижклмнопкрстуввхузАБЦДИФЖХИЖКЛМНОПКРСТУВВХУЗ")
    tr = {ord(a): ord(b) for a, b in zip(*symbols)}
    return word.translate(tr)


with open("/usr/share/dict/words") as words_file:
    WORDS = words_file.read().splitlines()


with open(args.o, 'w') as output_file:
    for i in range(int(args.c)):
        if (i + 1) % int(args.p) == 0:
            output_file.write(translate(WORDS[randint(0, len(WORDS) - 1)]) + " ")
        else:
            output_file.write(WORDS[randint(0, len(WORDS) - 1)] + " ")
