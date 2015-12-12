# -*- coding: utf-8 -*-

import random


def creaciontxt(inicio, fin):
    archivo = open('dato8.txt','w')

    i = 1
    while i <= fin:
        count_rel = int(random.uniform(1, 6)) # Devuelve un numero aleatorio entre 1 y 10
        j = 1
        rel_peso = ''
        conj_rel = []
        while j <= count_rel:
            peso = int(random.uniform(3, 1000))
            rel = int(random.uniform(inicio, fin))
            conj_rel.append(int(random.uniform(inicio, fin)))

            if j == 1 & rel == 1:
                rel = int(random.uniform(inicio+1, fin-1))

            for k in conj_rel:
                if k == rel:
                    rel = str(int(random.uniform(inicio, fin)))

            if j <= count_rel - 1:
                rel_peso += '{' + str(rel) + ',' + str(peso) + '}' + ' '
            else:
                rel_peso += '{' + str(rel) + ',' + str(peso) + '}'
            j += 1

        if i == 1:
            archivo.write(str(i) +'\t'+ str(0) +'\t'+ rel_peso + '\n')
        else:
            archivo.write(str(i) +'\t' + str(1000) +'\t'+ rel_peso + '\n')
        i += 1

    archivo.close()


creaciontxt(1,30000000)