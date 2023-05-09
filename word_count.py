from pyspark import SparkContext
import sys
import string

def escribeF(filename, txt):
    with open(filename, "w") as f:
        f.write(txt)
    print("Se ha guardado en el fichero " + filename + " correctamente.")

def word_split(line):
    for c in string.punctuation+"¿!«»":
        line = line.replace(c,' ')
        line = line.lower()
    return len(line.split())

def main(filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        lineas = sc.textFile(filename)
        words_rdd = lineas.map(word_split)
        print (words_rdd.collect())
        suma = words_rdd.sum()
        print ("La suma es: " + str(suma))
        if filename == "quijote_s05.txt":
            escribeF("out_quijote_s05.txt", str(suma))
        else:
            escribeF("out_quijote.txt", str(suma))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sys.argv[1])
