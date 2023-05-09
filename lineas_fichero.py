from pyspark import SparkContext
import sys
import random

def tirar_dado():
    return random.randint(1, 6)
def porcentaje():
    return random.randint(0, 100)

def main(infile, outfile):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        lines_rdd = sc.textFile(infile)
        with open(outfile, "w") as f:
            for line in lines_rdd.collect():
                dado = tirar_dado()
                cent = porcentaje()
                if dado < cent:
                    f.write(line)
        
        print("Lineas escritas en " + outfile )

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sys.argv[1], "quijote_s05.txt" )
