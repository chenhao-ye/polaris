python2.7 gprof2dot.py $1.out -f callgrind -n 0 -e 0 -s > $1.dot
dot -Tpng $1.dot -o $1.png
