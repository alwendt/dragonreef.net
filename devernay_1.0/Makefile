devernay: devernay_cmd.c io.c io.h devernay.c devernay.h
	$(CC) -O3 -o devernay devernay_cmd.c io.c devernay.c -lm

test: devernay
	./devernay image.pgm -t output.txt -p output.pdf -g output.svg

clean:
	rm -f devernay edges.bin edges.cgi output.txt output.pdf output.svg

edges.bin: edges_cgi.c devernay.c devernay.h
	$(CC) -O3 -Wall -Wextra -o edges.bin edges_cgi.c devernay.c -lm
