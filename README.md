gcc -g -Wall -I/usr/include/libxml2 server.c md5.c util.c -lxml2 -lrados -lllhttp -luriparser -lssl -lcrypto && ./a.out
