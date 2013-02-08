NAME=diamond-collectors
VERSION=$(shell cat version.txt)

.PHONY: builddeb
builddeb:
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) -d diamond --prefix /usr/share/diamond/collectors --deb-user root --deb-group root -x '**/example/*' -x '**/README.rst' -x '**/version.txt' -x '**/Makefile' -x '**/.git/*' ./
