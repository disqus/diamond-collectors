NAME=diamond-collectors
VERSION=$(shell cat version.txt)

.PHONY: builddeb
builddeb:
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) -d diamond -d "python-redis (>= 2.4.5)" --prefix /usr/share/diamond/collectors --deb-user root --deb-group root -x '**/example/*' -x '**/README.rst' -x '**/version.txt' -x '**/Makefile' -x '**/.git/*' ./
